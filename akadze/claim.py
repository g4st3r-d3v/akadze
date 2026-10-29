"""Claim ready jobs of one queue: plan from windows of the queue, then take the plan.

A claim reads the queue in claim order (priority, then run_at, then id) one
window at a time and plans in Python which rows to take. A window leaves out
rows the plan cannot use: keys that hit a limit, and in ``fair`` mode groups
whose next row cannot beat the rows already planned. So a key at its limit
at the head of the queue, or a deep backlog of one group, is read once, not
once per row it holds back. The plan is exact: it takes the rows a scan of
every ready row would, under the same limits and fair turns.
"""

from __future__ import annotations

import heapq
import logging
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from akadze.hooks import Hooks
from akadze.job import Job, columns, job_from_mapping
from akadze.ready import claimable

logger = logging.getLogger("akadze")

_FIRST_WINDOW_FACTOR = 4


@dataclass(frozen=True, slots=True)
class Ready:
    """A ready row of the queue, as a window returns it."""

    id: int
    limit_key: str | None
    priority: int
    run_at: datetime


@dataclass(frozen=True, slots=True)
class Policy:
    """How many more rows of a limited key this claim may start."""

    left: int


class ClaimPlan:
    """Rows a claim takes, built window by window from the queue in claim order.

    Pure: windows go in through ``add``, limits through ``limit_keys``;
    ``ids`` is the plan. In the default order the plan is the first ``limit`` rows
    that pass their key's limit. With ``fair``, rows of one priority are
    ordered by turn: the running jobs of the row's group (its ``limit_key``,
    or the rows without one) in this queue plus the row's place among the
    group's rows that pass their limit. A row whose place is above ``limit``
    has no turn.

    A limited key with ``left`` starts passes its first ``left`` ready rows in
    claim order; after that the key is blocked and later windows leave it out.
    In ``fair`` mode a group is capped once its next row's turn is above the
    turn of the last row the plan needs at this priority: no later row of it
    can be taken, so later windows leave it out too. Rows a window leaves out
    are never needed, so the plan stays the plan of a scan of every ready row.
    """

    def __init__(self, *, limit: int, fair: bool, busy: Mapping[str | None, int]) -> None:
        self.limit = limit
        self.fair = fair
        self._busy = dict(busy)
        self._policies: dict[str, Policy | None] = {}
        self._seen: Counter[str] = Counter()
        self._blocked: set[str] = set()
        self._taken: list[int] = []
        self._tier: int | None = None
        self._turns: list[tuple[int, datetime, int]] = []
        self._groups: dict[int, str | None] = {}
        self._places: Counter[str | None] = Counter()
        self._closed = False
        self._last: Ready | None = None

    @property
    def settled(self) -> bool:
        """Whether no row after the ones read can change the plan."""

        if self._closed:
            return True
        need = self.limit - len(self._taken)
        if need <= 0:
            return True
        if len(self._turns) < need:
            return False
        if not self.fair:
            return True
        # Rows after the ones read are later in run_at, so a tie on turn loses;
        # an unseen group's first row has turn 1 at least.
        return self._cut() <= 1

    def unknown_keys(self, rows: Iterable[Ready]) -> list[str]:
        """Keys of rows whose limit this plan has not been given yet."""

        return sorted(
            {
                row.limit_key
                for row in rows
                if row.limit_key is not None and row.limit_key not in self._policies
            }
        )

    def limit_keys(self, policies: Mapping[str, Policy | None]) -> None:
        """Record keys' limits; ``None`` is a key without one."""

        self._policies.update(policies)

    @property
    def last_read(self) -> Ready | None:
        """The last row ``add`` read."""

        return self._last

    def add(self, rows: Sequence[Ready]) -> None:
        """Read the next rows of the queue in claim order.

        Reading stops at a priority below one that settled the plan.
        """

        for row in rows:
            if self._closed:
                return
            if row.priority != self._tier:
                self.end_tier()
                if self._closed:
                    return
                self._tier = row.priority
            self._read(row)
            self._last = row

    def end_tier(self) -> None:
        """Mark every row of the current priority as read."""

        if self._tier is None:
            return
        need = self.limit - len(self._taken)
        closed = len(self._turns) >= need
        self._taken.extend(self._ordered())
        self._turns = []
        self._places = Counter()
        self._tier = None
        self._closed = closed

    def excluded(self, *, same_tier: bool) -> tuple[list[str], bool]:
        """Keys a window leaves out, and whether it leaves out rows without a key.

        ``same_tier`` windows continue the current priority, so they also leave
        out capped groups; a window that starts a lower priority does not.
        """

        keys = set(self._blocked)
        without_key = False
        if same_tier and self.fair:
            cut = self._cut()
            for group, place in self._places.items():
                if place >= self.limit or self._busy.get(group, 0) + place + 1 > cut:
                    if group is None:
                        without_key = True
                    else:
                        keys.add(group)
        return sorted(keys), without_key

    def ids(self) -> list[int]:
        """Planned rows in claim order.

        Rows past ``limit`` stand in for rows other claimers hold. In ``fair``
        mode only rows no unread row can beat stand in.
        """

        return self._taken + self._ordered()

    def group(self, row_id: int) -> str | None:
        """The limit_key of a planned row."""

        return self._groups[row_id]

    def _ordered(self) -> list[int]:
        cut = self._cut() if self.fair else float("inf")
        return [row_id for turn, _, row_id in sorted(self._turns) if turn <= cut]

    def _cut(self) -> float:
        """Turn of the last row the plan needs at this priority."""

        need = self.limit - len(self._taken)
        if need <= 0:
            return float("-inf")
        if len(self._turns) < need:
            return float("inf")
        return heapq.nsmallest(need, self._turns)[-1][0]

    def _read(self, row: Ready) -> None:
        key = row.limit_key
        if key is not None:
            if key in self._blocked:
                return
            policy = self._policies.get(key)
            if policy is not None:
                self._seen[key] += 1
                if self._seen[key] > policy.left:
                    self._blocked.add(key)
                    return
        place = self._places[key] + 1
        self._places[key] = place
        if self.fair:
            if place > self.limit:
                return
            turn = self._busy.get(key, 0) + place
        else:
            turn = 0
        self._turns.append((turn, row.run_at, row.id))
        self._groups[row.id] = key


_READY_COLUMNS = f"""
        SELECT j.id, j.limit_key, j.priority, j.run_at
        FROM akadze.jobs AS j
        WHERE j.queue = :queue
          AND {claimable("j", ":queue")}
          AND j.task = ANY(CAST(:tasks AS text[]))
          AND (j.limit_key IS NOT NULL OR NOT :without_key)
          AND (j.limit_key IS NULL OR j.limit_key <> ALL(CAST(:excluded AS text[])))
          AND j.id <> ALL(CAST(:held AS bigint[]))"""

_ORDER = """
        ORDER BY j.priority DESC, j.run_at, j.id
        LIMIT :size"""


@dataclass(frozen=True, slots=True)
class _Cursor:
    """Where the next window starts: after ``row`` in its priority, or below ``tier``."""

    tier: int
    row: Ready | None


async def _window(
    connection: AsyncConnection,
    *,
    queue: str,
    tasks: list[str],
    cursor: _Cursor | None,
    excluded: list[str],
    without_key: bool,
    held: list[int],
    size: int,
) -> list[Ready]:
    """Up to size ready rows after the cursor in claim order. A query.

    A job whose task this worker does not know stays queued for a worker that
    does, so a rolling deploy does not spend its attempts.
    """

    params: dict[str, object] = {
        "queue": queue,
        "tasks": tasks,
        "excluded": excluded,
        "without_key": without_key,
        "held": held,
        "size": size,
    }
    if cursor is None:
        where = ""
    elif cursor.row is None:
        where = "\n          AND j.priority < :tier"
        params["tier"] = cursor.tier
    else:
        # One priority at a time keeps the window a range of jobs_claim_idx.
        where = "\n          AND j.priority = :tier AND (j.run_at, j.id) > (:run_at, :id)"
        params.update(tier=cursor.tier, run_at=cursor.row.run_at, id=cursor.row.id)
    result = await connection.execute(text(_READY_COLUMNS + where + _ORDER), params)
    return [
        Ready(
            id=int(row.id),
            limit_key=row.limit_key,
            priority=int(row.priority),
            run_at=row.run_at,
        )
        for row in result
    ]


async def _lock_limits(connection: AsyncConnection, *, keys: list[str], wait: bool) -> list[str]:
    """Lock the limits rows of keys and return the keys locked.

    This is its own statement because under READ COMMITTED a statement reads
    one snapshot, taken before it waits for a lock; ``_policies`` reads the
    counts after these locks are held, so it sees running jobs and start marks
    that other claimers of the same keys have committed. Only a claim that holds
    no limits rows and no jobs yet waits; after that it skips a key another
    claimer holds, which waits for the next claim, so two claimers that lock
    across windows cannot deadlock.
    """

    skip = "" if wait else " SKIP LOCKED"
    rows = await connection.execute(
        text(
            f"""
            SELECT l.key
            FROM akadze.limits AS l
            WHERE l.key = ANY(CAST(:keys AS text[]))
            ORDER BY l.key
            FOR UPDATE{skip}
            """
        ),
        {"keys": keys},
    )
    return [str(row[0]) for row in rows]


async def _policies(
    connection: AsyncConnection, *, keys: list[str], locked: set[str]
) -> dict[str, Policy | None]:
    """Starts left for keys; a key with a limit this claim did not lock has none. A query."""

    rows = await connection.execute(
        text(
            """
            SELECT
                l.key,
                l.max_running,
                l.max_starts,
                (
                    SELECT count(*)::int
                    FROM akadze.jobs AS jobs
                    WHERE jobs.state = 'running'
                      AND jobs.limit_key = l.key
                ) AS running_count,
                (
                    SELECT count(*)::int
                    FROM akadze.limit_starts AS marks
                    WHERE marks.key = l.key
                      AND l.max_starts IS NOT NULL
                      AND marks.started_at > now() - l."window"
                ) AS start_count
            FROM akadze.limits AS l
            WHERE l.key = ANY(CAST(:keys AS text[]))
            """
        ),
        {"keys": keys},
    )
    policies: dict[str, Policy | None] = dict.fromkeys(keys)
    for row in rows.mappings():
        left = 0
        if row["key"] in locked:
            caps = []
            if row["max_running"] is not None:
                caps.append(row["max_running"] - row["running_count"])
            if row["max_starts"] is not None:
                caps.append(row["max_starts"] - row["start_count"])
            left = max(0, min(caps))
        policies[row["key"]] = Policy(left=left)
    return policies


async def _busy(connection: AsyncConnection, *, queue: str) -> dict[str | None, int]:
    """Running jobs of queue per limit_key group, across the cluster. A query."""

    rows = await connection.execute(
        text(
            """
            SELECT jobs.limit_key, count(*)::int AS busy
            FROM akadze.jobs AS jobs
            WHERE jobs.state = 'running'
              AND jobs.queue = :queue
            GROUP BY jobs.limit_key
            """
        ),
        {"queue": queue},
    )
    return {row.limit_key: int(row.busy) for row in rows}


class _Scan:
    """Reads windows of a queue into a plan, from where the last window stopped."""

    def __init__(
        self,
        connection: AsyncConnection,
        *,
        queue: str,
        tasks: list[str],
        size: int,
        locked: set[str],
        held: list[int],
        wait: bool,
    ):
        self.connection = connection
        self.queue = queue
        self.tasks = tasks
        self.size = size
        self.locked = locked
        self.held = held
        self.wait = wait
        self.cursor: _Cursor | None = None
        self.ran_out = False

    async def fill(self, plan: ClaimPlan) -> None:
        """Read windows until the plan settles or the queue runs out.

        Locks the limits rows of the keys read; ``locked`` collects them.
        Waits for a lock only when ``wait`` and no limits row is held yet.
        """

        while not plan.settled and not self.ran_out:
            same_tier = self.cursor is not None and self.cursor.row is not None
            excluded, without_key = plan.excluded(same_tier=same_tier)
            rows = await _window(
                self.connection,
                queue=self.queue,
                tasks=self.tasks,
                cursor=self.cursor,
                excluded=excluded,
                without_key=without_key,
                held=self.held,
                size=self.size,
            )
            keys = plan.unknown_keys(rows)
            if keys:
                fresh = [key for key in keys if key not in self.locked]
                if fresh:
                    self.locked.update(
                        await _lock_limits(
                            self.connection,
                            keys=fresh,
                            wait=self.wait and not self.locked,
                        )
                    )
                plan.limit_keys(await _policies(self.connection, keys=keys, locked=self.locked))
            plan.add(rows)
            last = plan.last_read
            if last is not None:
                self.cursor = _Cursor(tier=last.priority, row=last)
            if rows and last != rows[-1]:
                return
            if len(rows) == self.size:
                self.size *= 2
            elif same_tier and self.cursor is not None:
                # This priority ran out; go on below it.
                plan.end_tier()
                self.cursor = _Cursor(tier=self.cursor.tier, row=None)
            else:
                plan.end_tier()
                self.ran_out = True


async def _take(
    connection: AsyncConnection,
    *,
    worker_id: UUID,
    ids: list[int],
    limit: int,
    locked: set[str],
) -> list[Job]:
    """Claim up to limit of the planned rows, in plan order, skipping rows other claimers hold.

    A row whose key gained a limit after it was planned waits for the next
    claim. Records start marks of the claimed rows and prunes marks of the
    locked keys that left their window.
    """

    result = await connection.execute(
        text(
            f"""
            WITH planned AS (
                SELECT planned.id, planned.position
                FROM unnest(CAST(:ids AS bigint[])) WITH ORDINALITY AS planned(id, position)
            ),
            locked_limits AS (
                SELECT l.key, l.max_starts, l."window" AS rate_window
                FROM akadze.limits AS l
                WHERE l.key = ANY(CAST(:locked AS text[]))
            ),
            picked AS (
                SELECT job.id
                FROM akadze.jobs AS job
                INNER JOIN planned ON planned.id = job.id
                WHERE {claimable("job")}
                  AND (
                        job.limit_key IS NULL
                        OR job.limit_key = ANY(CAST(:locked AS text[]))
                        OR NOT EXISTS (
                            SELECT 1
                            FROM akadze.limits AS policy
                            WHERE policy.key = job.limit_key
                        )
                  )
                ORDER BY planned.position
                LIMIT :limit
                FOR UPDATE OF job SKIP LOCKED
            ),
            claimed AS (
                UPDATE akadze.jobs AS job
                SET state = 'running',
                    run_count = job.run_count + 1,
                    worker_id = :worker_id,
                    started_at = now()
                FROM picked
                WHERE job.id = picked.id
                RETURNING {columns("job")}, false AS expired
            ),
            recorded AS (
                INSERT INTO akadze.limit_starts (key, started_at)
                SELECT claimed.limit_key, now()
                FROM claimed
                INNER JOIN locked_limits
                    ON locked_limits.key = claimed.limit_key
                WHERE locked_limits.max_starts IS NOT NULL
                RETURNING key
            ),
            expired_marks AS (
                SELECT marks.id
                FROM akadze.limit_starts AS marks
                INNER JOIN locked_limits
                    ON locked_limits.key = marks.key
                WHERE locked_limits.rate_window IS NOT NULL
                  AND marks.started_at < now() - locked_limits.rate_window
                FOR UPDATE OF marks SKIP LOCKED
            ),
            pruned AS (
                DELETE FROM akadze.limit_starts AS marks
                USING expired_marks
                WHERE marks.id = expired_marks.id
                RETURNING marks.id
            )
            SELECT claimed.* FROM claimed
            """
        ),
        {"ids": ids, "limit": limit, "worker_id": worker_id, "locked": sorted(locked)},
    )
    jobs = [job_from_mapping(row) for row in result.mappings()]
    order = {job_id: position for position, job_id in enumerate(ids)}
    return sorted(jobs, key=lambda job: order[job.id])


async def claim_queue(
    connection: AsyncConnection,
    *,
    hooks: Hooks,
    worker_id: UUID,
    queue: str,
    tasks: list[str],
    limit: int,
    fair: bool,
) -> int:
    """Claim up to limit ready jobs of queue in this transaction; return how many.

    Plans from windows of the queue (see ``ClaimPlan``), then claims the plan.
    When other claimers hold planned rows, plans again without them, for
    twice as many rows each time, and takes the first free ones; in ``fair``
    mode each held row counts as running in its group, as it will once its
    claimer commits. Transition hooks run once the claim is final.
    """

    if limit <= 0:
        return 0
    jobs: list[Job] = []
    locked: set[str] = set()
    held: list[int] = []
    held_groups: Counter[str | None] = Counter()
    size = limit * _FIRST_WINDOW_FACTOR
    spread = 1
    while len(jobs) < limit:
        left = limit - len(jobs)
        busy: Counter[str | None] = Counter()
        if fair:
            busy.update(await _busy(connection, queue=queue))
            busy.update(held_groups)
        plan = ClaimPlan(limit=left * spread, fair=fair, busy=busy)
        scan = _Scan(
            connection,
            queue=queue,
            tasks=tasks,
            size=size,
            locked=locked,
            held=held,
            wait=not jobs,
        )
        await scan.fill(plan)
        ids = plan.ids()
        if not ids:
            break
        taken = await _take(connection, worker_id=worker_id, ids=ids, limit=left, locked=locked)
        jobs.extend(taken)
        if len(taken) == left:
            break
        # Every planned row was tried; the ones not taken are held by other
        # claimers. Plan again without them, counting each as running in its
        # group, and plan more rows than the slots left, so that many held rows
        # take a few plans, not one per slot.
        claimed = {job.id for job in taken}
        missed = [row_id for row_id in ids if row_id not in claimed]
        if not missed:
            break
        held.extend(missed)
        held_groups.update(plan.group(row_id) for row_id in missed)
        spread *= 2
        size *= 2
    for job in jobs:
        await hooks.ran_transition(connection, job, "queued", "running")
        logger.info("job %s queued -> running", job.id)
    return len(jobs)
