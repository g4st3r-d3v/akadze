"""Rescue dead workers and delete old finished rows. No leader."""

from __future__ import annotations

import logging
from datetime import timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from akadze.hooks import Hooks
from akadze.job import columns, job_from_mapping

logger = logging.getLogger("akadze")


async def rescue(engine: AsyncEngine, hooks: Hooks, *, ttl: timedelta) -> None:
    """Requeue jobs whose worker heartbeat is older than ttl. Returns nothing."""

    async with engine.begin() as connection:
        result = await connection.execute(
            text(
                f"""
                WITH stale AS (
                    SELECT id
                    FROM akadze.workers
                    WHERE heartbeat_at < now() - CAST(:ttl AS interval)
                ),
                victims AS (
                    SELECT jobs.id
                    FROM akadze.jobs AS jobs
                    JOIN stale ON stale.id = jobs.worker_id
                    WHERE jobs.state = 'running'
                    FOR UPDATE OF jobs SKIP LOCKED
                )
                UPDATE akadze.jobs AS job
                SET attempt = job.attempt + 1,
                    state = CASE
                        WHEN job.attempt + 1 >= job.max_attempts THEN 'failed'
                        ELSE 'queued'
                    END,
                    run_at = now(),
                    worker_id = NULL,
                    started_at = NULL,
                    finished_at = CASE
                        WHEN job.attempt + 1 >= job.max_attempts THEN now()
                        ELSE NULL
                    END,
                    errors = job.errors || jsonb_build_array(
                        jsonb_build_object(
                            'error', 'WorkerLost',
                            'message', 'heartbeat expired'
                        )
                    )
                FROM victims
                WHERE job.id = victims.id
                RETURNING {columns("job")}, false AS expired
                """
            ),
            {"ttl": ttl},
        )
        for row in result.mappings():
            job = job_from_mapping(row)
            logger.info("job %s running -> %s", job.id, job.state)
            await hooks.ran_transition(connection, job, "running", job.state)
        await connection.execute(
            text(
                """
                DELETE FROM akadze.workers AS worker
                WHERE worker.heartbeat_at < now() - CAST(:ttl AS interval)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM akadze.jobs
                      WHERE worker_id = worker.id AND state = 'running'
                  )
                """
            ),
            {"ttl": ttl},
        )


async def prune(engine: AsyncEngine, *, retention: timedelta, batch: int = 1000) -> None:
    """Delete finished jobs and old periodic runs. Returns nothing."""

    async with engine.begin() as connection:
        await connection.execute(
            text(
                """
                WITH picked AS (
                    SELECT id
                    FROM akadze.jobs
                    WHERE state IN ('succeeded', 'failed', 'cancelled')
                      AND finished_at < now() - CAST(:retention AS interval)
                    ORDER BY finished_at
                    LIMIT :batch
                    FOR UPDATE SKIP LOCKED
                )
                DELETE FROM akadze.jobs
                WHERE id IN (SELECT id FROM picked)
                """
            ),
            {"retention": retention, "batch": batch},
        )
        await connection.execute(
            text(
                """
                DELETE FROM akadze.periodic_runs
                WHERE fire_at < now() - CAST(:retention AS interval)
                """
            ),
            {"retention": retention},
        )
