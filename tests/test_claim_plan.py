"""ClaimPlan is pure: windows in, planned ids out. No database."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from akadze.claim import ClaimPlan, Policy, Ready

_START = datetime(2026, 1, 1, tzinfo=UTC)


def _rows(*spec: tuple[str | None, int]) -> list[Ready]:
    """Rows in claim order from (limit_key, priority) pairs; id is the position."""

    return [
        Ready(id=n, limit_key=key, priority=priority, run_at=_START + timedelta(seconds=n))
        for n, (key, priority) in enumerate(spec, start=1)
    ]


def test_default_order_plans_the_first_rows_that_pass_their_limit() -> None:
    # Arrange
    plan = ClaimPlan(limit=3, fair=False, busy={})
    plan.limit_keys({"a": Policy(left=1), "b": None})

    # Act
    plan.add(_rows(("a", 0), ("a", 0), ("b", 0), ("a", 0), ("b", 0)))

    # Assert
    assert (plan.settled, plan.ids()) == (True, [1, 3, 5])


def test_a_key_past_its_limit_is_left_out_of_later_windows() -> None:
    # Arrange
    plan = ClaimPlan(limit=3, fair=False, busy={})
    plan.limit_keys({"a": Policy(left=1)})
    plan.add(_rows(("a", 0), ("a", 0)))

    # Act
    excluded = plan.excluded(same_tier=True)

    # Assert
    assert excluded == (["a"], False)


def test_fair_orders_rows_by_their_group_load_and_place() -> None:
    # Arrange
    plan = ClaimPlan(limit=3, fair=True, busy={"a": 1})

    # Act
    plan.add(_rows(("a", 0), ("a", 0), (None, 0), ("b", 0)))

    # Assert
    assert plan.ids() == [3, 4, 1]


def test_fair_settles_once_the_last_needed_row_has_turn_one() -> None:
    # Arrange
    plan = ClaimPlan(limit=2, fair=True, busy={})

    # Act
    plan.add(_rows(("a", 0), ("a", 0), ("b", 0)))

    # Assert
    assert (plan.settled, plan.ids()) == (True, [1, 3])


def test_fair_keeps_reading_while_an_unread_group_could_win() -> None:
    # Arrange
    plan = ClaimPlan(limit=2, fair=True, busy={})

    # Act
    plan.add(_rows(("a", 0), ("a", 0), ("a", 0)))

    # Assert
    assert plan.settled is False


@pytest.mark.parametrize(
    ("busy", "expected"),
    [({}, (["a"], False)), ({None: 5}, ([], True))],
    ids=["keyed", "unkeyed"],
)
def test_fair_caps_groups_whose_next_row_cannot_win(
    busy: dict[str | None, int], expected: tuple[list[str], bool]
) -> None:
    # Arrange: a group is capped when its next turn is above the third-best turn
    plan = ClaimPlan(limit=3, fair=True, busy=busy)
    plan.add(_rows(("a", 0), ("a", 0), (None, 0)))

    # Act
    excluded = plan.excluded(same_tier=True)

    # Assert
    assert excluded == expected


def test_a_window_below_the_current_priority_leaves_out_only_blocked_keys() -> None:
    # Arrange
    plan = ClaimPlan(limit=3, fair=True, busy={})
    plan.add(_rows(("a", 0), ("a", 0), ("a", 0)))

    # Act
    excluded = plan.excluded(same_tier=False)

    # Assert
    assert excluded == ([], False)


def test_a_higher_priority_is_planned_before_turns_of_a_lower_one() -> None:
    # Arrange
    plan = ClaimPlan(limit=3, fair=True, busy={})
    plan.add(_rows(("a", 5), ("a", 5), ("b", 0), ("c", 0)))

    # Act
    plan.end_tier()

    # Assert
    assert plan.ids() == [1, 2, 3, 4]


def test_reading_stops_below_a_priority_that_filled_the_plan() -> None:
    # Arrange
    plan = ClaimPlan(limit=2, fair=True, busy={})

    # Act
    plan.add(_rows(("a", 5), ("a", 5), ("b", 0)))

    # Assert
    assert (plan.last_read and plan.last_read.id, plan.settled, plan.ids()) == (2, True, [1, 2])


def test_fair_stand_ins_are_only_rows_no_unread_row_can_beat() -> None:
    # Arrange: turns a=1,2,3 and b=1; cut for 2 rows is turn 1
    plan = ClaimPlan(limit=2, fair=True, busy={})

    # Act
    plan.add(_rows(("a", 0), ("a", 0), ("a", 0), ("b", 0)))

    # Assert
    assert plan.ids() == [1, 4]
