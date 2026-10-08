from __future__ import annotations

from datetime import timedelta

from akadze import Akadze, queue_snapshot
from akadze.worker import Worker


async def test_snapshot_reports_depth_lag_and_busy_slots(
    app: Akadze,
) -> None:
    # Arrange
    @app.task("demo")
    async def demo() -> None:
        return None

    async with app.engine.begin() as session:
        await demo.using(session=session).enqueue()
        await demo.using(session=session).enqueue()
        await demo.using(session=session, delay=timedelta(hours=1)).enqueue()
    await Worker(app, slots=1).claim_available()

    # Act
    shot = await queue_snapshot(app.engine)

    # Assert
    counts = {(item.queue, item.state): item.jobs for item in shot.counts}
    assert counts == {("default", "queued"): 2, ("default", "running"): 1}
    assert shot.busy == 1
    assert shot.lag is not None
    assert timedelta(0) <= shot.lag < timedelta(minutes=1)
