"""Prometheus text from a queue snapshot plus pause flags. No HTTP."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from akadze.queue import QueueSnapshot, queue_snapshot


async def render_metrics(engine: AsyncEngine) -> str:
    """Return Prometheus exposition text for jobs, lag, busy, and pauses."""

    shot = await queue_snapshot(engine)
    async with engine.connect() as connection:
        rows = await connection.execute(
            text("SELECT queue FROM akadze.queue_pauses ORDER BY queue")
        )
        paused = tuple(str(row.queue) for row in rows)
    return _format_metrics(shot, paused)


def _format_metrics(shot: QueueSnapshot, paused: tuple[str, ...]) -> str:
    lines: list[str] = [
        "# TYPE akadze_jobs gauge",
    ]
    for item in shot.counts:
        labels = _labels(queue=item.queue, state=item.state)
        lines.append(f"akadze_jobs{{{labels}}} {item.jobs}")

    if shot.lag is not None:
        lines.append("# TYPE akadze_queue_lag_seconds gauge")
        lines.append(f"akadze_queue_lag_seconds {_format_float(shot.lag.total_seconds())}")

    lines.append("# TYPE akadze_worker_busy gauge")
    lines.append(f"akadze_worker_busy {shot.busy}")

    lines.append("# TYPE akadze_queue_paused gauge")
    for queue in paused:
        lines.append(f'akadze_queue_paused{{{_labels(queue=queue)}}} 1')

    return "\n".join(lines) + "\n"


def _labels(**values: str) -> str:
    return ",".join(f'{name}="{_escape_label(value)}"' for name, value in values.items())


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _format_float(value: float) -> str:
    text_value = f"{value:.6f}".rstrip("0").rstrip(".")
    return text_value if text_value else "0"
