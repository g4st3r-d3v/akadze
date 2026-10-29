"""Tasks of the example. The app includes this module by name."""

from __future__ import annotations

from orders import orders
from sqlalchemy import update

from akadze import Akadze, current


def register(app: Akadze) -> None:
    @app.task("orders.confirm", max_attempts=5)
    async def confirm(order_id: int) -> None:
        # ... call a payment provider here; it may run twice, so pass it an idempotency key.
        async with current().complete_tx() as connection:
            # The order turns confirmed in the same transaction as the job turns succeeded.
            await connection.execute(
                update(orders).where(orders.c.id == order_id).values(status="confirmed")
            )
            await receipt.using(session=connection).enqueue(order_id=order_id)

    @app.task("orders.receipt", queue="mail")
    async def receipt(order_id: int) -> None:
        # ... send the receipt.
        return None
