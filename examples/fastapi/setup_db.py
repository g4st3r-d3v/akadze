"""Deploy step: akadze's schema, then the application's own table."""

from __future__ import annotations

import asyncio

from main import DATABASE_URL, app
from orders import metadata

from akadze import migrate


async def main() -> None:
    await migrate(DATABASE_URL)
    async with app.engine.begin() as connection:
        await connection.run_sync(metadata.create_all)
    await app.aclose()


if __name__ == "__main__":
    asyncio.run(main())
