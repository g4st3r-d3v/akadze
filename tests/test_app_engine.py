from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from akadze import Akadze, AkadzeError

URL = "postgresql://unused/akadze"


@pytest.mark.parametrize(("pool_size", "expected"), [(25, 25), (None, 5)], ids=["sized", "default"])
async def test_the_built_engine_gets_the_pool_size(pool_size: int | None, expected: int) -> None:
    # Act
    app = Akadze(database_url=URL, pool_size=pool_size)

    # Assert
    try:
        assert app.engine.pool.size() == expected  # type: ignore[attr-defined]
    finally:
        await app.aclose()


async def test_pool_size_with_an_engine_raises() -> None:
    # Arrange
    engine = create_async_engine("postgresql+asyncpg://unused/akadze")

    # Act / Assert
    try:
        with pytest.raises(
            AkadzeError, match="pool_size applies only to the engine built from database_url"
        ):
            Akadze(engine=engine, pool_size=25)
    finally:
        await engine.dispose()


@pytest.mark.parametrize("pool_size", [0, -1, True, 2.5, "25"])
def test_pool_size_that_is_not_a_positive_int_raises(pool_size: Any) -> None:
    # Act / Assert
    with pytest.raises(AkadzeError, match="pool_size must be an int of at least 1"):
        Akadze(database_url=URL, pool_size=pool_size)
