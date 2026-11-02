from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from akadze import Akadze, AkadzeError

URL = "postgresql://unused/akadze"


async def test_pool_size_sets_the_pool_of_the_built_engine() -> None:
    # Arrange
    app = Akadze(database_url=URL, pool_size=25)

    # Act
    size = app.engine.pool.size()  # type: ignore[attr-defined]

    # Assert
    assert size == 25
    await app.aclose()


async def test_without_pool_size_the_built_engine_keeps_the_default_pool() -> None:
    # Arrange
    app = Akadze(database_url=URL)

    # Act
    size = app.engine.pool.size()  # type: ignore[attr-defined]

    # Assert
    assert size == 5
    await app.aclose()


async def test_pool_size_with_an_engine_raises() -> None:
    # Arrange
    engine = create_async_engine("postgresql+asyncpg://unused/akadze")

    # Act / Assert
    with pytest.raises(
        AkadzeError, match="pool_size applies only to the engine built from database_url"
    ):
        Akadze(engine=engine, pool_size=25)
    await engine.dispose()


@pytest.mark.parametrize("pool_size", [0, -1])
def test_pool_size_below_one_raises(pool_size: int) -> None:
    # Act / Assert
    with pytest.raises(AkadzeError, match="pool_size must be at least 1"):
        Akadze(database_url=URL, pool_size=pool_size)
