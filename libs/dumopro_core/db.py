from __future__ import annotations

import asyncpg

from .models import SampleRow, StationInfo


async def init_pool(dsn: str, *, min_size: int = 1, max_size: int = 5) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn=dsn, min_size=min_size, max_size=max_size)


async def fetch_stations(pool: asyncpg.Pool) -> list[StationInfo]:
    rows = await pool.fetch(
        """
        SELECT station_id::text AS station_id,
               station_name,
               status,
               location_info
        FROM station
        ORDER BY station_name
        """
    )
    return [StationInfo(**dict(r)) for r in rows]


async def fetch_samples_since(
    pool: asyncpg.Pool,
    station_id: str,
    last_id: int,
    measurement_type: str,
    limit: int,
) -> list[SampleRow]:
    rows = await pool.fetch(
        """
        SELECT id, station_id::text AS station_id, measurement_type,
               value, unit, sampled_at
        FROM sensor_sample
        WHERE station_id = $1::uuid
          AND measurement_type = $2
          AND id > $3
        ORDER BY id
        LIMIT $4
        """,
        station_id,
        measurement_type,
        last_id,
        limit,
    )
    return [SampleRow(**dict(r)) for r in rows]


async def fetch_samples_latest(
    pool: asyncpg.Pool,
    station_id: str,
    measurement_type: str,
    limit: int,
) -> list[SampleRow]:
    """Latest N samples in chronological order (oldest first of the N newest)."""
    rows = await pool.fetch(
        """
        SELECT id, station_id::text AS station_id, measurement_type,
               value, unit, sampled_at
        FROM sensor_sample
        WHERE station_id = $1::uuid
          AND measurement_type = $2
        ORDER BY id DESC
        LIMIT $3
        """,
        station_id,
        measurement_type,
        limit,
    )
    # Reverse in Python so caller gets oldest→newest.
    return [SampleRow(**dict(r)) for r in reversed(rows)]


async def iter_all_samples(
    pool: asyncpg.Pool,
    station_id: str,
    measurement_type: str,
    *,
    chunk: int = 5000,
):
    """Yield samples in id order. Used for cold-start backfill."""
    last_id = 0
    while True:
        rows = await pool.fetch(
            """
            SELECT id, station_id::text AS station_id, measurement_type,
                   value, unit, sampled_at
            FROM sensor_sample
            WHERE station_id = $1::uuid
              AND measurement_type = $2
              AND id > $3
            ORDER BY id
            LIMIT $4
            """,
            station_id,
            measurement_type,
            last_id,
            chunk,
        )
        if not rows:
            return
        for r in rows:
            yield SampleRow(**dict(r))
        last_id = rows[-1]["id"]
        if len(rows) < chunk:
            return
