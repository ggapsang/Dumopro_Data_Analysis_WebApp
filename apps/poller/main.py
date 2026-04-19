from __future__ import annotations

import asyncio
import logging

from dumopro_core.config import get_settings
from dumopro_core.db import fetch_stations, init_pool
from dumopro_core.models import StationInfo
from dumopro_core.redis_client import RedisClient

from .health import HealthState, serve as serve_health
from .station_task import StationTask

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("poller.main")


async def _sync_stations_list(redis: RedisClient, stations: list[StationInfo]) -> None:
    payload = [s.model_dump() for s in stations]
    await redis.set_stations(payload)
    log.info("stations.synced count=%d", len(payload))


async def run() -> None:
    settings = get_settings()
    log.info("poller.boot pg=%s redis=%s", settings.pg_dsn.split("@")[-1], settings.redis_url)

    pool = await init_pool(settings.pg_dsn)
    redis = RedisClient(settings.redis_url)
    await redis.ping()

    stations = await fetch_stations(pool)
    await _sync_stations_list(redis, stations)

    health_state = HealthState()
    health_state.station_count = len(stations)

    tasks: list[asyncio.Task] = []
    station_tasks: list[StationTask] = []
    cold_signals: list[asyncio.Event] = []
    for st in stations:
        ev = asyncio.Event()
        cold_signals.append(ev)
        t = StationTask(st, pool, redis, settings, cold_start_signal=ev)
        station_tasks.append(t)
        tasks.append(asyncio.create_task(t.run(), name=f"station:{st.station_name}"))

    tasks.append(
        asyncio.create_task(serve_health(health_state, settings.health_port), name="health")
    )

    async def mark_ready() -> None:
        await asyncio.gather(*(ev.wait() for ev in cold_signals))
        health_state.cold_start_done = True
        # one more tick interval before declaring warm
        await asyncio.sleep(settings.poll_interval_sec * 2)
        health_state.tick_seen = True
        log.info("poller.ready station_count=%d", len(stations))

    tasks.append(asyncio.create_task(mark_ready(), name="ready_marker"))

    try:
        await asyncio.gather(*tasks)
    finally:
        for t in station_tasks:
            t.stop()
        await redis.close()
        await pool.close()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("poller.shutdown")


if __name__ == "__main__":
    main()
