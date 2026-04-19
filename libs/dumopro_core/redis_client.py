from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from redis.asyncio import Redis

from . import keys as k
from .keys import Target, Unit


class RedisClient:
    def __init__(self, url: str) -> None:
        self._redis: Redis = Redis.from_url(url, decode_responses=True)

    @property
    def raw(self) -> Redis:
        return self._redis

    async def ping(self) -> bool:
        return await self._redis.ping()

    async def close(self) -> None:
        await self._redis.aclose()

    # --- cursor -----------------------------------------------------------

    async def get_cursor(self, station: str) -> tuple[int, dict[str, str]]:
        data = await self._redis.hgetall(k.cursor(station))
        last_id = int(data.get("last_id", 0))
        return last_id, data

    async def set_cursor(
        self,
        station: str,
        *,
        last_id: int,
        last_sampled_at: datetime | None = None,
        extra: dict[str, str] | None = None,
    ) -> None:
        payload: dict[str, Any] = {"last_id": last_id}
        if last_sampled_at is not None:
            payload["last_sampled_at"] = last_sampled_at.isoformat()
        if extra:
            payload.update(extra)
        await self._redis.hset(k.cursor(station), mapping=payload)

    # --- live -------------------------------------------------------------

    async def add_live_raw(
        self,
        station: str,
        unit: Unit,
        bucket_key: str,
        sample_id: int,
        value: float,
    ) -> None:
        await self._redis.zadd(k.live_raw(station, unit, bucket_key), {str(sample_id): value})

    async def get_live_raw_values(
        self, station: str, unit: Unit, bucket_key: str
    ) -> list[float]:
        scores = await self._redis.zrange(
            k.live_raw(station, unit, bucket_key), 0, -1, withscores=True
        )
        return [float(score) for _, score in scores]

    async def set_live_stats(
        self, station: str, unit: Unit, bucket_key: str, stats_json: str
    ) -> None:
        await self._redis.set(k.live_stats(station, unit, bucket_key), stats_json)

    async def get_live_stats(
        self, station: str, unit: Unit, bucket_key: str
    ) -> dict | None:
        raw = await self._redis.get(k.live_stats(station, unit, bucket_key))
        return json.loads(raw) if raw else None

    # --- frozen -----------------------------------------------------------

    async def freeze_bucket(
        self,
        station: str,
        unit: Unit,
        bucket_key: str,
        stats_json: str,
        score: float,
    ) -> None:
        """Atomic: write frozen, add index, clear live artifacts."""
        pipe = self._redis.pipeline(transaction=True)
        pipe.set(k.frozen(station, unit, bucket_key), stats_json)
        pipe.zadd(k.frozen_index(station, unit), {bucket_key: score})
        pipe.delete(k.live_raw(station, unit, bucket_key))
        pipe.delete(k.live_stats(station, unit, bucket_key))
        await pipe.execute()

    async def get_frozen_range(
        self, station: str, unit: Unit, min_score: float, max_score: float
    ) -> list[str]:
        return await self._redis.zrangebyscore(
            k.frozen_index(station, unit), min_score, max_score
        )

    async def get_frozen_stats(
        self, station: str, unit: Unit, bucket_key: str
    ) -> dict | None:
        raw = await self._redis.get(k.frozen(station, unit, bucket_key))
        return json.loads(raw) if raw else None

    # --- pub/sub ----------------------------------------------------------

    async def publish_candle_event(self, station: str, event: dict) -> None:
        await self._redis.publish(k.channel_candle(station), json.dumps(event))

    async def subscribe_candle(self, station: str):
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(k.channel_candle(station))
        return pubsub

    # --- residual ---------------------------------------------------------

    async def residual_push(
        self, station: str, unit: Unit, target: Target, residuals: list[float], cap: int = 10_000
    ) -> None:
        key = k.residual(station, unit, target)
        if not residuals:
            return
        pipe = self._redis.pipeline(transaction=True)
        pipe.rpush(key, *[str(v) for v in residuals])
        pipe.ltrim(key, -cap, -1)
        await pipe.execute()

    async def residual_all(
        self, station: str, unit: Unit, target: Target
    ) -> list[float]:
        raw = await self._redis.lrange(k.residual(station, unit, target), 0, -1)
        return [float(v) for v in raw]

    # --- config / stations list ------------------------------------------

    async def set_stations(self, stations: list[dict]) -> None:
        await self._redis.set(k.stations_list(), json.dumps(stations))

    async def get_stations(self) -> list[dict]:
        raw = await self._redis.get(k.stations_list())
        return json.loads(raw) if raw else []

    async def get_runtime_config(self) -> dict[str, str]:
        return await self._redis.hgetall(k.config_runtime())

    async def set_runtime_config(self, mapping: dict[str, str]) -> None:
        if mapping:
            await self._redis.hset(k.config_runtime(), mapping=mapping)
