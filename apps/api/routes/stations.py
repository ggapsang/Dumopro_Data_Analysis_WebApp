from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends

from dumopro_core.redis_client import RedisClient

from ..deps import get_redis

router = APIRouter()


@router.get("/api/stations")
async def list_stations(redis: RedisClient = Depends(get_redis)) -> dict:
    stations = await redis.get_stations()
    now = datetime.now(timezone.utc)
    out = []
    for s in stations:
        name = s["station_name"]
        last_id, cursor_data = await redis.get_cursor(name)
        last_sampled_at = cursor_data.get("last_sampled_at")
        idle_seconds: float | None = None
        if last_sampled_at:
            try:
                dt = datetime.fromisoformat(last_sampled_at)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                idle_seconds = max(0.0, (now - dt).total_seconds())
            except ValueError:
                pass
        out.append(
            {
                "station_id": s["station_id"],
                "station_name": name,
                "status": s.get("status"),
                "location_info": s.get("location_info"),
                "last_id": last_id,
                "last_sampled_at": last_sampled_at,
                "idle_seconds": idle_seconds,
                "offline": False,
            }
        )
    return {"stations": out}
