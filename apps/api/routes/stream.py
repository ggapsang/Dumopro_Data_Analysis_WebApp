from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from ..services.sse_broadcaster import iter_sse

router = APIRouter()
log = logging.getLogger(__name__)


@router.get("/api/stream/{station}")
async def stream(station: str, request: Request) -> StreamingResponse:
    redis = request.app.state.redis
    broadcaster = request.app.state.broadcaster

    stations = {s["station_name"] for s in await redis.get_stations()}
    if station not in stations:
        raise HTTPException(status_code=404, detail=f"station {station} not found")

    queue = await broadcaster.subscribe(station)

    async def gen():
        try:
            yield ": connected\n\n"
            async for chunk in iter_sse(queue):
                if await request.is_disconnected():
                    break
                yield chunk
        finally:
            await broadcaster.unsubscribe(station, queue)
            log.info("sse.client_disconnect station=%s", station)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
