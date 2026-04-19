from typing import Literal

Unit = Literal["day", "week", "month"]
Target = Literal["median", "max", "q3"]


def live_raw(station: str, unit: Unit, bucket_key: str) -> str:
    return f"live:raw:{station}:{unit}:{bucket_key}"


def live_stats(station: str, unit: Unit, bucket_key: str) -> str:
    return f"live:stats:{station}:{unit}:{bucket_key}"


def frozen(station: str, unit: Unit, bucket_key: str) -> str:
    return f"frozen:{station}:{unit}:{bucket_key}"


def frozen_index(station: str, unit: Unit) -> str:
    return f"frozen:index:{station}:{unit}"


def cursor(station: str) -> str:
    return f"cursor:{station}"


def residual(station: str, unit: Unit, target: Target) -> str:
    return f"residual:{station}:{unit}:{target}"


def channel_candle(station: str) -> str:
    return f"channel:candle:{station}"


def config_runtime() -> str:
    return "config:runtime"


def stations_list() -> str:
    return "stations:list"
