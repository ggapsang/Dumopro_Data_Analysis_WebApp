from datetime import datetime, timezone
from typing import Literal

Unit = Literal["day", "week", "month"]
UNIT_LIST: tuple[Unit, ...] = ("day", "week", "month")


def _as_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def bucket_key_day(ts: datetime) -> str:
    ts = _as_utc(ts)
    return ts.strftime("%Y-%m-%d")


def bucket_key_week(ts: datetime) -> str:
    ts = _as_utc(ts)
    iso_year, iso_week, _ = ts.isocalendar()
    return f"{iso_year:04d}-W{iso_week:02d}"


def bucket_key_month(ts: datetime) -> str:
    ts = _as_utc(ts)
    return ts.strftime("%Y-%m")


def bucket_key(ts: datetime, unit: Unit) -> str:
    if unit == "day":
        return bucket_key_day(ts)
    if unit == "week":
        return bucket_key_week(ts)
    if unit == "month":
        return bucket_key_month(ts)
    raise ValueError(f"Unknown unit: {unit}")


def all_bucket_keys(ts: datetime) -> dict[str, str]:
    return {
        "day": bucket_key_day(ts),
        "week": bucket_key_week(ts),
        "month": bucket_key_month(ts),
    }


def bucket_score(ts: datetime, unit: Unit) -> float:
    """ZSET score for `frozen:index:*`. Use the UTC epoch seconds of the bucket start."""
    ts = _as_utc(ts)
    if unit == "day":
        start = datetime(ts.year, ts.month, ts.day, tzinfo=timezone.utc)
    elif unit == "week":
        iso_year, iso_week, _ = ts.isocalendar()
        start = datetime.fromisocalendar(iso_year, iso_week, 1).replace(tzinfo=timezone.utc)
    elif unit == "month":
        start = datetime(ts.year, ts.month, 1, tzinfo=timezone.utc)
    else:
        raise ValueError(f"Unknown unit: {unit}")
    return start.timestamp()


def is_boundary_crossed(prev_ts: datetime, new_ts: datetime, unit: Unit) -> bool:
    return bucket_key(prev_ts, unit) != bucket_key(new_ts, unit)
