import json
from typing import Any, Callable, Dict
from urllib.parse import quote_plus

import redis

from config import REDIS_URL

r = redis.Redis.from_url(REDIS_URL)


def _encode_filter_value(value: Any) -> str:
    if isinstance(value, (dict, list, tuple)):
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"))
    else:
        encoded = str(value)
    return quote_plus(encoded)


def get_or_fetch(cache_key: str, ttl: int, fetch_fn: Callable[[], Dict[str, Any]]) -> Dict[str, Any]:
    val = r.get(cache_key)
    if val:
        parsed = json.loads(val)
        if isinstance(parsed, dict):
            return parsed
        return {"data": parsed}

    result = fetch_fn()
    r.setex(cache_key, ttl, json.dumps(result, separators=(",", ":"), default=str))
    return result


def build_cache_key(role: str, endpoint: str, **filters: Any) -> str:
    parts = [role, endpoint]
    for key in sorted(filters):
        parts.append(f"{key}={_encode_filter_value(filters[key])}")
    return ":".join(parts)


def invalidate(pattern: str) -> int:
    count = 0
    for key in r.scan_iter(pattern):
        r.delete(key)
        count += 1
    return count
