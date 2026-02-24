from datetime import datetime

from fastapi import APIRouter

from api.cache import build_cache_key, get_or_fetch
from ._common import get_clickhouse_client


router = APIRouter()


@router.get("/hr/performance")
def hr_performance():
    cache_key = build_cache_key("hr", "performance", date=datetime.utcnow().date().isoformat())

    def _fetch():
        client = get_clickhouse_client()
        rows = client.execute(
            """
            SELECT
                agent_id,
                sum(total_calls) as total_calls,
                avg(call_success_rate) as success_rate,
                sum(total_talk_time_min) as talk_time,
                sum(amount_collected) as collections
            FROM hr_agent_performance_daily
            GROUP BY agent_id
            ORDER BY total_calls DESC
            LIMIT 50
            """
        )

        data = [
            {
                "agent_id": r[0],
                "total_calls": int(r[1] or 0),
                "success_rate": float(r[2] or 0.0),
                "talk_time": float(r[3] or 0.0),
                "collections": float(r[4] or 0.0),
            }
            for r in rows
        ]

        return {
            "agents": data,
            "generated_at": datetime.utcnow().isoformat(),
        }

    return get_or_fetch(cache_key, 60, _fetch)