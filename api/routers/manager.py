from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Header, HTTPException

from api.cache import build_cache_key, get_or_fetch, r
from ._common import get_clickhouse_client, get_claims_from_auth, require_role

router = APIRouter()


@router.get('/manager/branch-summary')
def branch_summary(
    branch_id: Optional[str] = None,
    date: Optional[str] = None,
    manager_id: Optional[str] = None,
):
    selected_branch_id = branch_id or ""
    if not selected_branch_id:
        raise HTTPException(status_code=400, detail="branch_id is required for this demo")

    query_date = date or datetime.utcnow().date().isoformat()
    cache_key = build_cache_key(
        "manager",
        "branch_summary",
        manager_id=manager_id or "demo",
        branch_id=selected_branch_id,
        date=query_date,
    )
    cache_hit = bool(r.get(cache_key))

    def _fetch():
        client = get_clickhouse_client()

        summary_sql = """
        SELECT
            agents_active_today,
            total_calls_made,
            collection_today,
            target_achievement_pct,
            followups_pending,
            calls_successful,
            collection_target
        FROM manager_branch_summary
        WHERE branch_id = %(branch_id)s
          AND report_date = toDate(%(report_date)s)
        ORDER BY report_date DESC
        LIMIT 1
        """
        summary_rows = client.execute(summary_sql, {"branch_id": selected_branch_id, "report_date": query_date})
        if summary_rows:
            s = summary_rows[0]
            agents_active = int(s[0] or 0)
            calls_made = int(s[1] or 0)
            collection_today = float(s[2] or 0.0)
            target_pct = float(s[3] or 0.0)
            followups_pending = int(s[4] or 0)
            calls_successful = int(s[5] or 0)
            collection_target = float(s[6] or 0.0)
        else:
            agents_active = 0
            calls_made = 0
            collection_today = 0.0
            target_pct = 0.0
            followups_pending = 0
            calls_successful = 0
            collection_target = 0.0

        top_agents_sql = """
        SELECT
            c.agent_id,
            count() AS calls_made,
            countIf(c.call_success_flag) AS calls_successful,
            round(avg(c.call_duration_sec), 2) AS avg_call_duration_sec
        FROM calls_analyzed c
        INNER JOIN agents_enriched a ON c.agent_id = a.agent_id
        WHERE a.branch_id = %(branch_id)s
          AND toDate(c.call_start_time) = toDate(%(report_date)s)
        GROUP BY c.agent_id
        ORDER BY calls_successful DESC, calls_made DESC
        LIMIT 5
        """
        top_agents_rows = client.execute(top_agents_sql, {"branch_id": selected_branch_id, "report_date": query_date})
        top_agents = [
            {
                "agent_id": row[0],
                "calls_made": int(row[1] or 0),
                "calls_successful": int(row[2] or 0),
                "avg_call_duration_sec": float(row[3] or 0.0),
            }
            for row in top_agents_rows
        ]

        return {
            "agents_active_today": agents_active,
            "calls_made": calls_made,
            "collection_today": collection_today,
            "target_pct": target_pct,
            "top_agents": top_agents,
            "followups_pending": followups_pending,
            "generated_at": datetime.utcnow().isoformat(),
            "calls_successful": calls_successful,
            "collection_target": collection_target,
        }

    result = get_or_fetch(cache_key, ttl=60, fetch_fn=_fetch)
    if isinstance(result, dict):
        result["cache_hit"] = cache_hit
        result.setdefault("generated_at", datetime.utcnow().isoformat())
    return result