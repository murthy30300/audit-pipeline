from datetime import datetime, date
from typing import Optional

from fastapi import APIRouter, Header

from api.cache import build_cache_key, get_or_fetch, r
from ._common import get_claims_from_auth, require_role, get_clickhouse_client

router = APIRouter()


@router.get('/agent/assigned-loans')
def assigned_loans(
    date: Optional[str] = None,
    status_filter: Optional[str] = None,
    authorization: Optional[str] = Header(default=None),
):
    claims = get_claims_from_auth(authorization)
    require_role(claims, "agent")

    agent_id = str(claims.get("agent_id") or claims.get("sub") or claims.get("user_id") or "")
    if not agent_id:
        return {"loans": [], "total": 0, "followups_due": 0, "cache_hit": False, "generated_at": datetime.utcnow().isoformat()}

    query_date = date or datetime.utcnow().date().isoformat()
    cache_key = build_cache_key(
        "agent",
        "assigned_loans",
        agent_id=agent_id,
        date=query_date,
        status_filter=status_filter or "ALL",
    )
    cache_hit = bool(r.get(cache_key))

    def _fetch():
        client = get_clickhouse_client()
        where_parts = ["agent_id = %(agent_id)s"]
        params = {"agent_id": agent_id, "query_date": query_date}

        if status_filter:
            where_parts.append("upper(last_call_status) = upper(%(status_filter)s)")
            params["status_filter"] = status_filter

        sql = f"""
        SELECT
            loan_id,
            borrower_name,
            dpd_days,
            overdue_amount,
            loan_aging_bucket,
            last_call_at,
            last_call_status,
            last_call_duration,
            followup_due,
            calls_today
        FROM agent_assigned_loans
        WHERE {' AND '.join(where_parts)}
        """
        rows = client.execute(sql, params)
        loans = [
            {
                "loan_id": row[0],
                "borrower_name": row[1],
                "dpd_days": int(row[2] or 0),
                "overdue_amount": float(row[3] or 0),
                "loan_aging_bucket": row[4],
                "last_call_at": row[5].isoformat() if row[5] else None,
                "last_call_status": row[6],
                "last_call_duration": int(row[7] or 0),
                "followup_due": bool(row[8]),
                "calls_today": int(row[9] or 0),
            }
            for row in rows
        ]
        followups_due = sum(1 for loan in loans if loan["followup_due"])
        return {
            "loans": loans,
            "total": len(loans),
            "followups_due": followups_due,
            "generated_at": datetime.utcnow().isoformat(),
        }

    result = get_or_fetch(cache_key, ttl=30, fetch_fn=_fetch)
    result["cache_hit"] = cache_hit
    result.setdefault("generated_at", datetime.utcnow().isoformat())
    return result
