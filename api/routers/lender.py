from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Header

from api.cache import build_cache_key, get_or_fetch, r
from ._common import get_claims_from_auth, require_role, get_clickhouse_client

router = APIRouter()


@router.get('/lender/portfolio-summary')
def portfolio_summary(
    as_of_date: Optional[str] = None,
    bucket_filter: Optional[str] = None,
    authorization: Optional[str] = Header(default=None),
):
    claims = get_claims_from_auth(authorization)
    require_role(claims, "lender")

    lender_id = str(claims.get("lender_id") or claims.get("sub") or claims.get("user_id") or "")
    if not lender_id:
        return {
            "total_disbursed": 0.0,
            "npa_ratio": 0.0,
            "bucket_breakdown": {},
            "collection_efficiency": 0.0,
            "generated_at": datetime.utcnow().isoformat(),
            "cache_hit": False,
        }

    query_date = as_of_date or datetime.utcnow().date().isoformat()
    cache_key = build_cache_key(
        "lender",
        "portfolio_summary",
        lender_id=lender_id,
        as_of_date=query_date,
        bucket_filter=bucket_filter or "ALL",
    )
    cache_hit = bool(r.get(cache_key))

    def _fetch():
        client = get_clickhouse_client()
        where_parts = ["lender_id = %(lender_id)s"]
        params = {"lender_id": lender_id}
        if bucket_filter:
            where_parts.append("loan_aging_bucket = %(bucket_filter)s")
            params["bucket_filter"] = bucket_filter

        sql = f"""
        SELECT
            sum(total_principal_disbursed) AS total_disbursed,
            sum(npa_amount) AS total_npa_amount,
            sum(total_principal_disbursed) AS total_principal,
            avg(collection_efficiency_pct) AS collection_efficiency,
            groupArray((loan_aging_bucket, total_loans_count, total_principal_disbursed)) AS bucket_breakdown
        FROM lender_portfolio_summary
        WHERE {' AND '.join(where_parts)}
        """
        rows = client.execute(sql, params)
        row = rows[0] if rows else (0.0, 0.0, 0.0, 0.0, [])
        total_disbursed = float(row[0] or 0.0)
        total_npa_amount = float(row[1] or 0.0)
        total_principal = float(row[2] or 0.0)
        collection_efficiency = float(row[3] or 0.0)
        npa_ratio = (total_npa_amount / total_principal * 100) if total_principal > 0 else 0.0

        breakdown = {}
        for bucket, count, amount in (row[4] or []):
            breakdown[str(bucket)] = {
                "count": int(count or 0),
                "amount": float(amount or 0.0),
            }

        return {
            "total_disbursed": total_disbursed,
            "npa_ratio": npa_ratio,
            "bucket_breakdown": breakdown,
            "collection_efficiency": collection_efficiency,
            "generated_at": datetime.utcnow().isoformat(),
        }

    result = get_or_fetch(cache_key, ttl=3600, fetch_fn=_fetch)
    result["cache_hit"] = cache_hit
    result.setdefault("generated_at", datetime.utcnow().isoformat())
    return result
