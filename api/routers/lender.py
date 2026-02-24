from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException

from api.cache import build_cache_key, get_or_fetch
from api.routers._common import get_clickhouse_client


router = APIRouter()

@router.get("/lender/portfolio-summary")
def portfolio_summary(

    lender_id: Optional[str] = None,

    bucket_filter: Optional[str] = None,

):

    # -----------------------------
    # Validate lender_id
    # -----------------------------

    lender_id = (lender_id or "").strip()

    if not lender_id:

        raise HTTPException(

            status_code=400,

            detail="lender_id is required"

        )

    # -----------------------------
    # Cache Key
    # -----------------------------

    cache_key = build_cache_key(

        "lender",

        "portfolio_summary",

        lender_id=lender_id,

        bucket=bucket_filter or "ALL"

    )


    # -----------------------------
    # Fetch Function
    # -----------------------------

    def _fetch():

        client = get_clickhouse_client()


        # -----------------------------------
        # Portfolio Summary
        # -----------------------------------

        sql_summary = f"""

        SELECT

            sum(total_principal_disbursed),

            sum(npa_amount),

            avg(collection_efficiency_pct)

        FROM lender_portfolio_summary

        WHERE lender_id = '{lender_id}'

        """


        rows = client.execute(sql_summary) or [(0,0,0)]

        total = float(rows[0][0] or 0)

        npa = float(rows[0][1] or 0)

        efficiency = float(rows[0][2] or 0)


        npa_ratio = (

            (npa / total) * 100

            if total > 0 else 0

        )


        # -----------------------------------
        # Bucket Breakdown
        # -----------------------------------

        bucket_sql = f"""

        SELECT

            loan_aging_bucket,

            sum(total_principal_disbursed)

        FROM lender_portfolio_summary

        WHERE lender_id = '{lender_id}'

        GROUP BY loan_aging_bucket

        """


        if bucket_filter:

            bucket_sql += f"""

            HAVING loan_aging_bucket = '{bucket_filter}'

            """


        bucket_rows = client.execute(bucket_sql)


        bucket_breakdown = {

            str(row[0]): float(row[1] or 0)

            for row in bucket_rows

        }


        # -----------------------------------
        # Response
        # -----------------------------------

        return {

            "lender_id": lender_id,

            "total_disbursed": total,

            "npa_ratio": npa_ratio,

            "collection_efficiency": efficiency,

            "bucket_breakdown": bucket_breakdown,

            "generated_at":

                datetime.utcnow().isoformat()

        }


    # -----------------------------
    # Redis Cache TTL = 1 hour
    # -----------------------------

    return get_or_fetch(

        cache_key,

        ttl=3600,

        fetch_fn=_fetch

    )


@router.get("/lender/npa-alerts")
def npa_alerts(limit: int = 50):
    client = get_clickhouse_client()
    rows = client.execute(
        """
        SELECT
            loan_id,
            borrower_id,
            dpd_days,
            overdue_amount,
            loan_aging_bucket,
            due_date
        FROM loans_clean
        WHERE loan_aging_bucket = 'NPA'
        ORDER BY dpd_days DESC
        LIMIT %(limit)s
        """,
        {"limit": limit},
    )

    alerts = [
        {
            "loan_id": r[0],
            "borrower_id": r[1],
            "dpd_days": int(r[2] or 0),
            "overdue_amount": float(r[3] or 0),
            "loan_aging_bucket": r[4],
            "due_date": str(r[5]) if r[5] is not None else None,
        }
        for r in rows
    ]

    return {"alerts": alerts}