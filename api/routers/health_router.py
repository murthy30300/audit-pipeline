from fastapi import APIRouter

from clickhouse_driver import Client
import redis


router = APIRouter()


def get_clickhouse():

    return Client(

        host="localhost",
        port=9000,
        user="default",
        password="",
        database="compliance",

    )


@router.get("/health")
def health():

    services = {}

    # ClickHouse check
    try:

        ch = get_clickhouse()

        tables = [

            "lender_portfolio_summary",
            "agent_assigned_loans",
            "manager_branch_summary",
            "hr_agent_performance_daily",

        ]

        counts = {}

        for t in tables:

            rows = ch.execute(

                f"SELECT count(*) FROM {t}"

            )

            counts[t] = rows[0][0]

        services["clickhouse"] = "ok"

        services["table_counts"] = counts

    except Exception:

        services["clickhouse"] = "error"


    # Redis check
    try:

        r = redis.Redis(

            host="localhost",
            port=6379,

        )

        r.ping()

        services["redis"] = "ok"

    except Exception:

        services["redis"] = "error"


    return {

        "status": "ok",

        "services": services,

    }