"""Loans ETL DAG: PostgreSQL -> ClickHouse bronze layer"""
from datetime import datetime, timedelta, timezone
import logging
import uuid

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.sdk.bases.hook import BaseHook
from airflow.utils.email import send_email
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor
from clickhouse_driver import Client as ClickHouseClient

LOGGER = logging.getLogger(__name__)
EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)
SOURCE_NAME  = "loans"
SOURCE_TABLE = "loans"
BRONZE_TABLE = "loans_raw"
REQUIRED_COLUMNS = ["loan_id", "borrower_id", "principal_amount", "updated_at"]
COERCION_RULES   = [{"kind": "float", "field": "principal_amount"}]


def on_failure_alert(context):
    dag_id   = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    task_id  = context.get("task_instance").task_id if context.get("task_instance") else "unknown_task"
    message  = (f"ETL failure\nDAG: {dag_id}\nTask: {task_id}\n"
                f"Exception: {context.get('exception')}")
    alert_emails = Variable.get("alert_emails", default_var="")
    if alert_emails:
        recipients = [e.strip() for e in alert_emails.split(",") if e.strip()]
        if recipients:
            send_email(to=recipients, subject=f"[Airflow] Failure: {dag_id}.{task_id}", html_content=message)
    slack_webhook = Variable.get("slack_webhook_url", default_var="")
    if slack_webhook:
        try:
            import requests
            requests.post(slack_webhook, json={"text": message}, timeout=10)
        except Exception:
            LOGGER.exception("Failed to send Slack alert")


def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id="postgres_compliance")
    return hook.get_conn()


def get_clickhouse_client():
    conn = BaseHook.get_connection("clickhouse_default")
    return ClickHouseClient(
        host=conn.host,
        port=conn.port or 9000,
        user=conn.login or "default",
        password=conn.password or "",
        database=conn.schema or "compliance",
    )


def _serialize(value):
    """Convert any non-serializable type to string for ClickHouse."""
    import datetime as dt
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    return value


def get_watermark(**_kwargs) -> str:
    watermark_raw = Variable.get(f"watermark_{SOURCE_NAME}", default_var=EPOCH_UTC.isoformat())
    try:
        watermark_dt = datetime.fromisoformat(watermark_raw.replace("Z", "+00:00"))
        if watermark_dt.tzinfo is None:
            watermark_dt = watermark_dt.replace(tzinfo=timezone.utc)
    except ValueError:
        watermark_dt = EPOCH_UTC
    return watermark_dt.isoformat()


def extract_from_postgres(**kwargs):
    watermark    = kwargs["ti"].xcom_pull(task_ids="get_watermark")
    watermark_dt = datetime.fromisoformat(watermark.replace("Z", "+00:00"))
    if watermark_dt.tzinfo is None:
        watermark_dt = watermark_dt.replace(tzinfo=timezone.utc)
    query = (f"SELECT * FROM {SOURCE_TABLE} WHERE updated_at > %s "
             "ORDER BY updated_at ASC LIMIT 50000")
    connection = get_postgres_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (watermark_dt,))
            rows = cursor.fetchall()
            return [{k: _serialize(v) for k, v in row.items()} for row in rows]
    finally:
        connection.close()


def validate_extract(**kwargs) -> bool:
    ti        = kwargs["ti"]
    rows      = ti.xcom_pull(task_ids="extract_from_postgres") or []
    watermark = ti.xcom_pull(task_ids="get_watermark")
    if rows and not isinstance(rows, list):
        raise AirflowException("Extract output must be list[dict]")
    if rows:
        missing = [c for c in REQUIRED_COLUMNS if c not in rows[0]]
        if missing:
            raise AirflowException(f"Schema check failed, missing columns: {missing}")
        for i, rule in enumerate(COERCION_RULES[:10]):
            try:
                if rule["kind"] == "float" and rule["field"] in rows[0]:
                    float(rows[0][rule["field"]])
            except Exception as exc:
                raise AirflowException(f"Type check failed rule {i}: {exc}") from exc
    return True


def load_to_bronze(**kwargs):
    rows = kwargs["ti"].xcom_pull(task_ids="extract_from_postgres") or []
    if not rows:
        LOGGER.info("No rows to load for %s", SOURCE_NAME)
        return {"rows_written": 0, "max_updated_at": None}
    import datetime as _dt
    def safe(v):
        if isinstance(v, (_dt.datetime, _dt.date)):
            return v.isoformat()
        return "" if v is None else v

    etl_ts = datetime.utcnow().isoformat()
    enriched = [{**row, "_etl_loaded_at": etl_ts} for row in rows]
    # Use all columns dynamically (ClickHouse schema matches source)
    available = list(enriched[0].keys())
    client = get_clickhouse_client()
    client.execute(f"INSERT INTO {BRONZE_TABLE} ({', '.join(available)}) VALUES",
                   [tuple(safe(r.get(c)) for c in available) for r in enriched])
    max_updated_at = max((r["updated_at"] for r in enriched if r.get("updated_at")), default=None)
    LOGGER.info("Loaded %d rows into %s", len(enriched), BRONZE_TABLE)
    return {"rows_written": len(enriched), "max_updated_at": max_updated_at}


def update_watermark(**kwargs):
    load_result    = kwargs["ti"].xcom_pull(task_ids="load_to_bronze") or {}
    max_updated_at = load_result.get("max_updated_at")
    if not max_updated_at:
        LOGGER.info("No rows loaded for %s; watermark unchanged", SOURCE_NAME)
        return
    Variable.set(f"watermark_{SOURCE_NAME}", str(max_updated_at))


def run_silver_transform(**kwargs):
    import requests
    sql_path = "/opt/airflow/dags/transforms/silver_loans.sql"
    with open(sql_path) as f:
        sql = f.read()
    resp = requests.post("http://clickhouse:8123/?database=compliance", data=sql)
    if resp.status_code != 200:
        raise AirflowException(f"ClickHouse silver_loans failed: {resp.text}")
    LOGGER.info("Silver loans transform completed")
def ensure_gold_views(**kwargs):

    sql_path = "/opt/airflow/dags/transforms/gold_views.sql"

    client = get_clickhouse_client()

    try:

        with open(sql_path) as f:
            sql = f.read()

        # Remove comment blocks
        import re

        sql = re.sub(
            r"/\*.*?\*/",
            "",
            sql,
            flags=re.DOTALL
        )

        statements = [

            stmt.strip()

            for stmt in sql.split(";")

            if stmt.strip()   # ⭐ VERY IMPORTANT

        ]

        for statement in statements:

            LOGGER.info(
                "Executing Gold SQL:\n%s",
                statement[:150]
            )

            client.execute(statement)

        LOGGER.info(
            "Gold tables + views ensured successfully"
        )

    except Exception as e:

        raise AirflowException(
            f"Gold SQL execution failed : {str(e)}"
        )
default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_alert,
}

dag = DAG(
    dag_id="etl_loans_pg_to_bronze",
    default_args=default_args,
    schedule="*/15 * * * *",
    start_date=datetime(2026, 2, 20),
    catchup=False,
    tags=["bronze", "loans"],
)

with dag:
    t1 = PythonOperator(task_id="get_watermark",         python_callable=get_watermark)
    t2 = PythonOperator(task_id="extract_from_postgres", python_callable=extract_from_postgres)
    t3 = PythonOperator(task_id="validate_extract",      python_callable=validate_extract)
    t4 = PythonOperator(task_id="load_to_bronze",        python_callable=load_to_bronze)
    t5 = PythonOperator(task_id="update_watermark",      python_callable=update_watermark)
    t6 = PythonOperator(task_id="silver_transform",      python_callable=run_silver_transform)
    t7 = PythonOperator(task_id="ensure_gold_views",     python_callable=ensure_gold_views,)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7