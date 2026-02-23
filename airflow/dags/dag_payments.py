"""Payments ETL DAG: PostgreSQL -> ClickHouse bronze layer"""
from datetime import datetime, timedelta, timezone
import logging

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from psycopg2.extras import RealDictCursor

from dag_helpers import on_failure_alert, get_postgres_connection, get_clickhouse_client, EPOCH_UTC


LOGGER = logging.getLogger(__name__)
SOURCE_NAME = "payments"
SOURCE_TABLE = "payments"
BRONZE_TABLE = "payments_raw"
SILVER_DAG_ID = "silver_payments_transform"
REQUIRED_COLUMNS = ["payment_id", "loan_id", "amount", "payment_date", "updated_at"]
COERCION_RULES = [{"kind": "float", "field": "amount"}]


def get_watermark(**_kwargs) -> str:
    variable_name = f"watermark_{SOURCE_NAME}"
    watermark_raw = Variable.get(variable_name, default_var=EPOCH_UTC.isoformat())
    try:
        watermark_dt = datetime.fromisoformat(watermark_raw.replace("Z", "+00:00"))
        if watermark_dt.tzinfo is None:
            watermark_dt = watermark_dt.replace(tzinfo=timezone.utc)
    except ValueError:
        watermark_dt = EPOCH_UTC
    return watermark_dt.isoformat()


def extract_from_postgres(**kwargs):
    watermark = kwargs["ti"].xcom_pull(task_ids="get_watermark")
    watermark_dt = datetime.fromisoformat(watermark.replace("Z", "+00:00"))
    if watermark_dt.tzinfo is None:
        watermark_dt = watermark_dt.replace(tzinfo=timezone.utc)

    query = (
        f"SELECT * FROM {SOURCE_TABLE} "
        "WHERE updated_at > %s "
        "ORDER BY updated_at ASC "
        "LIMIT 50000"
    )

    connection = get_postgres_connection()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (watermark_dt,))
            rows = cursor.fetchall()
            normalized_rows = []
            for row in rows:
                normalized = {}
                for key, value in row.items():
                    if isinstance(value, datetime):
                        normalized[key] = value.isoformat()
                    else:
                        normalized[key] = value
                normalized_rows.append(normalized)
            return normalized_rows
    finally:
        connection.close()


def validate_extract(**kwargs) -> bool:
    ti = kwargs["ti"]
    rows = ti.xcom_pull(task_ids="extract_from_postgres") or []
    watermark = ti.xcom_pull(task_ids="get_watermark")
    expected_min = 0 if str(watermark).startswith("1970-01-01") else 1

    if rows and not isinstance(rows, list):
        raise AirflowException("Extract output must be list[dict]")

    if rows:
        first_row = rows[0]
        missing_columns = [col for col in REQUIRED_COLUMNS if col not in first_row]
        if missing_columns:
            raise AirflowException(f"Schema check failed, missing columns: {missing_columns}")

        for index, rule in enumerate(COERCION_RULES[:10]):
            try:
                if rule["kind"] == "float" and rule["field"] in rows[0]:
                    float(rows[0][rule["field"]])
            except Exception as exc:
                raise AirflowException(f"Data type check failed at rule {index}: {exc}") from exc

        try:
            datetime.fromisoformat(str(rows[0]["updated_at"]).replace("Z", "+00:00"))
        except Exception as exc:
            raise AirflowException(f"Updated_at timestamp invalid: {exc}") from exc

    if len(rows) == 0 and expected_min > 0:
        connection = get_postgres_connection()
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    f"SELECT COUNT(*) as cnt FROM {SOURCE_TABLE} WHERE updated_at > %s",
                    (watermark,)
                )
                result = cursor.fetchone()
                if result and result["cnt"] > 0:
                    raise AirflowException(
                        f"0 extracted rows but {result['cnt']} new rows exist in source"
                    )
        finally:
            connection.close()

    return True


def load_to_bronze(**kwargs):
    rows = kwargs["ti"].xcom_pull(task_ids="extract_from_postgres") or []
    if not rows:
        return {"rows_written": 0, "max_updated_at": None}

    etl_loaded_at = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    enriched_rows = []
    for row in rows:
        enriched = dict(row)
        enriched["_etl_loaded_at"] = etl_loaded_at
        enriched_rows.append(enriched)

    columns = list(enriched_rows[0].keys())
    values = [tuple(row.get(column) for column in columns) for row in enriched_rows]
    insert_query = f"INSERT INTO {BRONZE_TABLE} ({', '.join(columns)}) VALUES"

    client = get_clickhouse_client()
    client.execute(insert_query, values)

    max_updated_at = max(row["updated_at"] for row in enriched_rows if row.get("updated_at"))
    return {"rows_written": len(enriched_rows), "max_updated_at": max_updated_at}


def update_watermark(**kwargs):
    load_result = kwargs["ti"].xcom_pull(task_ids="load_to_bronze") or {}
    max_updated_at = load_result.get("max_updated_at")
    if not max_updated_at:
        LOGGER.info("No rows loaded for %s; watermark unchanged", SOURCE_NAME)
        return
    Variable.set(f"watermark_{SOURCE_NAME}", str(max_updated_at))


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_alert,
}

dag = DAG(
    dag_id="etl_payments_pg_to_bronze",
    default_args=default_args,
    schedule="*/15 * * * *",
    start_date=datetime(2026, 2, 20),
    catchup=False,
    tags=["bronze", "payments"],
)

with dag:
    t1 = PythonOperator(task_id="get_watermark", python_callable=get_watermark)
    t2 = PythonOperator(task_id="extract_from_postgres", python_callable=extract_from_postgres)
    t3 = PythonOperator(task_id="validate_extract", python_callable=validate_extract)
    t4 = PythonOperator(task_id="load_to_bronze", python_callable=load_to_bronze)
    t5 = PythonOperator(task_id="update_watermark", python_callable=update_watermark)
    t6 = TriggerDagRunOperator(
        task_id="trigger_silver_transform",
        trigger_dag_id=SILVER_DAG_ID,
        conf={"source": SOURCE_NAME},
        wait_for_completion=False,
    )
    t1 >> t2 >> t3 >> t4 >> t5 >> t6
