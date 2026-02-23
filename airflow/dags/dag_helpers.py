from datetime import datetime, timedelta, timezone
import logging
from typing import Any, Dict, List

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.sdk.bases.hook import BaseHook
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email
from clickhouse_driver import Client as ClickHouseClient
from psycopg2.extras import RealDictCursor


LOGGER = logging.getLogger(__name__)
EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)


def on_failure_alert(context):
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown_task"
    execution_date = context.get("execution_date")
    exception = context.get("exception")
    message = (
        f"ETL failure detected\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Execution Date: {execution_date}\n"
        f"Exception: {exception}"
    )

    alert_emails = Variable.get("alert_emails", default_var="")
    if alert_emails:
        recipients = [email.strip() for email in alert_emails.split(",") if email.strip()]
        if recipients:
            send_email(to=recipients, subject=f"[Airflow] Failure: {dag_id}.{task_id}", html_content=message)

    slack_webhook = Variable.get("slack_webhook_url", default_var="")
    if slack_webhook:
        try:
            import requests

            requests.post(slack_webhook, json={"text": message}, timeout=10)
        except Exception:
            LOGGER.exception("Failed to send Slack alert")


def _get_postgres_connection():
    hook = PostgresHook(postgres_conn_id="postgres_compliance")
    return hook.get_conn()


def _get_clickhouse_client():
    conn = BaseHook.get_connection("clickhouse_default")
    return ClickHouseClient(
        host=conn.host,
        port=conn.port or 9000,
        user=conn.login or "default",
        password=conn.password or "",
        database=conn.schema or "default",
    )


def get_watermark(source_name: str, **_kwargs) -> str:
    variable_name = f"watermark_{source_name}"
    watermark_raw = Variable.get(variable_name, default_var=EPOCH_UTC.isoformat())
    try:
        watermark_dt = datetime.fromisoformat(watermark_raw.replace("Z", "+00:00"))
        if watermark_dt.tzinfo is None:
            watermark_dt = watermark_dt.replace(tzinfo=timezone.utc)
    except ValueError:
        watermark_dt = EPOCH_UTC
    return watermark_dt.isoformat()


def extract_from_postgres(source: str, **kwargs):
    watermark = kwargs["ti"].xcom_pull(task_ids="get_watermark")
    watermark_dt = datetime.fromisoformat(watermark.replace("Z", "+00:00"))
    if watermark_dt.tzinfo is None:
        watermark_dt = watermark_dt.replace(tzinfo=timezone.utc)

    query = (
        f"SELECT * FROM {source} "
        "WHERE updated_at > %s "
        "ORDER BY updated_at ASC "
        "LIMIT 50000"
    )

    connection = _get_postgres_connection()
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


def _coerce_sample_row(row: Dict[str, Any], coercion_rules: List[Dict[str, Any]]) -> None:
    for rule in coercion_rules:
        kind = rule["kind"]
        if kind == "float":
            value = row.get(rule["field"])
            if value is not None:
                float(value)
        elif kind == "float_any":
            fields = rule["fields"]
            value = None
            for field in fields:
                if row.get(field) is not None:
                    value = row[field]
                    break
            if value is not None:
                float(value)
        elif kind == "str":
            value = row.get(rule["field"])
            if value is not None:
                str(value)


def validate_extract(expected_min: int, source: str, required_columns: List[str], coercion_rules: List[Dict[str, Any]], **kwargs) -> bool:
    ti = kwargs["ti"]
    rows = ti.xcom_pull(task_ids="extract_from_postgres") or []
    watermark = ti.xcom_pull(task_ids="get_watermark")
    if str(watermark).startswith("1970-01-01"):
        expected_min = 0

    if rows and not isinstance(rows, list):
        raise AirflowException("Extract output must be list[dict]")

    if rows:
        first_row = rows[0]
        missing_columns = [column for column in required_columns if column not in first_row]
        if missing_columns:
            raise AirflowException(f"Schema check failed, missing columns: {missing_columns}")

        sample_rows = rows[:10]
        for index, row in enumerate(sample_rows):
            try:
                _coerce_sample_row(row, coercion_rules)
                datetime.fromisoformat(str(row["updated_at"]).replace("Z", "+00:00"))
            except Exception as exc:
                raise AirflowException(f"Data type coercion check failed in sample row {index}: {exc}") from exc

    if len(rows) == 0 and expected_min > 0:
        connection = _get_postgres_connection()
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT 1
                    FROM ingestion_log
                    WHERE source = %s
                      AND timestamp > %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (source, watermark),
                )
                has_recent_writes = cursor.fetchone() is not None
            if has_recent_writes:
                raise AirflowException(
                    "0 extracted rows but ingestion_log shows recent writes; possible watermark issue"
                )
        finally:
            connection.close()

    return True


def load_to_bronze(table_name: str, **kwargs):
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
    insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES"

    client = _get_clickhouse_client()
    client.execute(insert_query, values)

    max_updated_at = max(row["updated_at"] for row in enriched_rows if row.get("updated_at") is not None)
    return {"rows_written": len(enriched_rows), "max_updated_at": max_updated_at}


def update_watermark(source: str, **kwargs):
    load_result = kwargs["ti"].xcom_pull(task_ids="load_to_bronze") or {}
    max_updated_at = load_result.get("max_updated_at")
    if not max_updated_at:
        LOGGER.info("No rows loaded for source=%s; watermark unchanged", source)
        return
    Variable.set(f"watermark_{source}", str(max_updated_at))


def build_bronze_dag(
    dag_id: str,
    source_name: str,
    source_table: str,
    bronze_table: str,
    silver_dag_id: str,
    required_columns: List[str],
    coercion_rules: List[Dict[str, Any]],
):
    default_args = {
        "owner": "airflow",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_failure_alert,
    }

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule="*/15 * * * *",
        start_date=datetime(2026, 2, 20),
        catchup=False,
    ) as dag:
        t1_get_watermark = PythonOperator(
            task_id="get_watermark",
            python_callable=get_watermark,
            op_kwargs={"source_name": source_name},
        )

        t2_extract_from_postgres = PythonOperator(
            task_id="extract_from_postgres",
            python_callable=extract_from_postgres,
            op_kwargs={"source": source_table},
        )

        t3_validate_extract = PythonOperator(
            task_id="validate_extract",
            python_callable=validate_extract,
            op_kwargs={
                "expected_min": 1,
                "source": source_name,
                "required_columns": required_columns,
                "coercion_rules": coercion_rules,
            },
        )

        t4_load_to_bronze = PythonOperator(
            task_id="load_to_bronze",
            python_callable=load_to_bronze,
            op_kwargs={"table_name": bronze_table},
        )

        t5_update_watermark = PythonOperator(
            task_id="update_watermark",
            python_callable=update_watermark,
            op_kwargs={"source": source_name},
        )

        t6_trigger_silver = TriggerDagRunOperator(
            task_id="trigger_silver_transform",
            trigger_dag_id=silver_dag_id,
            conf={"source": source_name},
            wait_for_completion=False,
        )

        t1_get_watermark >> t2_extract_from_postgres >> t3_validate_extract >> t4_load_to_bronze >> t5_update_watermark >> t6_trigger_silver

    return dag
