from dag_helpers import build_bronze_dag


etl_calls_pg_to_bronze = build_bronze_dag(
    dag_id="etl_calls_pg_to_bronze",
    source_name="calls",
    source_table="calls",
    bronze_table="calls_raw",
    silver_dag_id="silver_calls_transform",
    required_columns=["call_id", "loan_id", "agent_id", "call_start_time", "updated_at"],
    coercion_rules=[{"kind": "float_any", "fields": ["call_duration_sec", "duration_seconds"]}],
)

# __all__ = ['dag']
