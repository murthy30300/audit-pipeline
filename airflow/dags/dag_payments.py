from dag_helpers import build_bronze_dag


dag = build_bronze_dag(
    dag_id="etl_payments_pg_to_bronze",
    source_name="payments",
    source_table="payments",
    bronze_table="payments_raw",
    silver_dag_id="silver_payments_transform",
    required_columns=["payment_id", "loan_id", "amount", "payment_date", "updated_at"],
    coercion_rules=[{"kind": "float", "field": "amount"}],
)
