from dag_helpers import build_bronze_dag


dag = build_bronze_dag(
    dag_id="etl_crm_pg_to_bronze",
    source_name="crm",
    source_table="crm",
    bronze_table="crm_raw",
    silver_dag_id="silver_crm_transform",
    required_columns=["customer_id", "name", "phone_number", "updated_at"],
    coercion_rules=[{"kind": "str", "field": "customer_id"}],
)
