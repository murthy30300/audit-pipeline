from dag_helpers import build_bronze_dag


etl_loans_pg_to_bronze = build_bronze_dag(
    dag_id="etl_loans_pg_to_bronze",
    source_name="loans",
    source_table="loans",
    bronze_table="loans_raw",
    silver_dag_id="silver_loans_transform",
    required_columns=["loan_id", "borrower_id", "principal_amount", "disbursement_date", "updated_at"],
    coercion_rules=[{"kind": "float", "field": "principal_amount"}],
)

# __all__ = ['dag']
