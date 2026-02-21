from dag_helpers import build_bronze_dag


dag = build_bronze_dag(
    dag_id="etl_messages_pg_to_bronze",
    source_name="messages",
    source_table="messages",
    bronze_table="messages_raw",
    silver_dag_id="silver_messages_transform",
    required_columns=["message_id", "customer_id", "channel", "sent_time", "updated_at"],
    coercion_rules=[{"kind": "str", "field": "message_id"}],
)
