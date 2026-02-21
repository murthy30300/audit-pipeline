# Audit Pipeline

This repository implements a multi-stage data pipeline as described in plan.html:

- Ingestion: CSV → PostgreSQL (Python, pandas, psycopg2)
- ETL: PostgreSQL → ClickHouse (Airflow DAGs)
- Transforms: Bronze → Silver → Gold (ClickHouse SQL)
- Serving: FastAPI + Redis
- Data Quality: Validators at ingestion and Silver→Gold gate

## Folder Structure
- ingestion/: Source-specific ingestors
- airflow/dags/: ETL DAGs
- transforms/: SQL transforms
- api/: FastAPI app and routers
- quality/: Data quality checks
- datasets/: Input CSVs

## Getting Started
1. Place your CSVs in datasets/
2. Run the appropriate ingestor script to load data into PostgreSQL
3. Start Airflow to run ETL DAGs
4. Apply SQL transforms in transforms/
5. Start FastAPI app for serving

See plan.html for detailed blueprint and execution order.
