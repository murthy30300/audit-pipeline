# Abstract base class for all ingestors
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values
from psycopg2 import sql


@dataclass
class ValidationReport:
    is_valid: bool
    errors: List[str]
    warning_rows: List[int]
    null_counts: Dict[str, int]
    duplicate_count: int


@dataclass
class WriteResult:
    rows_written: int
    rows_skipped: int
    status: str  # success | partial | failed
    error_msg: Optional[str]


class BaseIngestor(ABC):
    def __init__(self, source_name, pg_conn_str, csv_path):
        self.source_name = source_name
        self.pg_conn_str = pg_conn_str
        self.csv_path = Path(csv_path)

        self.logger = logging.getLogger(f"ingestion.{self.source_name}")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s %(levelname)s [%(name)s] %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

        self.source_metadata = {
            "source": self.source_name,
            "csv_path": str(self.csv_path),
            "file_name": self.csv_path.name,
            "started_at": datetime.now(timezone.utc),
        }

        self.pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=5,
            dsn=self.pg_conn_str,
        )

    @abstractmethod
    def load_csv(self):
        pass

    @abstractmethod
    def validate(self, df):
        pass

    def _table_name(self) -> str:
        return self.source_name

    def _resolve_primary_key(self, df: pd.DataFrame) -> str:
        if "id" in df.columns:
            return "id"
        source_pk = f"{self.source_name.rstrip('s')}_id"
        if source_pk in df.columns:
            return source_pk
        return df.columns[0]

    def _ensure_ingestion_log_table(self, cursor) -> None:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ingestion_log (
                id BIGSERIAL PRIMARY KEY,
                source TEXT NOT NULL,
                file_name TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                status TEXT NOT NULL,
                error_msg TEXT
            )
            """
        )

    def write_to_postgres(self, df, report):
        if not report.is_valid:
            error_msg = "; ".join(report.errors) if report.errors else "Validation failed"
            self.logger.error(
                "Ingestion aborted for source=%s due to validation errors: %s",
                self.source_name,
                error_msg,
            )
            return WriteResult(
                rows_written=0,
                rows_skipped=len(df.index),
                status="failed",
                error_msg=error_msg,
            )

        conn = None
        rows_written = 0
        rows_skipped = len(report.warning_rows)

        try:
            conn = self.pool.getconn()
            conn.autocommit = False

            with conn.cursor() as cursor:
                self._ensure_ingestion_log_table(cursor)

                if not df.empty:
                    table_name = self._table_name()
                    columns = [str(column) for column in df.columns]
                    primary_key = self._resolve_primary_key(df)

                    insert_columns = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
                    conflict_target = sql.Identifier(primary_key)
                    update_columns = [c for c in columns if c != primary_key]
                    if update_columns:
                        update_set = sql.SQL(", ").join(
                            sql.SQL("{} = EXCLUDED.{}").format(
                                sql.Identifier(c), sql.Identifier(c)
                            )
                            for c in update_columns
                        )
                        query = sql.SQL(
                            "INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) DO UPDATE SET {}"
                        ).format(
                            sql.Identifier(table_name),
                            insert_columns,
                            conflict_target,
                            update_set,
                        )
                    else:
                        query = sql.SQL(
                            "INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) DO NOTHING"
                        ).format(
                            sql.Identifier(table_name),
                            insert_columns,
                            conflict_target,
                        )

                    records = [tuple(row) for row in df.itertuples(index=False, name=None)]
                    execute_values(cursor, query.as_string(conn), records)
                    rows_written = len(records)

                status = "partial" if rows_skipped > 0 else "success"
                cursor.execute(
                    """
                    INSERT INTO ingestion_log (source, file_name, row_count, timestamp, status, error_msg)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        self.source_name,
                        self.source_metadata["file_name"],
                        rows_written,
                        datetime.now(timezone.utc),
                        status,
                        None,
                    ),
                )

            conn.commit()
            self.logger.info(
                "Ingestion completed for source=%s with status=%s rows_written=%s rows_skipped=%s",
                self.source_name,
                status,
                rows_written,
                rows_skipped,
            )
            return WriteResult(
                rows_written=rows_written,
                rows_skipped=rows_skipped,
                status=status,
                error_msg=None,
            )
        except Exception as exc:
            if conn is not None:
                conn.rollback()
            self.logger.exception("Ingestion failed for source=%s", self.source_name)
            return WriteResult(
                rows_written=rows_written,
                rows_skipped=rows_skipped,
                status="failed",
                error_msg=str(exc),
            )
        finally:
            if conn is not None:
                self.pool.putconn(conn)

    def run(self):
        df = self.load_csv()
        report = self.validate(df)
        result = self.write_to_postgres(df, report)
        return result
