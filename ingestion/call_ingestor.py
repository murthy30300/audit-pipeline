import pandas as pd
from psycopg2 import sql

from .base_ingestor import BaseIngestor, ValidationReport

class CallIngestor(BaseIngestor):
    def load_csv(self):
        dtype_map = {
            "call_id": "string",
            "CallID": "string",
            "loan_id": "string",
            "LoanID": "string",
            "agent_id": "string",
            "AgentID": "string",
            "call_start_time": "string",
            "call_duration_sec": "float64",
            "duration_seconds": "float64",
            "transcript": "string",
            "call_status": "string",
        }
        df = pd.read_csv(self.csv_path, dtype=dtype_map)

        canonical_map = {
            "CallID": "call_id",
            "LoanID": "loan_id",
            "AgentID": "agent_id",
            "duration_seconds": "call_duration_sec",
        }
        df = df.rename(columns=canonical_map)
        df["ingested_at"] = pd.Timestamp.utcnow()
        df["source_file"] = self.csv_path.name
        return df

    def validate(self, df):
        errors = []

        required_columns = ["call_id", "loan_id", "agent_id", "call_start_time"]
        for column in required_columns:
            if column not in df.columns:
                errors.append(f"Missing required column: {column}")

        for column in required_columns:
            if column in df.columns:
                missing_rows = df.index[df[column].isna() | (df[column].astype(str).str.strip() == "")].tolist()
                if missing_rows:
                    errors.append(f"Null/empty {column} at rows: {missing_rows[:25]}")

        if "call_duration_sec" in df.columns:
            duration = pd.to_numeric(df["call_duration_sec"], errors="coerce")
            invalid_rows = df.index[duration.isna() | (duration < 0) | (duration > 7200)].tolist()
            if invalid_rows:
                errors.append(f"call_duration_sec must be between 0 and 7200 at rows: {invalid_rows[:25]}")

        if "call_status" in df.columns and "transcript" in df.columns:
            status = df["call_status"].astype(str).str.upper().str.strip()
            transcript = df["transcript"].astype(str).str.strip()
            invalid_rows = df.index[(status == "COMPLETED") & (transcript == "")].tolist()
            if invalid_rows:
                errors.append(
                    f"transcript must be non-empty when call_status=COMPLETED at rows: {invalid_rows[:25]}"
                )

        if "loan_id" in df.columns:
            loan_ids = (
                df["loan_id"]
                .dropna()
                .astype(str)
                .str.strip()
            )
            loan_ids = [value for value in loan_ids.unique().tolist() if value]
            if loan_ids:
                conn = None
                try:
                    conn = self.pool.getconn()
                    with conn.cursor() as cursor:
                        cursor.execute(
                            sql.SQL("SELECT loan_id FROM {} WHERE loan_id = ANY(%s)").format(
                                sql.Identifier("loans")
                            ),
                            (loan_ids,),
                        )
                        existing = {row[0] for row in cursor.fetchall()}

                    missing_fk_rows = df.index[
                        df["loan_id"].notna()
                        & df["loan_id"].astype(str).str.strip().ne("")
                        & ~df["loan_id"].astype(str).isin(existing)
                    ].tolist()
                    if missing_fk_rows:
                        errors.append(
                            f"loan_id does not exist in loans table at rows: {missing_fk_rows[:25]}"
                        )
                except Exception as exc:
                    errors.append(f"FK validation query failed: {exc}")
                finally:
                    if conn is not None:
                        self.pool.putconn(conn)

        null_counts = {column: int(df[column].isna().sum()) for column in df.columns}
        duplicate_count = int(df["call_id"].duplicated().sum()) if "call_id" in df.columns else 0

        return ValidationReport(
            is_valid=len(errors) == 0,
            errors=errors,
            warning_rows=[],
            null_counts=null_counts,
            duplicate_count=duplicate_count,
        )
