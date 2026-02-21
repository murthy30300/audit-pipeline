import pandas as pd

from .base_ingestor import BaseIngestor, ValidationReport

class TTSIngestor(BaseIngestor):
    def load_csv(self):
        df = pd.read_csv(self.csv_path, dtype=str)
        df["ingested_at"] = pd.Timestamp.utcnow()
        df["source_file"] = self.csv_path.name
        return df

    def validate(self, df):
        errors = []
        required_columns = ["tts_id", "customer_id", "delivery_time", "status"]
        for column in required_columns:
            if column not in df.columns:
                errors.append(f"Missing required column: {column}")
            else:
                missing_rows = df.index[df[column].isna() | (df[column].astype(str).str.strip() == "")].tolist()
                if missing_rows:
                    errors.append(f"Null/empty {column} at rows: {missing_rows[:25]}")

        null_counts = {column: int(df[column].isna().sum()) for column in df.columns}
        duplicate_count = int(df["tts_id"].duplicated().sum()) if "tts_id" in df.columns else 0

        return ValidationReport(
            is_valid=len(errors) == 0,
            errors=errors,
            warning_rows=[],
            null_counts=null_counts,
            duplicate_count=duplicate_count,
        )
