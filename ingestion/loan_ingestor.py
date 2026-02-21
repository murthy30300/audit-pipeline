from datetime import datetime, timezone

import pandas as pd

from .base_ingestor import BaseIngestor, ValidationReport

class LoanIngestor(BaseIngestor):
    def load_csv(self):
        dtype_map = {
            "LoanID": "string",
            "loan_id": "string",
            "BorrowerID": "string",
            "borrower_id": "string",
            "customer_id": "string",
            "phone_number": "string",
            "Amt": "float64",
            "Principal": "float64",
            "principal_amount": "float64",
            "loan_amount": "float64",
            "interest_rate": "float64",
            "DisbursementDate": "string",
            "disbursement_date": "string",
            "LoanStatus": "string",
            "loan_status": "string",
        }
        df = pd.read_csv(self.csv_path, dtype=dtype_map)

        canonical_map = {
            "LoanID": "loan_id",
            "loanid": "loan_id",
            "BorrowerID": "borrower_id",
            "CustomerID": "borrower_id",
            "customer_id": "borrower_id",
            "Amt": "principal_amount",
            "Principal": "principal_amount",
            "loan_amount": "principal_amount",
            "DisbursementDate": "disbursement_date",
            "LoanStatus": "loan_status",
        }
        df = df.rename(columns=canonical_map)
        df["ingested_at"] = datetime.utcnow().replace(tzinfo=timezone.utc)
        df["source_file"] = self.csv_path.name
        return df

    def validate(self, df):
        errors = []
        warning_rows = set()

        required_columns = [
            "loan_id",
            "borrower_id",
            "principal_amount",
            "disbursement_date",
        ]
        for column in required_columns:
            if column not in df.columns:
                errors.append(f"Missing required column: {column}")

        if "loan_id" in df.columns:
            null_loan_id_rows = df.index[df["loan_id"].isna() | (df["loan_id"].astype(str).str.strip() == "")].tolist()
            if null_loan_id_rows:
                errors.append(f"Null/empty loan_id at rows: {null_loan_id_rows[:25]}")

            duplicate_mask = (
                df["loan_id"].notna()
                & (df["loan_id"].astype(str).str.strip() != "")
                & df["loan_id"].duplicated(keep=False)
            )
            duplicate_rows = df.index[duplicate_mask].tolist()
            if duplicate_rows:
                errors.append(f"Duplicate loan_id found at rows: {duplicate_rows[:25]}")

        for column in ["borrower_id", "principal_amount", "disbursement_date"]:
            if column in df.columns:
                missing_rows = df.index[df[column].isna() | (df[column].astype(str).str.strip() == "")].tolist()
                if missing_rows:
                    errors.append(f"Null/empty {column} at rows: {missing_rows[:25]}")

        if "principal_amount" in df.columns:
            principal = pd.to_numeric(df["principal_amount"], errors="coerce")
            invalid_rows = df.index[principal.isna() | (principal <= 0)].tolist()
            if invalid_rows:
                errors.append(f"principal_amount must be > 0 at rows: {invalid_rows[:25]}")

        if "interest_rate" in df.columns:
            interest = pd.to_numeric(df["interest_rate"], errors="coerce")
            invalid_rows = df.index[interest.notna() & ((interest < 0) | (interest > 100))].tolist()
            if invalid_rows:
                errors.append(f"interest_rate must be between 0 and 100 at rows: {invalid_rows[:25]}")

        if "disbursement_date" in df.columns:
            disbursement_dates = pd.to_datetime(df["disbursement_date"], errors="coerce", utc=True)
            today = pd.Timestamp.now(tz="UTC")
            too_old = pd.Timestamp("2010-01-01", tz="UTC")

            invalid_date_rows = df.index[disbursement_dates.isna()].tolist()
            if invalid_date_rows:
                errors.append(f"Invalid disbursement_date format at rows: {invalid_date_rows[:25]}")

            future_rows = df.index[disbursement_dates.notna() & (disbursement_dates > today)].tolist()
            if future_rows:
                errors.append(f"disbursement_date cannot be in future at rows: {future_rows[:25]}")

            old_rows = df.index[disbursement_dates.notna() & (disbursement_dates < too_old)].tolist()
            if old_rows:
                errors.append(f"disbursement_date cannot be before 2010-01-01 at rows: {old_rows[:25]}")

        if "loan_status" in df.columns:
            allowed_status = {"ACTIVE", "CLOSED", "NPA", "WRITTEN_OFF"}
            normalized_status = df["loan_status"].astype(str).str.upper().str.strip()
            invalid_status_rows = df.index[~normalized_status.isin(allowed_status)].tolist()
            if invalid_status_rows:
                errors.append(
                    f"Invalid loan_status (allowed: ACTIVE,CLOSED,NPA,WRITTEN_OFF) at rows: {invalid_status_rows[:25]}"
                )

        if "phone_number" in df.columns:
            warning_rows.update(
                df.index[df["phone_number"].isna() | (df["phone_number"].astype(str).str.strip() == "")].tolist()
            )

        null_counts = {column: int(df[column].isna().sum()) for column in df.columns}
        duplicate_count = int(df["loan_id"].duplicated().sum()) if "loan_id" in df.columns else 0

        return ValidationReport(
            is_valid=len(errors) == 0,
            errors=errors,
            warning_rows=sorted(warning_rows),
            null_counts=null_counts,
            duplicate_count=duplicate_count,
        )
