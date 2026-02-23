#!/usr/bin/env python3
import sys
from pathlib import Path

# Works both locally and inside Docker container
HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))

from ingestion.loan_ingestor import LoanIngestor
from ingestion.payment_ingestor import PaymentIngestor
from ingestion.call_ingestor import CallIngestor
from ingestion.sms_ingestor import SMSIngestor
from ingestion.crm_ingestor import CRMIngestor
from ingestion.tts_ingestor import TTSIngestor

# Inside Docker: postgres-compliance hostname, outside: localhost:5434
PG_CONN = "postgresql://compliance_user:compliance_pass@postgres-compliance:5432/compliance_audit"
DATASETS = HERE / "datasets"

ingestors = [
    ("loans", LoanIngestor("loans", PG_CONN, DATASETS / "nbfc_loans.csv")),
    ("payments", PaymentIngestor("payments", PG_CONN, DATASETS / "payment_system.csv")),
    ("calls", CallIngestor("calls", PG_CONN, DATASETS / "call_system.csv")),
    ("messages", SMSIngestor("messages", PG_CONN, DATASETS / "sms_whatsapp.csv")),
    ("crm", CRMIngestor("crm", PG_CONN, DATASETS / "crm_system.csv")),
    ("tts", TTSIngestor("tts", PG_CONN, DATASETS / "tts_system.csv")),
]

print("Loading data via ingestion modules...\n")
for name, ingestor in ingestors:
    result = ingestor.run()
    status = "✓" if result.status == "success" else "✗"
    print(f"{status} {name}: {result.rows_written} rows")
