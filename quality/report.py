# Validation result model for reporting
from dataclasses import dataclass
from typing import List, Dict, Optional
from datetime import datetime

@dataclass
class ValidationReport:
    is_valid: bool
    source: str
    total_rows: int
    errors: List[str]
    warnings: List[str]
    null_counts: Dict[str, int]
    duplicate_count: int
    checked_at: datetime
