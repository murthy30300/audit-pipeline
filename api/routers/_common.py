from base64 import urlsafe_b64decode
from datetime import date
import json
from typing import Any, Dict, Optional

from clickhouse_driver import Client as ClickHouseClient
from fastapi import HTTPException


# ===========================================================
# JWT CLAIMS (DEMO SIMPLE DECODE)
# ===========================================================

def _decode_jwt_payload(token: str) -> Dict[str, Any]:

    parts = token.split(".")

    if len(parts) != 3:
        raise HTTPException(status_code=401, detail="Invalid JWT")

    payload_part = parts[1]

    padding = "=" * ((4 - len(payload_part) % 4) % 4)

    try:

        payload_raw = urlsafe_b64decode(payload_part + padding)

        payload = json.loads(payload_raw.decode("utf-8"))

    except Exception:
        raise HTTPException(status_code=401, detail="Invalid JWT payload")

    if not isinstance(payload, dict):
        raise HTTPException(status_code=401, detail="Invalid JWT payload")

    return payload


def get_claims_from_auth(authorization: Optional[str]) -> Dict[str, Any]:

    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid Authorization scheme")

    token = authorization.split(" ", 1)[1].strip()

    if not token:
        raise HTTPException(status_code=401, detail="Missing token")

    return _decode_jwt_payload(token)


def require_role(claims: Dict[str, Any], expected_role: str):

    role = str(claims.get("role", "")).lower()

    if role != expected_role.lower():
        raise HTTPException(status_code=403, detail="Forbidden for role")


# ===========================================================
# CLICKHOUSE CONNECTION (HARDCODED)
# ===========================================================

def get_clickhouse_client() -> ClickHouseClient:

    return ClickHouseClient(

        host="localhost",     # docker clickhouse host

        port=9000,            # native tcp

        user="default",

        password="",          # add if password exists

        database="compliance" # 
    )


# ===========================================================
# QUERY HELPER
# ===========================================================

def query_ch(query: str, params: Optional[Dict[str, Any]] = None):

    client = get_clickhouse_client()

    return client.execute(

        query,

        params or {},

        with_column_types=False

    )


def today_iso() -> str:

    return date.today().isoformat()