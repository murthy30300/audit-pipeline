from dotenv import load_dotenv
import os

load_dotenv()

# Configuration for DB URIs, TTLs, thresholds
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")

PG_CONN_STR = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

CH_HOST = os.getenv("CH_HOST")
CH_PORT = os.getenv("CH_PORT")
CH_DATABASE = os.getenv("CH_DATABASE")
CH_USER = os.getenv("CH_USER")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CLICKHOUSE_CONN_STR = f"clickhouse://{CH_USER}:{CH_PASSWORD}@{CH_HOST}:{CH_PORT}/{CH_DATABASE}"

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")
REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"

CACHE_TTLS = {
    "agent": 30,
    "manager": 60,
    "hr": 60,
}

