# Configuration for DB URIs, TTLs, thresholds
PG_CONN_STR = "postgresql://admin:admin@localhost:5432/audit_db"
#CLICKHOUSE_CONN_STR = "clickhouse://default:9a3GEKq3.pTJc@localhost:9000"
REDIS_URL = "redis://localhost:6379/0"
CACHE_TTLS = {
    "agent": 30,
    "manager": 60,
    "hr": 60
}
client = clickhouse_connect.get_client(
        host='hikwepe139.ap-south-1.aws.clickhouse.cloud',
        user='default',
        password='9a3GEKq3.pTJc',
        secure=True
    )

