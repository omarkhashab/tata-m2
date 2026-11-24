"""One place for paths, connection settings and names shared by every M2 stage.

Everything can be overridden with an environment variable so the same code runs on a
laptop (services on localhost) and inside docker compose (services by name).
"""

from __future__ import annotations

import os
from pathlib import Path

M2_DIR = Path(__file__).resolve().parents[1]
REPO_DIR = M2_DIR.parent
M1_DIR = REPO_DIR / "M1"

# the raw pack lives once, in M1/data/raw (committed byte-for-byte there)
RAW_DIR = Path(os.environ.get("KESTREL_RAW_DIR", M1_DIR / "data" / "raw"))
OUT_DIR = Path(os.environ.get("KESTREL_M2_OUT", M2_DIR / "output"))

DB_URL = os.environ.get("KESTREL_DB_URL", "postgresql+psycopg2://kestrel:kestrel@localhost:5432/kestrel")
DW_SCHEMA = os.environ.get("KESTREL_DW_SCHEMA", "kestrel_dw")
ANALYTICS_SCHEMA = os.environ.get("KESTREL_ANALYTICS_SCHEMA", "kestrel_analytics")

KAFKA_BOOTSTRAP = os.environ.get("KESTREL_KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RAW = "kestrel.agent.events"                   # Appendix B: raw topic, 3 partitions, key = session_id
TOPIC_CURATED = "kestrel.agent.events.curated"
TOPIC_REJECTED = "kestrel.agent.events.rejected"
TOPIC_PARTITIONS = 3
CONSUMER_GROUP = os.environ.get("KESTREL_CONSUMER_GROUP", "kestrel-tata")   # one group per team
WINDOW_GROUP = os.environ.get("KESTREL_WINDOW_GROUP", "kestrel-tata-windows")

WATERMARK_S = 60          # Appendix B: 60 s lateness on event_time
WINDOW_S = 60             # bonus: tumbling one-minute windows

# the pack's batch window (Dataset Description 1): used by range and freshness checks
PACK_START = "2026-01-31T00:00:00Z"
PACK_END = "2026-04-01T00:00:00Z"             # exclusive; the stream replays 2026-04-01 00:00-00:32
STREAM_END = "2026-04-02T00:00:00Z"


def sqlalchemy_url() -> str:
    return DB_URL


def psycopg_dsn() -> str:
    """The same database as a libpq DSN (for psycopg2.connect)."""
    from sqlalchemy.engine import make_url
    u = make_url(DB_URL)
    return f"host={u.host} port={u.port or 5432} dbname={u.database} user={u.username} password={u.password}"


def jdbc_url() -> tuple[str, dict]:
    """The same database as a Spark JDBC url + properties."""
    from sqlalchemy.engine import make_url
    u = make_url(DB_URL)
    return (f"jdbc:postgresql://{u.host}:{u.port or 5432}/{u.database}",
            {"user": u.username, "password": u.password, "driver": "org.postgresql.Driver"})
