from __future__ import annotations

import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schema" / "rwa_v1.sql"
DEFAULT_DB_PATH = ROOT / "data" / "rwa_v1" / "hrevn_rwa_v1.db"


def ensure_rwa_v1_schema(db_path: Path | None = None) -> Path:
    target = Path(db_path) if db_path else DEFAULT_DB_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with sqlite3.connect(target) as conn:
        conn.executescript(schema_sql)
        conn.commit()
    return target
