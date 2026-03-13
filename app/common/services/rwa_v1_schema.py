from __future__ import annotations

import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schema" / "rwa_v1.sql"
DEFAULT_DB_PATH = ROOT / "data" / "rwa_v1" / "hrevn_rwa_v1.db"


def _ensure_visit_capture_columns(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(rwa_visits)").fetchall()}
    wanted = {
        "direct_capture_session_status": "TEXT NOT NULL DEFAULT 'open'",
        "direct_capture_started_at_utc": "TEXT",
        "direct_capture_last_activity_at_utc": "TEXT",
        "direct_capture_closed_at_utc": "TEXT",
        "direct_capture_closed_reason": "TEXT",
        "direct_capture_window_minutes": "INTEGER NOT NULL DEFAULT 0",
    }
    for name, ddl in wanted.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE rwa_visits ADD COLUMN {name} {ddl}")


def ensure_rwa_v1_schema(db_path: Path | None = None) -> Path:
    target = Path(db_path) if db_path else DEFAULT_DB_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with sqlite3.connect(target) as conn:
        conn.executescript(schema_sql)
        _ensure_visit_capture_columns(conn)
        conn.commit()
    return target
