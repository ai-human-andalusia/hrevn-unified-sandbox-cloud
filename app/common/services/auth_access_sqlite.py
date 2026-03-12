from __future__ import annotations

import hashlib
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


@dataclass(frozen=True)
class AuthRequestContext:
    ip_public: str
    user_agent: str
    request_origin: str


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_auth_access_db(db_path: Path) -> None:
    with _connect(db_path) as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS auth_login_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_email TEXT,
              user_role TEXT,
              identifier_attempted TEXT,
              event_type TEXT NOT NULL,
              success_flag INTEGER NOT NULL DEFAULT 0,
              failure_reason TEXT,
              ip_public TEXT,
              user_agent TEXT,
              request_origin TEXT,
              created_at_utc TEXT NOT NULL,
              details_json TEXT
            );

            CREATE TABLE IF NOT EXISTS auth_active_sessions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              session_id TEXT UNIQUE NOT NULL,
              user_email TEXT NOT NULL,
              user_role TEXT NOT NULL,
              session_token_hash TEXT NOT NULL,
              ip_public TEXT,
              user_agent TEXT,
              request_origin TEXT,
              session_state TEXT NOT NULL DEFAULT 'active',
              created_at_utc TEXT NOT NULL,
              last_seen_at_utc TEXT NOT NULL,
              revoked_at_utc TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_auth_login_events_created_at ON auth_login_events(created_at_utc);
            CREATE INDEX IF NOT EXISTS idx_auth_login_events_email ON auth_login_events(user_email);
            CREATE INDEX IF NOT EXISTS idx_auth_active_sessions_email ON auth_active_sessions(user_email);
            CREATE INDEX IF NOT EXISTS idx_auth_active_sessions_state ON auth_active_sessions(session_state);
            '''
        )


def log_auth_event(
    db_path: Path,
    *,
    user_email: str | None,
    user_role: str | None,
    identifier_attempted: str | None,
    event_type: str,
    success_flag: bool,
    failure_reason: str | None,
    context: AuthRequestContext,
    details_json: str | None = None,
) -> None:
    ensure_auth_access_db(db_path)
    with _connect(db_path) as conn:
        conn.execute(
            '''
            INSERT INTO auth_login_events(
              user_email, user_role, identifier_attempted, event_type, success_flag,
              failure_reason, ip_public, user_agent, request_origin, created_at_utc, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                user_email,
                user_role,
                identifier_attempted,
                event_type,
                1 if success_flag else 0,
                failure_reason,
                context.ip_public,
                context.user_agent,
                context.request_origin,
                _utc_now(),
                details_json,
            ),
        )


def create_auth_session(
    db_path: Path,
    *,
    user_email: str,
    user_role: str,
    context: AuthRequestContext,
) -> str:
    ensure_auth_access_db(db_path)
    session_id = secrets.token_hex(16)
    token_hash = _sha256_text(session_id)
    now = _utc_now()
    with _connect(db_path) as conn:
        conn.execute(
            '''
            INSERT INTO auth_active_sessions(
              session_id, user_email, user_role, session_token_hash,
              ip_public, user_agent, request_origin,
              session_state, created_at_utc, last_seen_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)
            ''',
            (
                session_id,
                user_email,
                user_role,
                token_hash,
                context.ip_public,
                context.user_agent,
                context.request_origin,
                now,
                now,
            ),
        )
    return session_id


def touch_auth_session(db_path: Path, session_id: str) -> None:
    if not session_id:
        return
    ensure_auth_access_db(db_path)
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE auth_active_sessions SET last_seen_at_utc=? WHERE session_id=? AND session_state='active'",
            (_utc_now(), session_id),
        )


def revoke_auth_session(db_path: Path, session_id: str) -> None:
    if not session_id:
        return
    ensure_auth_access_db(db_path)
    now = _utc_now()
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE auth_active_sessions SET session_state='revoked', revoked_at_utc=?, last_seen_at_utc=? WHERE session_id=? AND session_state='active'",
            (now, now, session_id),
        )


def get_recent_auth_snapshot(db_path: Path, limit: int = 20) -> dict[str, list[dict[str, Any]]]:
    ensure_auth_access_db(db_path)
    with _connect(db_path) as conn:
        events = [dict(r) for r in conn.execute(
            "SELECT user_email, user_role, identifier_attempted, event_type, success_flag, failure_reason, ip_public, user_agent, request_origin, created_at_utc FROM auth_login_events ORDER BY id DESC LIMIT ?",
            (limit,),
        )]
        sessions = [dict(r) for r in conn.execute(
            "SELECT session_id, user_email, user_role, ip_public, user_agent, request_origin, session_state, created_at_utc, last_seen_at_utc, revoked_at_utc FROM auth_active_sessions ORDER BY id DESC LIMIT ?",
            (limit,),
        )]
    return {"events": events, "sessions": sessions}
