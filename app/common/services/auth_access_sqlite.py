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
            CREATE TABLE IF NOT EXISTS auth_accounts (
              user_email TEXT PRIMARY KEY,
              user_role TEXT NOT NULL,
              account_status TEXT NOT NULL DEFAULT 'active',
              account_source TEXT,
              created_at_utc TEXT NOT NULL,
              updated_at_utc TEXT NOT NULL,
              suspended_at_utc TEXT,
              closed_at_utc TEXT
            );

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

            CREATE TABLE IF NOT EXISTS auth_account_lifecycle_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_email TEXT NOT NULL,
              user_role TEXT,
              previous_status TEXT,
              resulting_status TEXT NOT NULL,
              event_type TEXT NOT NULL,
              performed_by_user_email TEXT,
              performed_by_user_role TEXT,
              reason TEXT,
              ip_public TEXT,
              user_agent TEXT,
              request_origin TEXT,
              created_at_utc TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_auth_accounts_status ON auth_accounts(account_status);
            CREATE INDEX IF NOT EXISTS idx_auth_login_events_created_at ON auth_login_events(created_at_utc);
            CREATE INDEX IF NOT EXISTS idx_auth_login_events_email ON auth_login_events(user_email);
            CREATE INDEX IF NOT EXISTS idx_auth_active_sessions_email ON auth_active_sessions(user_email);
            CREATE INDEX IF NOT EXISTS idx_auth_active_sessions_state ON auth_active_sessions(session_state);
            CREATE INDEX IF NOT EXISTS idx_auth_lifecycle_email ON auth_account_lifecycle_events(user_email);
            CREATE INDEX IF NOT EXISTS idx_auth_lifecycle_created_at ON auth_account_lifecycle_events(created_at_utc);
            '''
        )


def upsert_auth_account(
    db_path: Path,
    *,
    user_email: str,
    user_role: str,
    account_source: str,
) -> None:
    ensure_auth_access_db(db_path)
    now = _utc_now()
    with _connect(db_path) as conn:
        conn.execute(
            '''
            INSERT INTO auth_accounts(
              user_email, user_role, account_status, account_source, created_at_utc, updated_at_utc
            ) VALUES (?, ?, 'active', ?, ?, ?)
            ON CONFLICT(user_email) DO UPDATE SET
              user_role=excluded.user_role,
              account_source=excluded.account_source,
              updated_at_utc=excluded.updated_at_utc
            ''',
            (user_email, user_role, account_source, now, now),
        )


def get_account_status(db_path: Path, user_email: str) -> str:
    ensure_auth_access_db(db_path)
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT account_status FROM auth_accounts WHERE lower(user_email)=lower(?)",
            (user_email,),
        ).fetchone()
    return str(row["account_status"]) if row else "active"


def set_account_status(
    db_path: Path,
    *,
    user_email: str,
    resulting_status: str,
    performed_by_user_email: str | None,
    performed_by_user_role: str | None,
    reason: str | None,
    context: AuthRequestContext,
) -> None:
    ensure_auth_access_db(db_path)
    now = _utc_now()
    with _connect(db_path) as conn:
        current = conn.execute(
            "SELECT user_role, account_status FROM auth_accounts WHERE lower(user_email)=lower(?)",
            (user_email,),
        ).fetchone()
        if current is None:
            raise KeyError(f"account_not_found:{user_email}")

        previous_status = str(current["account_status"])
        user_role = str(current["user_role"])
        event_type = f"account_{resulting_status}"
        suspended_at = now if resulting_status == "suspended" else None
        closed_at = now if resulting_status == "closed" else None

        conn.execute(
            '''
            UPDATE auth_accounts
            SET account_status=?,
                updated_at_utc=?,
                suspended_at_utc=CASE WHEN ?='suspended' THEN ? ELSE suspended_at_utc END,
                closed_at_utc=CASE WHEN ?='closed' THEN ? ELSE closed_at_utc END
            WHERE lower(user_email)=lower(?)
            ''',
            (resulting_status, now, resulting_status, suspended_at, resulting_status, closed_at, user_email),
        )
        conn.execute(
            '''
            UPDATE auth_active_sessions
            SET session_state='revoked',
                revoked_at_utc=?,
                last_seen_at_utc=?
            WHERE lower(user_email)=lower(?) AND session_state='active'
            ''',
            (now, now, user_email),
        )
        conn.execute(
            '''
            INSERT INTO auth_account_lifecycle_events(
              user_email, user_role, previous_status, resulting_status, event_type,
              performed_by_user_email, performed_by_user_role, reason,
              ip_public, user_agent, request_origin, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                user_email,
                user_role,
                previous_status,
                resulting_status,
                event_type,
                performed_by_user_email,
                performed_by_user_role,
                reason,
                context.ip_public,
                context.user_agent,
                context.request_origin,
                now,
            ),
        )


def reactivate_account(
    db_path: Path,
    *,
    user_email: str,
    performed_by_user_email: str | None,
    performed_by_user_role: str | None,
    reason: str | None,
    context: AuthRequestContext,
) -> None:
    ensure_auth_access_db(db_path)
    now = _utc_now()
    with _connect(db_path) as conn:
        current = conn.execute(
            "SELECT user_role, account_status FROM auth_accounts WHERE lower(user_email)=lower(?)",
            (user_email,),
        ).fetchone()
        if current is None:
            raise KeyError(f"account_not_found:{user_email}")

        previous_status = str(current["account_status"])
        user_role = str(current["user_role"])
        conn.execute(
            '''
            UPDATE auth_accounts
            SET account_status='active',
                updated_at_utc=?,
                suspended_at_utc=NULL
            WHERE lower(user_email)=lower(?)
            ''',
            (now, user_email),
        )
        conn.execute(
            '''
            INSERT INTO auth_account_lifecycle_events(
              user_email, user_role, previous_status, resulting_status, event_type,
              performed_by_user_email, performed_by_user_role, reason,
              ip_public, user_agent, request_origin, created_at_utc
            ) VALUES (?, ?, ?, 'active', 'account_reactivated', ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                user_email,
                user_role,
                previous_status,
                performed_by_user_email,
                performed_by_user_role,
                reason,
                context.ip_public,
                context.user_agent,
                context.request_origin,
                now,
            ),
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
        accounts = [dict(r) for r in conn.execute(
            "SELECT user_email, user_role, account_status, account_source, created_at_utc, updated_at_utc, suspended_at_utc, closed_at_utc FROM auth_accounts ORDER BY lower(user_email)",
        )]
        events = [dict(r) for r in conn.execute(
            "SELECT user_email, user_role, identifier_attempted, event_type, success_flag, failure_reason, ip_public, user_agent, request_origin, created_at_utc FROM auth_login_events ORDER BY id DESC LIMIT ?",
            (limit,),
        )]
        sessions = [dict(r) for r in conn.execute(
            "SELECT session_id, user_email, user_role, ip_public, user_agent, request_origin, session_state, created_at_utc, last_seen_at_utc, revoked_at_utc FROM auth_active_sessions ORDER BY id DESC LIMIT ?",
            (limit,),
        )]
        lifecycle = [dict(r) for r in conn.execute(
            "SELECT user_email, user_role, previous_status, resulting_status, event_type, performed_by_user_email, performed_by_user_role, reason, created_at_utc FROM auth_account_lifecycle_events ORDER BY id DESC LIMIT ?",
            (limit,),
        )]
    return {"accounts": accounts, "events": events, "sessions": sessions, "lifecycle": lifecycle}
