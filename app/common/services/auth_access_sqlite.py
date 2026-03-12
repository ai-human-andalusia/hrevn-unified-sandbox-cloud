from __future__ import annotations

import base64
import hashlib
import hmac
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


def _random_token() -> str:
    return secrets.token_urlsafe(24)


def _password_material(password: str, salt_b64: str | None = None) -> tuple[str, str]:
    if salt_b64 is None:
        salt = secrets.token_bytes(16)
        salt_b64 = base64.b64encode(salt).decode("ascii")
    else:
        salt = base64.b64decode(salt_b64.encode("ascii"))
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return base64.b64encode(digest).decode("ascii"), salt_b64


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

            CREATE TABLE IF NOT EXISTS auth_local_credentials (
              user_email TEXT PRIMARY KEY,
              password_hash_b64 TEXT NOT NULL,
              password_salt_b64 TEXT NOT NULL,
              email_verified_flag INTEGER NOT NULL DEFAULT 0,
              recovery_email TEXT,
              verification_token TEXT,
              verification_token_expires_at_utc TEXT,
              verified_at_utc TEXT,
              password_reset_token TEXT,
              password_reset_expires_at_utc TEXT,
              created_at_utc TEXT NOT NULL,
              updated_at_utc TEXT NOT NULL
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

            CREATE TABLE IF NOT EXISTS auth_notification_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              related_user_email TEXT,
              target_email TEXT,
              event_type TEXT NOT NULL,
              delivery_channel TEXT NOT NULL,
              delivery_status TEXT NOT NULL,
              subject TEXT,
              error_detail TEXT,
              created_at_utc TEXT NOT NULL,
              details_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_auth_accounts_status ON auth_accounts(account_status);
            CREATE INDEX IF NOT EXISTS idx_auth_local_verified ON auth_local_credentials(email_verified_flag);
            CREATE INDEX IF NOT EXISTS idx_auth_login_events_created_at ON auth_login_events(created_at_utc);
            CREATE INDEX IF NOT EXISTS idx_auth_login_events_email ON auth_login_events(user_email);
            CREATE INDEX IF NOT EXISTS idx_auth_active_sessions_email ON auth_active_sessions(user_email);
            CREATE INDEX IF NOT EXISTS idx_auth_active_sessions_state ON auth_active_sessions(session_state);
            CREATE INDEX IF NOT EXISTS idx_auth_lifecycle_email ON auth_account_lifecycle_events(user_email);
            CREATE INDEX IF NOT EXISTS idx_auth_lifecycle_created_at ON auth_account_lifecycle_events(created_at_utc);
            CREATE INDEX IF NOT EXISTS idx_auth_notifications_created_at ON auth_notification_events(created_at_utc);
            '''
        )


def upsert_auth_account(
    db_path: Path,
    *,
    user_email: str,
    user_role: str,
    account_source: str,
    initial_status: str = "active",
) -> None:
    ensure_auth_access_db(db_path)
    now = _utc_now()
    with _connect(db_path) as conn:
        existing = conn.execute(
            "SELECT user_email FROM auth_accounts WHERE lower(user_email)=lower(?)",
            (user_email,),
        ).fetchone()
        conn.execute(
            '''
            INSERT INTO auth_accounts(
              user_email, user_role, account_status, account_source, created_at_utc, updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_email) DO UPDATE SET
              user_role=excluded.user_role,
              account_source=excluded.account_source,
              updated_at_utc=excluded.updated_at_utc
            ''',
            (user_email, user_role, initial_status, account_source, now, now),
        )
        if existing is None:
            conn.execute(
                '''
                INSERT INTO auth_account_lifecycle_events(
                  user_email, user_role, previous_status, resulting_status, event_type,
                  performed_by_user_email, performed_by_user_role, reason,
                  ip_public, user_agent, request_origin, created_at_utc
                ) VALUES (?, ?, NULL, ?, 'account_registered', NULL, NULL, ?, 'system', 'system', ?, ?)
                ''',
                (user_email, user_role, initial_status, f"registered_from:{account_source}", account_source, now),
            )


def create_local_account(
    db_path: Path,
    *,
    user_email: str,
    password: str,
    recovery_email: str | None,
    context: AuthRequestContext,
) -> str:
    ensure_auth_access_db(db_path)
    user_email = user_email.strip().lower()
    upsert_auth_account(
        db_path,
        user_email=user_email,
        user_role="operator",
        account_source="self_signup",
        initial_status="pending_verification",
    )
    password_hash_b64, password_salt_b64 = _password_material(password)
    now = _utc_now()
    verification_token = _random_token()
    verification_expires_at = now
    # keep the simple timestamp format and a clear short-lived demo token horizon note in UI
    with _connect(db_path) as conn:
        existing = conn.execute(
            "SELECT user_email FROM auth_local_credentials WHERE lower(user_email)=lower(?)",
            (user_email,),
        ).fetchone()
        if existing is not None:
            raise ValueError("account_already_exists")
        conn.execute(
            '''
            INSERT INTO auth_local_credentials(
              user_email, password_hash_b64, password_salt_b64, email_verified_flag,
              recovery_email, verification_token, verification_token_expires_at_utc,
              verified_at_utc, password_reset_token, password_reset_expires_at_utc,
              created_at_utc, updated_at_utc
            ) VALUES (?, ?, ?, 0, ?, ?, ?, NULL, NULL, NULL, ?, ?)
            ''',
            (
                user_email,
                password_hash_b64,
                password_salt_b64,
                recovery_email.strip().lower() if recovery_email else None,
                verification_token,
                verification_expires_at,
                now,
                now,
            ),
        )
        conn.execute(
            '''
            INSERT INTO auth_account_lifecycle_events(
              user_email, user_role, previous_status, resulting_status, event_type,
              performed_by_user_email, performed_by_user_role, reason,
              ip_public, user_agent, request_origin, created_at_utc
            ) VALUES (?, 'operator', 'pending_verification', 'pending_verification', 'verification_requested',
                      NULL, NULL, 'self_signup', ?, ?, ?, ?)
            ''',
            (user_email, context.ip_public, context.user_agent, context.request_origin, now),
        )
    return verification_token


def authenticate_local_account(db_path: Path, *, user_email: str, password: str) -> dict[str, Any] | None:
    ensure_auth_access_db(db_path)
    user_email = user_email.strip().lower()
    with _connect(db_path) as conn:
        row = conn.execute(
            '''
            SELECT a.user_email, a.user_role, a.account_status, c.password_hash_b64, c.password_salt_b64, c.email_verified_flag
            FROM auth_accounts a
            JOIN auth_local_credentials c ON lower(a.user_email)=lower(c.user_email)
            WHERE lower(a.user_email)=lower(?)
            ''',
            (user_email,),
        ).fetchone()
    if row is None:
        return None
    expected_hash, _ = _password_material(password, str(row["password_salt_b64"]))
    if not hmac.compare_digest(expected_hash, str(row["password_hash_b64"])):
        return None
    return dict(row)


def verify_email_token(db_path: Path, *, user_email: str, token: str, context: AuthRequestContext) -> bool:
    ensure_auth_access_db(db_path)
    user_email = user_email.strip().lower()
    token = token.strip()
    now = _utc_now()
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT verification_token, email_verified_flag FROM auth_local_credentials WHERE lower(user_email)=lower(?)",
            (user_email,),
        ).fetchone()
        if row is None or int(row["email_verified_flag"] or 0) == 1:
            return False
        if str(row["verification_token"] or "") != token:
            return False
        conn.execute(
            '''
            UPDATE auth_local_credentials
            SET email_verified_flag=1,
                verification_token=NULL,
                verification_token_expires_at_utc=NULL,
                verified_at_utc=?,
                updated_at_utc=?
            WHERE lower(user_email)=lower(?)
            ''',
            (now, now, user_email),
        )
        conn.execute(
            '''
            UPDATE auth_accounts
            SET account_status='active', updated_at_utc=?
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
            ) VALUES (?, 'operator', 'pending_verification', 'active', 'email_verified',
                      ?, 'self_service', NULL, ?, ?, ?, ?)
            ''',
            (user_email, user_email, context.ip_public, context.user_agent, context.request_origin, now),
        )
    return True


def issue_password_reset_token(db_path: Path, *, user_email: str, context: AuthRequestContext) -> str | None:
    ensure_auth_access_db(db_path)
    user_email = user_email.strip().lower()
    now = _utc_now()
    token = _random_token()
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT user_email FROM auth_local_credentials WHERE lower(user_email)=lower(?)",
            (user_email,),
        ).fetchone()
        if row is None:
            return None
        conn.execute(
            '''
            UPDATE auth_local_credentials
            SET password_reset_token=?, password_reset_expires_at_utc=?, updated_at_utc=?
            WHERE lower(user_email)=lower(?)
            ''',
            (token, now, now, user_email),
        )
        conn.execute(
            '''
            INSERT INTO auth_account_lifecycle_events(
              user_email, user_role, previous_status, resulting_status, event_type,
              performed_by_user_email, performed_by_user_role, reason,
              ip_public, user_agent, request_origin, created_at_utc
            ) VALUES (?, 'operator', NULL, NULL, 'password_reset_token_issued',
                      NULL, NULL, NULL, ?, ?, ?, ?)
            ''',
            (user_email, context.ip_public, context.user_agent, context.request_origin, now),
        )
    return token


def reset_local_password(db_path: Path, *, user_email: str, token: str, new_password: str, context: AuthRequestContext) -> bool:
    ensure_auth_access_db(db_path)
    user_email = user_email.strip().lower()
    token = token.strip()
    now = _utc_now()
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT password_reset_token FROM auth_local_credentials WHERE lower(user_email)=lower(?)",
            (user_email,),
        ).fetchone()
        if row is None or str(row["password_reset_token"] or "") != token:
            return False
        password_hash_b64, password_salt_b64 = _password_material(new_password)
        conn.execute(
            '''
            UPDATE auth_local_credentials
            SET password_hash_b64=?, password_salt_b64=?, password_reset_token=NULL,
                password_reset_expires_at_utc=NULL, updated_at_utc=?
            WHERE lower(user_email)=lower(?)
            ''',
            (password_hash_b64, password_salt_b64, now, user_email),
        )
        conn.execute(
            '''
            INSERT INTO auth_account_lifecycle_events(
              user_email, user_role, previous_status, resulting_status, event_type,
              performed_by_user_email, performed_by_user_role, reason,
              ip_public, user_agent, request_origin, created_at_utc
            ) VALUES (?, 'operator', NULL, NULL, 'password_reset_completed',
                      ?, 'self_service', NULL, ?, ?, ?, ?)
            ''',
            (user_email, user_email, context.ip_public, context.user_agent, context.request_origin, now),
        )
    return True


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


def log_auth_notification_event(
    db_path: Path,
    *,
    related_user_email: str | None,
    target_email: str | None,
    event_type: str,
    delivery_channel: str,
    delivery_status: str,
    subject: str | None,
    error_detail: str | None = None,
    details_json: str | None = None,
) -> None:
    ensure_auth_access_db(db_path)
    with _connect(db_path) as conn:
        conn.execute(
            '''
            INSERT INTO auth_notification_events(
              related_user_email, target_email, event_type, delivery_channel, delivery_status,
              subject, error_detail, created_at_utc, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                related_user_email,
                target_email,
                event_type,
                delivery_channel,
                delivery_status,
                subject,
                error_detail,
                _utc_now(),
                details_json,
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
        notifications = [dict(r) for r in conn.execute(
            "SELECT related_user_email, target_email, event_type, delivery_channel, delivery_status, subject, error_detail, created_at_utc FROM auth_notification_events ORDER BY id DESC LIMIT ?",
            (limit,),
        )]
    return {"accounts": accounts, "events": events, "sessions": sessions, "lifecycle": lifecycle, "notifications": notifications}
