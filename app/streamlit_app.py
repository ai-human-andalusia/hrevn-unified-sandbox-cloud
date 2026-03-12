"""Streamlit app for documentary-only sandbox exploration.

No real access, no external source systems, documentary-safe views only.
"""

from __future__ import annotations

import json
import pandas as pd
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import streamlit as st
from common.config import load_common_config
from common.security import evaluate_secret_posture, redact_config_for_ui
from common.services.ai_router import choose_ai_provider
from common.services.gmail_connector import get_mail_connector_status
from common.services.github_connector import get_github_connector_status
from common.services.agent_operations_package import AERSigningConfig, build_agent_operation_aer_package
from common.services.auth_access_sqlite import (
    AuthRequestContext,
    authenticate_local_account,
    clear_failed_login_state,
    clear_ip_failed_state,
    count_active_sessions,
    create_local_account,
    create_auth_session,
    get_account_record,
    get_account_status,
    get_ip_control_record,
    get_recent_auth_snapshot,
    ip_is_blocked,
    ip_is_in_cooldown,
    is_account_temporarily_locked,
    issue_password_reset_token,
    log_auth_event,
    log_auth_notification_event,
    register_failed_login,
    register_failed_login_with_window,
    register_failed_ip_attempt,
    reactivate_account,
    revoke_all_active_sessions_for_user,
    revoke_auth_session,
    reset_local_password,
    set_account_status,
    touch_auth_session,
    unblock_ip,
    upsert_auth_account,
    verify_email_token,
)
from common.services.auth_notifications import send_smtp_notification
from common.services.agent_operations_sqlite import (
    load_agent_operations_snapshot,
    set_agent_operation_decision,
)
from common.services.real_estate_sqlite import (
    build_real_estate_workspace,
    load_real_estate_snapshot,
)
from common.services.telegram_connector import (
    get_telegram_connector_status,
    send_controlled_test_message,
)
from common.tools.secret_hygiene_scan import run_secret_hygiene_scan


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "schema"
MAPPINGS_DIR = ROOT / "mappings"
DOCS_DIR = ROOT / "docs"
SAMPLES_DIR = ROOT / "samples"
IMPORTERS_DIR = ROOT / "importers"
TESTS_DIR = ROOT / "tests"
APP_DATA_DIR = ROOT / "app" / "data"
REAL_ESTATE_SQLITE_PATH = APP_DATA_DIR / "real_estate" / "hrevn_real_estate.db"
AGENT_OPERATIONS_SQLITE_PATH = APP_DATA_DIR / "agent_operations" / "hrevn_agent_operations.db"
AUTH_ACCESS_SQLITE_PATH = APP_DATA_DIR / "auth" / "hrevn_auth_access.db"


@dataclass
class ValidationResult:
    file_name: str
    ok: bool
    checks: Dict[str, bool]
    notes: List[str]


@dataclass(frozen=True)
class AuthShellConfig:
    auth_enabled: bool
    has_configured_accounts: bool
    admin_email: str
    admin_password: str
    user_email: str
    user_password: str
    recovery_notify_email: str


@dataclass(frozen=True)
class RealEstateReadiness:
    observation_count: int
    photo_count: int
    required_photo_count: int
    all_observations_have_lpi: bool
    min_photos_ok: bool
    naming_policy_ready: bool
    ai_gate_ready: bool
    already_issued: bool
    lpi_dictionary_size: int
    issuance_ready: bool


def _render_global_table_style() -> None:
    st.markdown(
        """
        <style>
        div[data-testid="stDataFrame"] [role="columnheader"] {
            text-transform: uppercase !important;
            letter-spacing: 0.06em;
            font-size: 0.72rem !important;
            font-weight: 700 !important;
            font-family: 'SFMono-Regular', Menlo, Consolas, monospace !important;
        }
        div[data-testid="stDataFrame"] [role="gridcell"] {
            font-size: 0.8rem !important;
            font-family: 'SFMono-Regular', Menlo, Consolas, monospace !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _safe_read(path: Path, max_chars: int = 12000) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
        return text[:max_chars]
    except Exception as exc:
        return f"[read_error] {exc}"


def _list_files(directory: Path, patterns: Tuple[str, ...]) -> List[Path]:
    files: List[Path] = []
    for pattern in patterns:
        files.extend(directory.glob(pattern))
    return sorted([f for f in files if f.is_file()])


def _load_yaml_if_available(raw: str):
    try:
        import yaml  # type: ignore

        return yaml.safe_load(raw), None
    except Exception as exc:
        return None, str(exc)


def _validate_mapping_file(path: Path) -> ValidationResult:
    raw = _safe_read(path, max_chars=200000)
    parsed, yaml_err = _load_yaml_if_available(raw)

    checks = {
        "has_source_table_text": "source_table" in raw,
        "has_mappings_text": "mappings" in raw,
        "has_related_ids_text": "related_" in raw,
        "yaml_parseable": yaml_err is None,
    }
    notes: List[str] = []

    if yaml_err is not None:
        notes.append("PyYAML unavailable or YAML parse error. Text checks still applied.")
        notes.append(f"parser_detail: {yaml_err}")
    else:
        if not isinstance(parsed, dict):
            notes.append("Top-level YAML should be a dictionary-like object.")
        else:
            if "source" not in parsed and "source_table" not in parsed:
                notes.append("Missing top-level `source`/`source_table` key.")
            if "mappings" not in parsed:
                notes.append("Missing top-level `mappings` key.")

    ok = all(checks.values()) if yaml_err is None else (
        checks["has_source_table_text"] and checks["has_mappings_text"]
    )
    return ValidationResult(path.name, ok, checks, notes)


def _directory_snapshot() -> List[Tuple[str, Path]]:
    return [
        ("docs", DOCS_DIR),
        ("samples", SAMPLES_DIR),
        ("schema", SCHEMA_DIR),
        ("mappings", MAPPINGS_DIR),
        ("importers", IMPORTERS_DIR),
        ("tests", TESTS_DIR),
    ]


def _stats_for(path: Path) -> Dict[str, int]:
    if not path.exists() or not path.is_dir():
        return {"files": 0, "md": 0, "yaml_yml": 0, "json": 0, "py": 0}
    files = [p for p in path.rglob("*") if p.is_file()]
    return {
        "files": len(files),
        "md": len([f for f in files if f.suffix.lower() == ".md"]),
        "yaml_yml": len([f for f in files if f.suffix.lower() in {".yaml", ".yml"}]),
        "json": len([f for f in files if f.suffix.lower() == ".json"]),
        "py": len([f for f in files if f.suffix.lower() == ".py"]),
    }


def _secret_value(key: str, default: str = "") -> str:
    try:
        value = st.secrets.get(key, default)
        return str(value or default)
    except Exception:
        return default


def _secret_bool(key: str, default: bool = False) -> bool:
    raw = _secret_value(key, "1" if default else "0")
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _secret_int(key: str, default: int) -> int:
    raw = _secret_value(key, str(default))
    try:
        return int(raw)
    except Exception:
        return default


def _load_auth_shell_config() -> AuthShellConfig:
    admin_email = _secret_value("SANDBOX_ADMIN_EMAIL", "")
    admin_password = _secret_value("SANDBOX_ADMIN_PASSWORD", "")
    user_email = _secret_value("SANDBOX_USER_EMAIL", "")
    user_password = _secret_value("SANDBOX_USER_PASSWORD", "")
    configured = bool((admin_email and admin_password) or (user_email and user_password))
    auth_enabled = _secret_bool("SANDBOX_AUTH_ENABLED", configured)
    return AuthShellConfig(
        auth_enabled=auth_enabled,
        has_configured_accounts=configured,
        admin_email=admin_email,
        admin_password=admin_password,
        user_email=user_email,
        user_password=user_password,
        recovery_notify_email=_secret_value("SANDBOX_RECOVERY_NOTIFY_EMAIL", _secret_value("MAIL_FROM", "")),
    )


def _sync_auth_accounts(cfg: AuthShellConfig) -> None:
    if cfg.admin_email:
        upsert_auth_account(
            AUTH_ACCESS_SQLITE_PATH,
            user_email=cfg.admin_email.strip().lower(),
            user_role="admin",
            account_source="streamlit_secrets",
        )
    if cfg.user_email:
        upsert_auth_account(
            AUTH_ACCESS_SQLITE_PATH,
            user_email=cfg.user_email.strip().lower(),
            user_role="operator",
            account_source="streamlit_secrets",
        )
    upsert_auth_account(
        AUTH_ACCESS_SQLITE_PATH,
        user_email="demo@hrevn.local",
        user_role="demo",
        account_source="built_in_demo",
    )


def _send_access_notification(
    *,
    related_user_email: str | None,
    target_email: str | None,
    event_type: str,
    subject: str,
    body: str,
) -> None:
    if not target_email:
        log_auth_notification_event(
            AUTH_ACCESS_SQLITE_PATH,
            related_user_email=related_user_email,
            target_email=target_email,
            event_type=event_type,
            delivery_channel="none",
            delivery_status="not_configured",
            subject=subject,
            error_detail=None,
            details_json=json.dumps({"reason": "missing_target_email"}),
        )
        return

    smtp_enabled = _secret_bool("SMTP_ENABLED", False)
    smtp_host = _secret_value("SMTP_HOST", "")
    smtp_port = _secret_int("SMTP_PORT", 587)
    smtp_user = _secret_value("SMTP_USER", "")
    smtp_pass = _secret_value("SMTP_PASS", "")
    mail_from = _secret_value("MAIL_FROM", "") or _secret_value("SANDBOX_RECOVERY_NOTIFY_EMAIL", "")

    if not (smtp_enabled and smtp_host and smtp_user and smtp_pass and mail_from):
        log_auth_notification_event(
            AUTH_ACCESS_SQLITE_PATH,
            related_user_email=related_user_email,
            target_email=target_email,
            event_type=event_type,
            delivery_channel="none",
            delivery_status="not_configured",
            subject=subject,
            error_detail=None,
            details_json=json.dumps({"reason": "smtp_not_configured"}),
        )
        return

    result = send_smtp_notification(
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_pass=smtp_pass,
        mail_from=mail_from,
        target_email=target_email,
        subject=subject,
        body=body,
    )
    log_auth_notification_event(
        AUTH_ACCESS_SQLITE_PATH,
        related_user_email=related_user_email,
        target_email=result.target_email,
        event_type=event_type,
        delivery_channel=result.delivery_channel,
        delivery_status=result.delivery_status,
        subject=result.subject,
        error_detail=result.error_detail,
        details_json=None,
    )


def _load_aer_signing_config() -> AERSigningConfig:
    enabled = _secret_bool("HREVN_SIGNING_ENABLED", False)
    return AERSigningConfig(
        enabled=enabled,
        issuer=_secret_value("HREVN_SIGNING_ISSUER", "H-REVN"),
        key_id=_secret_value("HREVN_SIGNING_KEY_ID", "hrevn-primary-signing-key-001"),
        private_key=_secret_value("HREVN_SIGNING_PRIVATE_KEY", ""),
        verification_url=_secret_value("HREVN_SIGNING_VERIFICATION_URL", "https://hrevn.com/.well-known/hrevn-signing-key.json"),
        signature_profile=_secret_value("HREVN_SIGNING_PROFILE", "hrevn_signing_v1"),
        algorithm="ed25519",
    )


def _get_auth_request_context() -> AuthRequestContext:
    headers = {}
    try:
        headers = dict(getattr(st.context, "headers", {}) or {})
    except Exception:
        headers = {}

    forwarded_for = str(headers.get("X-Forwarded-For") or headers.get("x-forwarded-for") or "").strip()
    ip_public = forwarded_for.split(",")[0].strip() if forwarded_for else "unknown"
    user_agent = str(headers.get("User-Agent") or headers.get("user-agent") or "unknown").strip()[:240]
    origin = str(headers.get("Origin") or headers.get("origin") or headers.get("Host") or headers.get("host") or "streamlit_cloud").strip()[:240]
    return AuthRequestContext(
        ip_public=ip_public or "unknown",
        user_agent=user_agent or "unknown",
        request_origin=origin or "streamlit_cloud",
    )


def _init_auth_state() -> None:
    st.session_state.setdefault("auth_logged_in", False)
    st.session_state.setdefault("auth_role", "guest")
    st.session_state.setdefault("auth_email", "")
    st.session_state.setdefault("auth_session_id", "")
    st.session_state.setdefault("recovery_requests", [])


def _password_policy_ok(password: str) -> tuple[bool, str]:
    if len(password) < 10:
        return False, "Password must have at least 10 characters."
    if password.lower() == password or password.upper() == password:
        return False, "Password should mix upper and lower case."
    if not any(ch.isdigit() for ch in password):
        return False, "Password should include at least one number."
    return True, ""


def _max_active_sessions() -> int:
    return _secret_int("SANDBOX_MAX_ACTIVE_SESSIONS_PER_USER", 3)


def _ip_cooldown_threshold() -> int:
    return _secret_int("SANDBOX_IP_COOLDOWN_THRESHOLD", 5)


def _ip_block_threshold() -> int:
    return _secret_int("SANDBOX_IP_BLOCK_THRESHOLD", 8)


def _user_lockout_seconds() -> int:
    return _secret_int("SANDBOX_USER_LOCKOUT_SECONDS", 900)


def _ip_cooldown_seconds() -> int:
    return _secret_int("SANDBOX_IP_COOLDOWN_SECONDS", 600)


def _ip_block_seconds() -> int:
    return _secret_int("SANDBOX_IP_BLOCK_SECONDS", 1800)


def _send_telegram_security_alert(event_type: str, message: str) -> None:
    cfg = load_common_config()
    telegram_status = get_telegram_connector_status(cfg)
    if not telegram_status.ready:
        log_auth_notification_event(
            AUTH_ACCESS_SQLITE_PATH,
            related_user_email=None,
            target_email="telegram_admin_channel",
            event_type=event_type,
            delivery_channel="telegram",
            delivery_status="not_configured",
            subject=event_type,
            error_detail=None,
            details_json=json.dumps({"message": message}),
        )
        return
    ok, detail = send_controlled_test_message(message)
    log_auth_notification_event(
        AUTH_ACCESS_SQLITE_PATH,
        related_user_email=None,
        target_email="telegram_admin_channel",
        event_type=event_type,
        delivery_channel="telegram",
        delivery_status="sent" if ok else "failed",
        subject=event_type,
        error_detail=None if ok else detail,
        details_json=json.dumps({"message": message}),
    )


def _logout() -> None:
    session_id = st.session_state.get("auth_session_id") or ""
    auth_email = st.session_state.get("auth_email") or None
    auth_role = st.session_state.get("auth_role") or None
    if session_id:
        context = _get_auth_request_context()
        revoke_auth_session(AUTH_ACCESS_SQLITE_PATH, session_id)
        log_auth_event(
            AUTH_ACCESS_SQLITE_PATH,
            user_email=auth_email,
            user_role=auth_role,
            identifier_attempted=auth_email,
            event_type="logout",
            success_flag=True,
            failure_reason=None,
            context=context,
        )
    st.session_state["auth_logged_in"] = False
    st.session_state["auth_role"] = "guest"
    st.session_state["auth_email"] = ""
    st.session_state["auth_session_id"] = ""


def _render_auth_shell() -> None:
    cfg = _load_auth_shell_config()
    _sync_auth_accounts(cfg)
    _init_auth_state()

    if st.session_state.get("auth_logged_in"):
        current_email = st.session_state.get("auth_email") or ""
        if current_email and get_account_status(AUTH_ACCESS_SQLITE_PATH, current_email) != "active":
            _logout()
            st.warning("This account is no longer active. The current session has been closed.")
            st.stop()
        active_session_id = st.session_state.get("auth_session_id") or ""
        if active_session_id:
            touch_auth_session(AUTH_ACCESS_SQLITE_PATH, active_session_id)
        if st.session_state.get("auth_role") == "admin":
            st.sidebar.markdown("### Admin space")
            if st.sidebar.button("Central Console", use_container_width=True):
                st.session_state["main_tab_target"] = "central_console"
                st.rerun()
            if st.sidebar.button("Access & Security", use_container_width=True):
                st.session_state["main_tab_target"] = "access_security"
                st.rerun()
            st.sidebar.markdown("#### Verticals")
            st.sidebar.button("Real Estate", disabled=True, use_container_width=True)
            st.sidebar.button("GOV / Photovoltaic", disabled=True, use_container_width=True)
            st.sidebar.button("Graphic Evidence", disabled=True, use_container_width=True)
            st.sidebar.button("GENIUS Operations", disabled=True, use_container_width=True)
            if st.sidebar.button("Agent Operations", use_container_width=True):
                st.session_state["main_tab_target"] = "agent_operations"
                st.rerun()
            st.sidebar.markdown("#### Communications")
            st.sidebar.button("Email", disabled=True, use_container_width=True)
            st.sidebar.button("Telegram", disabled=True, use_container_width=True)
        return

    st.title("HREVN Unified V1 — Access Shell")
    st.caption("Documentary-safe access shell for the unified pilot. No real source access is enabled here.")

    login_tab, signup_tab, verify_tab, recovery_tab = st.tabs(["Login", "Register", "Verify Email", "Password Recovery"])

    with login_tab:
        left, right = st.columns([1.1, 0.9])
        with left:
            email = st.text_input("Email", key="login_email")
            password = st.text_input("Password", type="password", key="login_password")
            if st.button("Access workspace", type="primary"):
                context = _get_auth_request_context()
                ip_record = get_ip_control_record(AUTH_ACCESS_SQLITE_PATH, context.ip_public)
                if ip_is_blocked(ip_record):
                    log_auth_event(
                        AUTH_ACCESS_SQLITE_PATH,
                        user_email=None,
                        user_role=None,
                        identifier_attempted=email.strip().lower() or None,
                        event_type="login_failure",
                        success_flag=False,
                        failure_reason="ip_blocked",
                        context=context,
                    )
                    st.error("This IP is temporarily blocked.")
                    st.stop()
                if ip_is_in_cooldown(ip_record):
                    log_auth_event(
                        AUTH_ACCESS_SQLITE_PATH,
                        user_email=None,
                        user_role=None,
                        identifier_attempted=email.strip().lower() or None,
                        event_type="login_failure",
                        success_flag=False,
                        failure_reason="ip_cooldown",
                        context=context,
                    )
                    st.error("This IP is in cooldown due to repeated failed attempts.")
                    st.stop()
                matched = None
                if cfg.admin_email and cfg.admin_password and email.strip().lower() == cfg.admin_email.strip().lower() and password == cfg.admin_password:
                    matched = ("admin", cfg.admin_email)
                elif cfg.user_email and cfg.user_password and email.strip().lower() == cfg.user_email.strip().lower() and password == cfg.user_password:
                    matched = ("operator", cfg.user_email)
                else:
                    local_match = authenticate_local_account(
                        AUTH_ACCESS_SQLITE_PATH,
                        user_email=email.strip().lower(),
                        password=password,
                    )
                    if local_match:
                        matched = (str(local_match.get("user_role") or "operator"), str(local_match.get("user_email") or email.strip().lower()))

                if matched:
                    account_record = get_account_record(AUTH_ACCESS_SQLITE_PATH, matched[1])
                    account_status = str((account_record or {}).get("account_status") or get_account_status(AUTH_ACCESS_SQLITE_PATH, matched[1]))
                    if is_account_temporarily_locked(account_record):
                        log_auth_event(
                            AUTH_ACCESS_SQLITE_PATH,
                            user_email=matched[1],
                            user_role=matched[0],
                            identifier_attempted=email.strip().lower() or None,
                            event_type="login_failure",
                            success_flag=False,
                            failure_reason="temporary_lockout",
                            context=context,
                        )
                        st.error("This account is temporarily locked after repeated failed login attempts.")
                    elif account_status != "active":
                        log_auth_event(
                            AUTH_ACCESS_SQLITE_PATH,
                            user_email=matched[1],
                            user_role=matched[0],
                            identifier_attempted=email.strip().lower() or None,
                            event_type="login_failure",
                            success_flag=False,
                            failure_reason=f"account_{account_status}",
                            context=context,
                        )
                        st.error(f"Account is {account_status}. Access is blocked.")
                    else:
                        current_active_sessions = count_active_sessions(AUTH_ACCESS_SQLITE_PATH, user_email=matched[1])
                        max_sessions = _max_active_sessions()
                        if current_active_sessions >= max_sessions:
                            log_auth_event(
                                AUTH_ACCESS_SQLITE_PATH,
                                user_email=matched[1],
                                user_role=matched[0],
                                identifier_attempted=email.strip().lower() or None,
                                event_type="login_failure",
                                success_flag=False,
                                failure_reason="session_limit_reached",
                                context=context,
                                details_json=json.dumps({"max_active_sessions": max_sessions}),
                            )
                            st.error(f"Session limit reached for this account ({max_sessions}).")
                            st.stop()
                        clear_failed_login_state(AUTH_ACCESS_SQLITE_PATH, user_email=matched[1])
                        clear_ip_failed_state(AUTH_ACCESS_SQLITE_PATH, ip_public=context.ip_public)
                        session_id = create_auth_session(
                            AUTH_ACCESS_SQLITE_PATH,
                            user_email=matched[1],
                            user_role=matched[0],
                            context=context,
                        )
                        log_auth_event(
                            AUTH_ACCESS_SQLITE_PATH,
                            user_email=matched[1],
                            user_role=matched[0],
                            identifier_attempted=email.strip().lower() or None,
                            event_type="login_success",
                            success_flag=True,
                            failure_reason=None,
                            context=context,
                        )
                        st.session_state["auth_logged_in"] = True
                        st.session_state["auth_role"] = matched[0]
                        st.session_state["auth_email"] = matched[1]
                        st.session_state["auth_session_id"] = session_id
                        st.success("Access granted.")
                        st.rerun()
                elif cfg.auth_enabled and cfg.has_configured_accounts:
                    local_account = get_account_record(AUTH_ACCESS_SQLITE_PATH, email.strip().lower())
                    ip_result = register_failed_ip_attempt(
                        AUTH_ACCESS_SQLITE_PATH,
                        ip_public=context.ip_public,
                        cooldown_threshold=_ip_cooldown_threshold(),
                        block_threshold=_ip_block_threshold(),
                        cooldown_seconds=_ip_cooldown_seconds(),
                        block_seconds=_ip_block_seconds(),
                    )
                    if local_account:
                        updated = register_failed_login_with_window(
                            AUTH_ACCESS_SQLITE_PATH,
                            user_email=email.strip().lower(),
                            lockout_threshold=5,
                            lockout_seconds=_user_lockout_seconds(),
                        )
                        failure_reason = "invalid_credentials_or_password"
                        if is_account_temporarily_locked(updated):
                            failure_reason = "temporary_lockout"
                            _send_telegram_security_alert(
                                "user_lockout",
                                f"H-REVN security alert: user lockout triggered for {email.strip().lower()} from IP {context.ip_public}.",
                            )
                    else:
                        failure_reason = "invalid_credentials_or_password"
                    if ip_is_blocked(ip_result):
                        failure_reason = "ip_blocked"
                        _send_telegram_security_alert(
                            "ip_blocked",
                            f"H-REVN security alert: IP blocked after repeated failed attempts. IP={context.ip_public}.",
                        )
                    elif ip_is_in_cooldown(ip_result):
                        failure_reason = "ip_cooldown"
                        _send_telegram_security_alert(
                            "ip_cooldown",
                            f"H-REVN security alert: IP cooldown triggered. IP={context.ip_public}.",
                        )
                    log_auth_event(
                        AUTH_ACCESS_SQLITE_PATH,
                        user_email=None,
                        user_role=None,
                        identifier_attempted=email.strip().lower() or None,
                        event_type="login_failure",
                        success_flag=False,
                        failure_reason=failure_reason,
                        context=context,
                    )
                    if failure_reason == "ip_blocked":
                        st.error("Too many failed attempts from this IP. It is now blocked.")
                    elif failure_reason == "ip_cooldown":
                        st.error("This IP has entered cooldown after repeated failed attempts.")
                    elif local_account and failure_reason == "temporary_lockout":
                        st.error("Too many failed login attempts. This account is temporarily locked.")
                    else:
                        st.error("Invalid credentials.")
                else:
                    st.warning("Auth shell is not fully configured yet. Use documentary demo access below.")

            if (not cfg.auth_enabled) or (not cfg.has_configured_accounts):
                if st.button("Continue in documentary demo mode"):
                    context = _get_auth_request_context()
                    if get_account_status(AUTH_ACCESS_SQLITE_PATH, "demo@hrevn.local") != "active":
                        st.error("Demo access is currently blocked.")
                    else:
                        session_id = create_auth_session(
                            AUTH_ACCESS_SQLITE_PATH,
                            user_email="demo@hrevn.local",
                            user_role="demo",
                            context=context,
                        )
                        log_auth_event(
                            AUTH_ACCESS_SQLITE_PATH,
                            user_email="demo@hrevn.local",
                            user_role="demo",
                            identifier_attempted="demo@hrevn.local",
                            event_type="login_success_demo",
                            success_flag=True,
                            failure_reason=None,
                            context=context,
                        )
                        st.session_state["auth_logged_in"] = True
                        st.session_state["auth_role"] = "demo"
                        st.session_state["auth_email"] = "demo@hrevn.local"
                        st.session_state["auth_session_id"] = session_id
                        st.rerun()
        with right:
            st.markdown("#### Access status")
            status_rows = [
                {"Item": "Auth shell", "State": "enabled" if cfg.auth_enabled else "demo mode"},
                {"Item": "Configured accounts", "State": "available" if cfg.has_configured_accounts else "not configured yet"},
                {"Item": "Fallback access", "State": "documentary demo mode"},
                {"Item": "Workspace scope", "State": "sandbox only"},
            ]
            st.dataframe(status_rows, use_container_width=True, hide_index=True)
            if cfg.auth_enabled and st.session_state.get("auth_role") == "admin":
                snapshot = get_recent_auth_snapshot(AUTH_ACCESS_SQLITE_PATH, limit=8)
                if snapshot["events"]:
                    st.markdown("#### Recent access events")
                    event_rows = [
                        {
                            "Event": row["event_type"],
                            "Identifier": row["identifier_attempted"] or row["user_email"] or "-",
                            "IP": row["ip_public"] or "-",
                            "At": row["created_at_utc"],
                        }
                        for row in snapshot["events"]
                    ]
                    st.dataframe(event_rows, use_container_width=True, hide_index=True)
            st.info(
                "This layer gives us the structure for login and recovery now. Real credential hardening can be attached later through Streamlit secrets."
            )

    with signup_tab:
        signup_left, signup_right = st.columns([1.1, 0.9])
        with signup_left:
            register_email = st.text_input("Email", key="register_email")
            register_recovery = st.text_input("Recovery email (optional)", key="register_recovery_email")
            register_password = st.text_input("Password", type="password", key="register_password")
            register_password_2 = st.text_input("Confirm password", type="password", key="register_password_confirm")
            if st.button("Create account", type="primary"):
                context = _get_auth_request_context()
                email_value = register_email.strip().lower()
                recovery_value = register_recovery.strip().lower()
                password_ok, password_message = _password_policy_ok(register_password)
                if not email_value or "@" not in email_value:
                    st.error("A valid email is required.")
                elif register_password != register_password_2:
                    st.error("Passwords do not match.")
                elif not password_ok:
                    st.error(password_message)
                else:
                    try:
                        verification_token = create_local_account(
                            AUTH_ACCESS_SQLITE_PATH,
                            user_email=email_value,
                            password=register_password,
                            recovery_email=recovery_value or None,
                            context=context,
                        )
                        log_auth_event(
                            AUTH_ACCESS_SQLITE_PATH,
                            user_email=email_value,
                            user_role="operator",
                            identifier_attempted=email_value,
                            event_type="signup_success",
                            success_flag=True,
                            failure_reason=None,
                            context=context,
                        )
                        _send_access_notification(
                            related_user_email=email_value,
                            target_email=email_value,
                            event_type="verification_email_sent",
                            subject="Verify your H-REVN access account",
                            body=(
                                "Your H-REVN unified workspace account has been created.\n\n"
                                f"Verification token: {verification_token}\n\n"
                                "Use the Verify Email tab to activate the account."
                            ),
                        )
                        st.success("Account created. Verification token issued.")
                        st.code(verification_token, language=None)
                    except ValueError:
                        log_auth_event(
                            AUTH_ACCESS_SQLITE_PATH,
                            user_email=email_value or None,
                            user_role="operator",
                            identifier_attempted=email_value or None,
                            event_type="signup_failure",
                            success_flag=False,
                            failure_reason="account_already_exists",
                            context=context,
                        )
                        st.error("An account with that email already exists.")
        with signup_right:
            st.markdown("#### Registration rules")
            st.dataframe(
                [
                    {"Rule": "Email", "Requirement": "valid and unique"},
                    {"Rule": "Password", "Requirement": "10+ chars, mixed case, one number"},
                    {"Rule": "Verification", "Requirement": "email token required before login"},
                    {"Rule": "Audit trail", "Requirement": "signup, verification, IP and session events are recorded"},
                ],
                use_container_width=True,
                hide_index=True,
            )

    with verify_tab:
        verify_left, verify_right = st.columns([1.1, 0.9])
        with verify_left:
            verify_email = st.text_input("Email", key="verify_email")
            verify_token = st.text_input("Verification token", key="verify_token")
            if st.button("Verify account", type="primary"):
                context = _get_auth_request_context()
                verified = verify_email_token(
                    AUTH_ACCESS_SQLITE_PATH,
                    user_email=verify_email,
                    token=verify_token,
                    context=context,
                )
                if verified:
                    log_auth_event(
                        AUTH_ACCESS_SQLITE_PATH,
                        user_email=verify_email.strip().lower() or None,
                        user_role="operator",
                        identifier_attempted=verify_email.strip().lower() or None,
                        event_type="email_verified",
                        success_flag=True,
                        failure_reason=None,
                        context=context,
                    )
                    st.success("Email verified. The account is now active.")
                else:
                    log_auth_event(
                        AUTH_ACCESS_SQLITE_PATH,
                        user_email=verify_email.strip().lower() or None,
                        user_role="operator",
                        identifier_attempted=verify_email.strip().lower() or None,
                        event_type="email_verification_failure",
                        success_flag=False,
                        failure_reason="invalid_verification_token",
                        context=context,
                    )
                    st.error("Verification failed. Check the email and token.")
        with verify_right:
            st.markdown("#### Verification flow")
            st.dataframe(
                [
                    {"Step": "1", "Action": "Create account"},
                    {"Step": "2", "Action": "Receive token by email"},
                    {"Step": "3", "Action": "Paste token in Verify Email"},
                    {"Step": "4", "Action": "Account becomes active"},
                ],
                use_container_width=True,
                hide_index=True,
            )

    with recovery_tab:
        recovery_email = st.text_input("Recovery email", key="recovery_email")
        recovery_token = st.text_input("Reset token (optional)", key="recovery_token")
        recovery_new_password = st.text_input("New password (optional)", type="password", key="recovery_new_password")
        st.text_area(
            "Recovery message preview",
            value="We have received your password recovery request. If the account is registered, the recovery flow will continue through the configured secure channel.",
            height=120,
            disabled=True,
        )
        if st.button("Request recovery"):
            context = _get_auth_request_context()
            requests = list(st.session_state.get("recovery_requests", []))
            requested_email = recovery_email.strip().lower()
            requests.append(
                {
                    "email": requested_email,
                    "status": "received",
                    "delivery_channel": "configured_secure_channel" if cfg.recovery_notify_email else "not_configured",
                }
            )
            st.session_state["recovery_requests"] = requests[-20:]
            log_auth_event(
                AUTH_ACCESS_SQLITE_PATH,
                user_email=None,
                user_role=None,
                identifier_attempted=requested_email or None,
                event_type="password_reset_requested",
                success_flag=True,
                failure_reason=None,
                context=context,
            )
            reset_token = issue_password_reset_token(
                AUTH_ACCESS_SQLITE_PATH,
                user_email=requested_email,
                context=context,
            )
            _send_access_notification(
                related_user_email=requested_email or None,
                target_email=requested_email or cfg.recovery_notify_email,
                event_type="password_reset_requested",
                subject="H-REVN access recovery request received",
                body=(
                    "We have received a password recovery request for the H-REVN unified workspace.\n\n"
                    f"Reset token: {reset_token or 'not-issued'}\n\n"
                    "If the account is registered and eligible, the secure recovery path will continue through the configured channel.\n\n"
                    "This message is part of the H-REVN access audit trail."
                ),
            )
            if cfg.recovery_notify_email:
                st.success("Recovery request recorded. Delivery is configured for the secure notification path.")
            else:
                st.success("Recovery request recorded in documentary mode. No outbound reset path is configured yet.")

        if st.button("Reset password with token"):
            context = _get_auth_request_context()
            requested_email = recovery_email.strip().lower()
            password_ok, password_message = _password_policy_ok(recovery_new_password)
            if not requested_email or not recovery_token.strip():
                st.error("Email and reset token are required.")
            elif not password_ok:
                st.error(password_message)
            else:
                reset_ok = reset_local_password(
                    AUTH_ACCESS_SQLITE_PATH,
                    user_email=requested_email,
                    token=recovery_token,
                    new_password=recovery_new_password,
                    context=context,
                )
                if reset_ok:
                    log_auth_event(
                        AUTH_ACCESS_SQLITE_PATH,
                        user_email=requested_email,
                        user_role="operator",
                        identifier_attempted=requested_email,
                        event_type="password_reset_completed",
                        success_flag=True,
                        failure_reason=None,
                        context=context,
                    )
                    _send_access_notification(
                        related_user_email=requested_email,
                        target_email=requested_email,
                        event_type="password_reset_completed",
                        subject="H-REVN password reset completed",
                        body="The password for your H-REVN unified workspace account has been updated.",
                    )
                    st.success("Password reset completed.")
                else:
                    log_auth_event(
                        AUTH_ACCESS_SQLITE_PATH,
                        user_email=requested_email or None,
                        user_role="operator",
                        identifier_attempted=requested_email or None,
                        event_type="password_reset_failure",
                        success_flag=False,
                        failure_reason="invalid_reset_token",
                        context=context,
                    )
                    st.error("Password reset failed. Check the token.")

        if st.session_state.get("recovery_requests"):
            st.markdown("#### Recovery queue snapshot")
            st.dataframe(st.session_state["recovery_requests"], use_container_width=True)

    st.stop()


def render_access_security_panel() -> None:
    st.subheader("Access & Security")
    st.caption("Administrative visibility over access events and active authentication sessions in the unified sandbox.")

    snapshot = get_recent_auth_snapshot(AUTH_ACCESS_SQLITE_PATH, limit=50)
    accounts = snapshot.get("accounts", [])
    events = snapshot.get("events", [])
    sessions = snapshot.get("sessions", [])
    lifecycle = snapshot.get("lifecycle", [])
    notifications = snapshot.get("notifications", [])
    ip_controls = snapshot.get("ip_controls", [])

    total_events = len(events)
    login_success = sum(1 for row in events if row.get("event_type") in {"login_success", "login_success_demo"})
    login_failure = sum(1 for row in events if row.get("event_type") == "login_failure")
    active_sessions = sum(1 for row in sessions if row.get("session_state") == "active")
    suspended_accounts = sum(1 for row in accounts if row.get("account_status") == "suspended")
    closed_accounts = sum(1 for row in accounts if row.get("account_status") == "closed")

    top_a, top_b, top_c, top_d, top_e, top_f = st.columns(6)
    with top_a:
        st.metric("Events tracked", total_events)
    with top_b:
        st.metric("Login success", login_success)
    with top_c:
        st.metric("Login failure", login_failure)
    with top_d:
        st.metric("Active sessions", active_sessions)
    with top_e:
        st.metric("Suspended", suspended_accounts)
    with top_f:
        st.metric("Closed", closed_accounts)

    tab_accounts, tab_events, tab_sessions, tab_lifecycle, tab_notifications, tab_ip = st.tabs(["Accounts", "Access Events", "Active Sessions", "Lifecycle", "Notifications", "IP Controls"])

    with tab_accounts:
        account_rows = [
            {
                "USER": row.get("user_email") or "-",
                "ROLE": row.get("user_role") or "-",
                "STATUS": row.get("account_status") or "-",
                "SOURCE": row.get("account_source") or "-",
                "SUSPENDED": row.get("suspended_at_utc") or "-",
                "CLOSED": row.get("closed_at_utc") or "-",
            }
            for row in accounts
        ]
        if account_rows:
            st.dataframe(account_rows, use_container_width=True, hide_index=True)

            selectable_accounts = [row["USER"] for row in account_rows]
            selected_account = st.selectbox("Account", selectable_accounts, key="auth_account_target")
            account_row = next((row for row in accounts if (row.get("user_email") or "").lower() == selected_account.lower()), None) or {}

            left, middle, right = st.columns([1, 1, 1.2])
            with left:
                st.markdown("##### Account")
                st.dataframe(
                    [{
                        "USER": account_row.get("user_email") or "-",
                        "ROLE": account_row.get("user_role") or "-",
                        "STATUS": account_row.get("account_status") or "-",
                        "FAILED LOGINS": account_row.get("failed_login_count") or 0,
                        "LOCKOUT": account_row.get("lockout_until_utc") or "-",
                    }],
                    use_container_width=True,
                    hide_index=True,
                )
            with middle:
                st.markdown("##### Session impact")
                impacted_sessions = [
                    row for row in sessions
                    if (row.get("user_email") or "").lower() == selected_account.lower() and row.get("session_state") == "active"
                ]
                st.dataframe(
                    [{
                        "ACTIVE SESSIONS": len(impacted_sessions),
                        "ACTION": "revoked on suspend/close",
                    }],
                    use_container_width=True,
                    hide_index=True,
                )
                related_events = [
                    row for row in events
                    if ((row.get("user_email") or "").lower() == selected_account.lower())
                    or ((row.get("identifier_attempted") or "").lower() == selected_account.lower())
                ]
                related_lifecycle = [
                    row for row in lifecycle
                    if (row.get("user_email") or "").lower() == selected_account.lower()
                ]
                related_notifications = [
                    row for row in notifications
                    if ((row.get("related_user_email") or "").lower() == selected_account.lower())
                    or ((row.get("target_email") or "").lower() == selected_account.lower())
                ]
                related_ips = sorted(
                    {
                        row.get("ip_public") or "-"
                        for row in related_events
                        if row.get("ip_public")
                    }
                )
                st.dataframe(
                    [{
                        "RELATED EVENTS": len(related_events),
                        "LIFECYCLE EVENTS": len(related_lifecycle),
                        "NOTIFICATIONS": len(related_notifications),
                        "KNOWN IPS": len([item for item in related_ips if item != "-"]),
                    }],
                    use_container_width=True,
                    hide_index=True,
                )
            with right:
                st.markdown("##### Decision rationale")
                action_reason = st.text_area(
                    "Admin rationale",
                    key=f"auth_admin_reason_{selected_account}",
                    placeholder="Reason for suspend / close / reactivate",
                    height=140,
                )
                current_status = str(account_row.get("account_status") or "active")
                current_admin_email = (st.session_state.get("auth_email") or "").strip().lower()
                is_self = selected_account.strip().lower() == current_admin_email
                if is_self:
                    st.warning("Self-suspend and self-close are blocked in the admin panel.")
                button_a, button_b, button_c = st.columns(3)
                context = _get_auth_request_context()
                if button_a.button("Suspend", use_container_width=True, disabled=is_self or current_status == "closed"):
                    set_account_status(
                        AUTH_ACCESS_SQLITE_PATH,
                        user_email=selected_account,
                        resulting_status="suspended",
                        performed_by_user_email=st.session_state.get("auth_email"),
                        performed_by_user_role=st.session_state.get("auth_role"),
                        reason=(action_reason or "").strip() or None,
                        context=context,
                    )
                    log_auth_event(
                        AUTH_ACCESS_SQLITE_PATH,
                        user_email=selected_account,
                        user_role=account_row.get("user_role"),
                        identifier_attempted=selected_account,
                        event_type="account_suspended",
                        success_flag=True,
                        failure_reason=None,
                        context=context,
                        details_json=json.dumps({"reason": (action_reason or "").strip() or None}),
                    )
                    _send_access_notification(
                        related_user_email=selected_account,
                        target_email=selected_account,
                        event_type="account_suspended",
                        subject="H-REVN access suspended",
                        body=(
                            f"Access for {selected_account} has been suspended in the H-REVN unified workspace.\n\n"
                            f"Reason: {(action_reason or '').strip() or 'No reason provided'}\n\n"
                            "Any active sessions were revoked as part of this action."
                        ),
                    )
                    _send_telegram_security_alert(
                        "account_suspended",
                        f"H-REVN security alert: account suspended for {selected_account}.",
                    )
                    st.success("Account suspended.")
                    st.rerun()
                close_disabled = is_self or current_status == "closed" or not (action_reason or "").strip()
                if button_b.button("Close", use_container_width=True, disabled=close_disabled):
                    set_account_status(
                        AUTH_ACCESS_SQLITE_PATH,
                        user_email=selected_account,
                        resulting_status="closed",
                        performed_by_user_email=st.session_state.get("auth_email"),
                        performed_by_user_role=st.session_state.get("auth_role"),
                        reason=(action_reason or "").strip() or None,
                        context=context,
                    )
                    log_auth_event(
                        AUTH_ACCESS_SQLITE_PATH,
                        user_email=selected_account,
                        user_role=account_row.get("user_role"),
                        identifier_attempted=selected_account,
                        event_type="account_closed",
                        success_flag=True,
                        failure_reason=None,
                        context=context,
                        details_json=json.dumps({"reason": (action_reason or "").strip() or None}),
                    )
                    _send_access_notification(
                        related_user_email=selected_account,
                        target_email=selected_account,
                        event_type="account_closed",
                        subject="H-REVN account closed",
                        body=(
                            f"Access for {selected_account} has been closed in the H-REVN unified workspace.\n\n"
                            f"Reason: {(action_reason or '').strip()}\n\n"
                            "Any active sessions were revoked as part of this action."
                        ),
                    )
                    _send_telegram_security_alert(
                        "account_closed",
                        f"H-REVN security alert: account closed for {selected_account}.",
                    )
                    st.success("Account closed.")
                    st.rerun()
                if button_c.button("Reactivate", use_container_width=True, disabled=current_status == "active"):
                    reactivate_account(
                        AUTH_ACCESS_SQLITE_PATH,
                        user_email=selected_account,
                        performed_by_user_email=st.session_state.get("auth_email"),
                        performed_by_user_role=st.session_state.get("auth_role"),
                        reason=(action_reason or "").strip() or None,
                        context=context,
                    )
                    log_auth_event(
                        AUTH_ACCESS_SQLITE_PATH,
                        user_email=selected_account,
                        user_role=account_row.get("user_role"),
                        identifier_attempted=selected_account,
                        event_type="account_reactivated",
                        success_flag=True,
                        failure_reason=None,
                        context=context,
                        details_json=json.dumps({"reason": (action_reason or "").strip() or None}),
                    )
                    _send_access_notification(
                        related_user_email=selected_account,
                        target_email=selected_account,
                        event_type="account_reactivated",
                        subject="H-REVN account reactivated",
                        body=(
                            f"Access for {selected_account} has been reactivated in the H-REVN unified workspace.\n\n"
                            f"Reason: {(action_reason or '').strip() or 'No reason provided'}"
                        ),
                    )
                    _send_telegram_security_alert(
                        "account_reactivated",
                        f"H-REVN security alert: account reactivated for {selected_account}.",
                    )
                    st.success("Account reactivated.")
                    st.rerun()
                if st.button("Revoke active sessions", use_container_width=True, key=f"revoke_sessions_{selected_account}"):
                    revoked = revoke_all_active_sessions_for_user(
                        AUTH_ACCESS_SQLITE_PATH,
                        user_email=selected_account,
                    )
                    log_auth_event(
                        AUTH_ACCESS_SQLITE_PATH,
                        user_email=selected_account,
                        user_role=account_row.get("user_role"),
                        identifier_attempted=selected_account,
                        event_type="sessions_revoked_by_admin",
                        success_flag=True,
                        failure_reason=None,
                        context=context,
                        details_json=json.dumps({"revoked_sessions": revoked}),
                    )
                    _send_telegram_security_alert(
                        "sessions_revoked_by_admin",
                        f"H-REVN security alert: admin revoked {revoked} active session(s) for {selected_account}.",
                    )
                    st.success(f"Revoked {revoked} active session(s).")
                    st.rerun()
                if current_status != "active" and is_self:
                    st.info("Self-management remains blocked for suspend and close.")
                elif current_status != "active" and not (action_reason or "").strip():
                    st.caption("A reason is required before closing an account.")
            st.markdown("##### Security history")
            history_tab_a, history_tab_b, history_tab_c, history_tab_d = st.tabs(["Access", "Sessions", "Lifecycle", "Notifications"])
            with history_tab_a:
                if related_events:
                    history_rows = [
                        {
                            "WHEN": row.get("created_at_utc") or "-",
                            "EVENT": row.get("event_type") or "-",
                            "RESULT": "success" if int(row.get("success_flag") or 0) else "failure",
                            "IP": row.get("ip_public") or "-",
                            "REASON": row.get("failure_reason") or "-",
                        }
                        for row in related_events
                    ]
                    st.dataframe(history_rows, use_container_width=True, hide_index=True)
                else:
                    st.info("No access history for this account yet.")
            with history_tab_b:
                if impacted_sessions or any((row.get("user_email") or "").lower() == selected_account.lower() for row in sessions):
                    session_history_rows = [
                        {
                            "SESSION": row.get("session_id") or "-",
                            "STATE": row.get("session_state") or "-",
                            "IP": row.get("ip_public") or "-",
                            "CREATED": row.get("created_at_utc") or "-",
                            "LAST SEEN": row.get("last_seen_at_utc") or "-",
                            "REVOKED": row.get("revoked_at_utc") or "-",
                        }
                        for row in sessions
                        if (row.get("user_email") or "").lower() == selected_account.lower()
                    ]
                    st.dataframe(session_history_rows, use_container_width=True, hide_index=True)
                else:
                    st.info("No session history for this account yet.")
            with history_tab_c:
                if related_lifecycle:
                    lifecycle_rows = [
                        {
                            "WHEN": row.get("created_at_utc") or "-",
                            "EVENT": row.get("event_type") or "-",
                            "FROM": row.get("previous_status") or "-",
                            "TO": row.get("resulting_status") or "-",
                            "BY": row.get("performed_by_user_email") or "-",
                            "REASON": row.get("reason") or "-",
                        }
                        for row in related_lifecycle
                    ]
                    st.dataframe(lifecycle_rows, use_container_width=True, hide_index=True)
                else:
                    st.info("No lifecycle history for this account yet.")
            with history_tab_d:
                if related_notifications:
                    notification_rows = [
                        {
                            "WHEN": row.get("created_at_utc") or "-",
                            "EVENT": row.get("event_type") or "-",
                            "TARGET": row.get("target_email") or "-",
                            "CHANNEL": row.get("delivery_channel") or "-",
                            "STATUS": row.get("delivery_status") or "-",
                        }
                        for row in related_notifications
                    ]
                    st.dataframe(notification_rows, use_container_width=True, hide_index=True)
                else:
                    st.info("No notification history for this account yet.")
            if related_ips:
                st.markdown("##### Related IPs")
                st.dataframe([{"IP": ip_value} for ip_value in related_ips], use_container_width=True, hide_index=True)
        else:
            st.info("No accounts registered yet.")

    with tab_events:
        if not events:
            st.info("No access events recorded yet.")
        else:
            event_rows = [
                {
                    "WHEN": row.get("created_at_utc") or "-",
                    "EVENT": row.get("event_type") or "-",
                    "USER": row.get("user_email") or "-",
                    "ROLE": row.get("user_role") or "-",
                    "IDENTIFIER": row.get("identifier_attempted") or "-",
                    "IP": row.get("ip_public") or "-",
                    "RESULT": "success" if int(row.get("success_flag") or 0) else "failure",
                    "REASON": row.get("failure_reason") or "-",
                }
                for row in events
            ]
            st.dataframe(event_rows, use_container_width=True, hide_index=True)

    with tab_sessions:
        if not sessions:
            st.info("No auth sessions recorded yet.")
        else:
            session_rows = [
                {
                    "SESSION": row.get("session_id") or "-",
                    "USER": row.get("user_email") or "-",
                    "ROLE": row.get("user_role") or "-",
                    "STATE": row.get("session_state") or "-",
                    "IP": row.get("ip_public") or "-",
                    "CREATED": row.get("created_at_utc") or "-",
                    "LAST SEEN": row.get("last_seen_at_utc") or "-",
                    "REVOKED": row.get("revoked_at_utc") or "-",
                }
                for row in sessions
            ]
            st.dataframe(session_rows, use_container_width=True, hide_index=True)

    with tab_lifecycle:
        if not lifecycle:
            st.info("No account lifecycle changes recorded yet.")
        else:
            lifecycle_rows = [
                {
                    "WHEN": row.get("created_at_utc") or "-",
                    "USER": row.get("user_email") or "-",
                    "ROLE": row.get("user_role") or "-",
                    "EVENT": row.get("event_type") or "-",
                    "FROM": row.get("previous_status") or "-",
                    "TO": row.get("resulting_status") or "-",
                    "BY": row.get("performed_by_user_email") or "-",
                    "REASON": row.get("reason") or "-",
                }
                for row in lifecycle
            ]
            st.dataframe(lifecycle_rows, use_container_width=True, hide_index=True)

    with tab_notifications:
        if not notifications:
            st.info("No access notifications recorded yet.")
        else:
            notification_rows = [
                {
                    "WHEN": row.get("created_at_utc") or "-",
                    "EVENT": row.get("event_type") or "-",
                    "USER": row.get("related_user_email") or "-",
                    "TARGET": row.get("target_email") or "-",
                    "CHANNEL": row.get("delivery_channel") or "-",
                    "STATUS": row.get("delivery_status") or "-",
                    "SUBJECT": row.get("subject") or "-",
                    "ERROR": row.get("error_detail") or "-",
                }
                for row in notifications
            ]
            st.dataframe(notification_rows, use_container_width=True, hide_index=True)

    with tab_ip:
        if not ip_controls:
            st.info("No IP controls recorded yet.")
        else:
            ip_rows = [
                {
                    "IP": row.get("ip_public") or "-",
                    "FAILED LOGINS": row.get("failed_login_count") or 0,
                    "LAST FAILED": row.get("last_failed_login_at_utc") or "-",
                    "COOLDOWN": row.get("cooldown_until_utc") or "-",
                    "BLOCKED": row.get("blocked_until_utc") or "-",
                    "REASON": row.get("block_reason") or "-",
                }
                for row in ip_controls
            ]
            st.dataframe(ip_rows, use_container_width=True, hide_index=True)
            selectable_ips = [row["IP"] for row in ip_rows if row["IP"] != "-"]
            if selectable_ips:
                selected_ip = st.selectbox("IP", selectable_ips, key="auth_ip_target")
                if st.button("Unblock IP", use_container_width=True):
                    unblock_ip(AUTH_ACCESS_SQLITE_PATH, ip_public=selected_ip)
                    log_auth_event(
                        AUTH_ACCESS_SQLITE_PATH,
                        user_email=st.session_state.get("auth_email"),
                        user_role=st.session_state.get("auth_role"),
                        identifier_attempted=selected_ip,
                        event_type="ip_unblocked_by_admin",
                        success_flag=True,
                        failure_reason=None,
                        context=_get_auth_request_context(),
                    )
                    _send_telegram_security_alert(
                        "ip_unblocked_by_admin",
                        f"H-REVN security alert: admin unblocked IP {selected_ip}.",
                    )
                    st.success("IP unblocked.")
                    st.rerun()


def render_central_console() -> None:
    st.subheader("Central Console")
    st.caption("Executive business view and technical architecture view for the unified sandbox.")

    business_tab, technical_tab = st.tabs(["Business", "Technical Architecture"])

    with business_tab:
        snapshot = get_recent_auth_snapshot(AUTH_ACCESS_SQLITE_PATH, limit=100)
        accounts = snapshot.get("accounts", [])
        events = snapshot.get("events", [])
        sessions = snapshot.get("sessions", [])
        notifications = snapshot.get("notifications", [])
        ip_controls = snapshot.get("ip_controls", [])

        locked_accounts = [row for row in accounts if row.get("lockout_until_utc")]
        suspended_accounts = [row for row in accounts if row.get("account_status") == "suspended"]
        closed_accounts = [row for row in accounts if row.get("account_status") == "closed"]
        blocked_ips = [row for row in ip_controls if row.get("blocked_until_utc")]
        cooldown_ips = [row for row in ip_controls if row.get("cooldown_until_utc") and not row.get("blocked_until_utc")]
        active_sessions = [row for row in sessions if row.get("session_state") == "active"]
        failed_events = [row for row in events if int(row.get("success_flag") or 0) == 0]
        critical_notifications = [
            row for row in notifications
            if (row.get("event_type") or "") in {
                "user_lockout",
                "ip_cooldown",
                "ip_blocked",
                "account_suspended",
                "account_closed",
                "sessions_revoked_by_admin",
            }
        ]

        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric("Accounts", len(accounts))
        m2.metric("Active sessions", len(active_sessions))
        m3.metric("Locked accounts", len(locked_accounts))
        m4.metric("Suspended", len(suspended_accounts))
        m5.metric("Blocked IPs", len(blocked_ips))
        m6.metric("Critical alerts", len(critical_notifications))

        row_left, row_right = st.columns([1.15, 0.85])
        with row_left:
            st.markdown("##### Current risk picture")
            risk_rows = [
                {"AREA": "Accounts", "STATE": "locked", "COUNT": len(locked_accounts)},
                {"AREA": "Accounts", "STATE": "suspended", "COUNT": len(suspended_accounts)},
                {"AREA": "Accounts", "STATE": "closed", "COUNT": len(closed_accounts)},
                {"AREA": "IPs", "STATE": "cooldown", "COUNT": len(cooldown_ips)},
                {"AREA": "IPs", "STATE": "blocked", "COUNT": len(blocked_ips)},
                {"AREA": "Access", "STATE": "failed events", "COUNT": len(failed_events)},
            ]
            st.dataframe(risk_rows, use_container_width=True, hide_index=True)

            st.markdown("##### Critical events")
            critical_event_rows = [
                {
                    "WHEN": row.get("created_at_utc") or "-",
                    "EVENT": row.get("event_type") or "-",
                    "IDENTIFIER": row.get("identifier_attempted") or row.get("user_email") or "-",
                    "IP": row.get("ip_public") or "-",
                    "REASON": row.get("failure_reason") or "-",
                }
                for row in events
                if (row.get("event_type") or "") in {
                    "login_failure",
                    "account_suspended",
                    "account_closed",
                    "sessions_revoked_by_admin",
                    "ip_unblocked_by_admin",
                }
            ][:20]
            if critical_event_rows:
                st.dataframe(critical_event_rows, use_container_width=True, hide_index=True)
            else:
                st.info("No critical access events recorded yet.")

        with row_right:
            st.markdown("##### Most exposed entities")
            exposed_accounts = sorted(
                [
                    {
                        "USER": row.get("user_email") or "-",
                        "STATUS": row.get("account_status") or "-",
                        "FAILED LOGINS": row.get("failed_login_count") or 0,
                        "LOCKOUT": row.get("lockout_until_utc") or "-",
                    }
                    for row in accounts
                ],
                key=lambda item: (int(item["FAILED LOGINS"]), item["STATUS"] != "active"),
                reverse=True,
            )[:10]
            if exposed_accounts:
                st.dataframe(exposed_accounts, use_container_width=True, hide_index=True)
            else:
                st.info("No account exposure data yet.")

            st.markdown("##### IP pressure")
            ip_rows = [
                {
                    "IP": row.get("ip_public") or "-",
                    "FAILED LOGINS": row.get("failed_login_count") or 0,
                    "COOLDOWN": row.get("cooldown_until_utc") or "-",
                    "BLOCKED": row.get("blocked_until_utc") or "-",
                }
                for row in ip_controls
            ][:10]
            if ip_rows:
                st.dataframe(ip_rows, use_container_width=True, hide_index=True)
            else:
                st.info("No IP control events yet.")

            st.markdown("##### Delivery health")
            delivery_rows = [
                {
                    "CHANNEL": row.get("delivery_channel") or "-",
                    "EVENT": row.get("event_type") or "-",
                    "STATUS": row.get("delivery_status") or "-",
                    "TARGET": row.get("target_email") or "-",
                }
                for row in notifications[:12]
            ]
            if delivery_rows:
                st.dataframe(delivery_rows, use_container_width=True, hide_index=True)
            else:
                st.info("No notification delivery data yet.")

    with technical_tab:
        render_dry_run_dashboard()


def _prepare_real_estate_context(snapshot, visit_id: str) -> dict:
    visits = snapshot.visits
    assets = snapshot.assets
    observations = snapshot.observations
    photos = snapshot.photos

    selected_visit = next((item for item in visits if item.get("visit_id") == visit_id), None)
    selected_asset = None
    if selected_visit:
        selected_asset = next(
            (item for item in assets if item.get("asset_id") == selected_visit.get("asset_id")),
            None,
        )

    selected_observations = [item for item in observations if item.get("visit_id") == visit_id]
    selected_photos = [item for item in photos if item.get("visit_id") == visit_id]

    lpi_options = sorted({str(item.get("lpi_code") or "").strip() for item in observations if item.get("lpi_code")})
    severity_counts: dict[int, int] = {}
    for item in selected_observations:
        sev = int(item.get("severity_0_5") or 0)
        severity_counts[sev] = severity_counts.get(sev, 0) + 1

    return {
        "selected_visit": selected_visit,
        "selected_asset": selected_asset,
        "selected_observations": selected_observations,
        "selected_photos": selected_photos,
        "lpi_options": lpi_options,
        "severity_counts": severity_counts,
    }


def _build_real_estate_readiness(context: dict, workspace: dict | None) -> RealEstateReadiness:
    observations = context["selected_observations"]
    photos = context["selected_photos"]
    all_observations_have_lpi = bool(workspace.get("all_observations_have_lpi")) if workspace else False
    required_photos = int(workspace.get("total_required_photos") or 0) if workspace else 0
    present_photos = int(workspace.get("total_present_photos") or len(photos)) if workspace else len(photos)
    min_photos_ok = present_photos >= required_photos if observations else False
    naming_policy_ready = bool(context["selected_visit"]) and bool(context["selected_asset"])
    ai_gate_ready = len(photos) > 0
    already_issued = bool(workspace.get("already_issued")) if workspace else False
    lpi_dictionary_size = int(workspace.get("lpi_dictionary_size") or 0) if workspace else 0
    issuance_ready = bool(workspace.get("issuance_ready")) if workspace else False
    return RealEstateReadiness(
        observation_count=len(observations),
        photo_count=len(photos),
        required_photo_count=required_photos,
        all_observations_have_lpi=all_observations_have_lpi,
        min_photos_ok=min_photos_ok,
        naming_policy_ready=naming_policy_ready,
        ai_gate_ready=ai_gate_ready,
        already_issued=already_issued,
        lpi_dictionary_size=lpi_dictionary_size,
        issuance_ready=issuance_ready,
    )


def _render_real_estate_overview(snapshot, context: dict, readiness: RealEstateReadiness) -> None:
    assets = snapshot.assets
    sessions = snapshot.visits
    cities = sorted(
        {
            str(item.get("asset_city")).strip()
            for item in assets
            if isinstance(item, dict) and item.get("asset_city")
        }
    )

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Assets", len(assets))
    m2.metric("Visits", len(sessions))
    m3.metric("Observations", readiness.observation_count)
    m4.metric("Photos", readiness.photo_count)

    st.markdown("### Portfolio snapshot")
    c1, c2 = st.columns([1.2, 0.8])
    with c1:
        st.dataframe(sessions, use_container_width=True)
    with c2:
        st.write({"cities": cities, "asset_count": len(assets), "visit_count": len(sessions)})


def _render_real_estate_workspace(context: dict, workspace: dict | None, readiness: RealEstateReadiness, cfg) -> None:
    workspace = workspace or {}
    visit = workspace.get("visit") or context["selected_visit"] or {}
    asset = workspace.get("asset") or context["selected_asset"] or {}
    observations = workspace.get("observations") or context["selected_observations"]
    photos = workspace.get("photos") or context["selected_photos"]
    severity_counts = context["severity_counts"]
    photo_counts = workspace.get("photo_counts_by_record") or {}

    header_left, header_right = st.columns([1.2, 0.8])
    with header_left:
        st.markdown("### Visit workspace")
        st.write(
            {
                "visit_id": visit.get("visit_id"),
                "asset_id": visit.get("asset_id"),
                "asset_public_id": asset.get("asset_public_id"),
                "visit_date_utc": visit.get("visit_date_utc"),
                "inspector_name": visit.get("inspector_name"),
                "client_name": asset.get("client_name"),
                "asset_name": asset.get("asset_name"),
                "asset_city": asset.get("asset_city"),
                "asset_type": asset.get("asset_type") or asset.get("asset_template_type"),
            }
        )
    with header_right:
        st.markdown("### Issuance readiness")
        st.write(
            {
                "all_observations_have_lpi": readiness.all_observations_have_lpi,
                "required_photos_total": readiness.required_photo_count,
                "present_photos_total": readiness.photo_count,
                "minimum_photo_rule_ok": readiness.min_photos_ok,
                "naming_policy_ready": readiness.naming_policy_ready,
                "ai_gate_ready": readiness.ai_gate_ready,
                "already_issued": readiness.already_issued,
                "issuance_ready": readiness.issuance_ready,
            }
        )
        if readiness.already_issued:
            st.info("This visit already has issuance traces recorded in the SQLite snapshot.")
        elif readiness.issuance_ready:
            st.success("This visit is structurally ready for the issuance stage.")
        else:
            st.warning("This visit still needs one or more readiness conditions before issuance.")

    obs_col, detail_col = st.columns([1.1, 0.9])
    with obs_col:
        st.markdown("### Observation list")
        if observations:
            labels = [
                f"{item.get('record_uuid')} | {item.get('lpi_code') or '-'} | {item.get('lpi_title') or 'untitled'} | sev {item.get('severity_0_5') or 0}"
                for item in observations
            ]
            selected_label = st.selectbox("Select observation", labels, key="workspace_observation")
            selected_id = selected_label.split(" | ")[0]
            selected_observation = next((item for item in observations if item.get("record_uuid") == selected_id), observations[0])
            st.dataframe(
                [
                    {
                        "record_uuid": item.get("record_uuid"),
                        "lpi_code": item.get("lpi_code"),
                        "lpi_title": item.get("lpi_title"),
                        "severity_0_5": item.get("severity_0_5"),
                        "min_photos_required": item.get("min_photos_required"),
                        "photos_for_observation": photo_counts.get(str(item.get("record_uuid") or ""), 0),
                        "review_status": item.get("review_status"),
                        "row_status": item.get("row_status"),
                    }
                    for item in observations
                ],
                use_container_width=True,
            )
        else:
            selected_observation = {}
            st.info("No observations found for this visit.")
    with detail_col:
        st.markdown("### Observation detail")
        if selected_observation:
            record_uuid = str(selected_observation.get("record_uuid") or "")
            st.text_input("record_uuid", value=record_uuid, disabled=True)
            st.text_input("lpi_code", value=str(selected_observation.get("lpi_code") or ""), disabled=True)
            st.text_input("lpi_title", value=str(selected_observation.get("lpi_title") or ""), disabled=True)
            st.number_input("severity_0_5", min_value=0, max_value=5, value=int(selected_observation.get("severity_0_5") or 0), disabled=True)
            st.number_input("min_photos_required", min_value=0, max_value=50, value=int(selected_observation.get("min_photos_required") or 0), disabled=True)
            st.number_input("photos_for_observation", min_value=0, max_value=50, value=int(photo_counts.get(record_uuid, 0)), disabled=True)
            st.text_input("review_status", value=str(selected_observation.get("review_status") or ""), disabled=True)
            st.text_input("row_status", value=str(selected_observation.get("row_status") or ""), disabled=True)
            st.text_area("observation_description", value=str(selected_observation.get("observation_description") or ""), height=140, disabled=True)
            st.text_area("coordinator_notes", value=str(selected_observation.get("coordinator_notes") or ""), height=120, disabled=True)
            if int(selected_observation.get("out_of_scope_flag") or 0):
                st.warning(f"Out of scope: {selected_observation.get('out_of_scope_reason') or 'no reason provided'}")
        st.markdown("### Severity mix")
        st.write(severity_counts or {"no_data": 0})

    photo_col, ai_col = st.columns([1.1, 0.9])
    with photo_col:
        st.markdown("### Photo queue")
        st.dataframe(
            [
                {
                    "photo_uuid": item.get("photo_uuid"),
                    "record_uuid": item.get("record_uuid"),
                    "photo_role": item.get("photo_role"),
                    "photo_filename": item.get("photo_filename"),
                    "quality_flags": item.get("quality_flags"),
                    "captured_at_utc": item.get("captured_at_utc"),
                }
                for item in photos
            ],
            use_container_width=True,
        )
        next_number = len(photos) + 1
        preview_filename = f"{visit.get('asset_id') or 'AST'}_{visit.get('visit_id') or 'VIS'}_{next_number:03d}"
        st.text_input("Next technical filename preview", value=preview_filename, disabled=True)
        st.caption("Technical filename stays vertical-specific. Semantic title remains an AI suggestion layer.")
    with ai_col:
        st.markdown("### AI review state")
        quality_state = "ready_for_review" if photos else "blocked_no_photos"
        st.write(
            {
                "provider": choose_ai_provider(cfg).selected,
                "review_state": quality_state,
                "blocking_policy": "block_if_inconsistencies_detected",
                "semantic_titles_mode": "planned_common_contract",
                "delivery_mode": "async_after_finalize",
                "blockchain_target": cfg.blockchain_target,
            }
        )
        if photos:
            sample_title = f"Proposed title: {asset.get('asset_public_id') or visit.get('asset_id') or 'asset'} / visit evidence / first image"
            st.text_area("Semantic title preview", value=sample_title, height=100, disabled=True)

    st.markdown("### Finalization behavior")
    st.info(
        "Target V1 behavior: when the operator finalizes the visit, the system moves to async issuance, runs AI pre-issuance checks, and delivers the certificate by email when ready. Anchoring remains configured against the selected blockchain target."
    )


def _render_legacy_panel_a(context: dict) -> None:
    st.markdown("### Legacy Panel A — Operational Observation Panel")
    st.caption("Recreated from `hrevn_panel.py`. Read-only comparison of the legacy observation workflow.")

    visit = context["selected_visit"] or {}
    asset = context["selected_asset"] or {}
    observations = context["selected_observations"]
    photos = context["selected_photos"]
    lpi_options = context["lpi_options"]

    st.write(
        {
            "visit_id": visit.get("visit_id"),
            "asset_id": visit.get("asset_id"),
            "asset_public_id": asset.get("asset_public_id"),
            "asset_template_type": asset.get("asset_template_type"),
            "asset_type": asset.get("asset_type"),
        }
    )

    left, right = st.columns(2)
    with left:
        st.markdown("#### Ficha de observación")
        if observations:
            labels = [f"{item.get('record_uuid')} ({item.get('lpi_code') or '-'})" for item in observations]
            selected_label = st.selectbox("Observations in visit", labels, key="legacy_a_observation")
            record_uuid = selected_label.split(" (")[0]
            selected_observation = next(
                (item for item in observations if item.get("record_uuid") == record_uuid),
                observations[0],
            )
        else:
            selected_observation = {}
            st.info("No observations found for this visit.")

        current_lpi = str(selected_observation.get("lpi_code") or "")
        lpi_index = lpi_options.index(current_lpi) if current_lpi in lpi_options else 0
        st.selectbox(
            "LPI code (official)",
            options=lpi_options or [""],
            index=lpi_index if lpi_options else 0,
            disabled=True,
            key="legacy_a_lpi",
        )
        severity = int(selected_observation.get("severity_0_5") or 0)
        st.selectbox(
            "Severity (0-5)",
            options=[0, 1, 2, 3, 4, 5],
            index=[0, 1, 2, 3, 4, 5].index(severity),
            disabled=True,
            key="legacy_a_severity",
        )
        min_photos = 3 if severity >= 3 else 1
        st.info(f"Legacy auto rule: minimum photos required = {min_photos}")
        st.text_area(
            "Description",
            value=str(selected_observation.get("observation_description") or ""),
            height=140,
            disabled=True,
            key="legacy_a_description",
        )
        st.text_area(
            "Coordinator notes",
            value=str(selected_observation.get("coordinator_notes") or ""),
            height=120,
            disabled=True,
            key="legacy_a_notes",
        )

    with right:
        st.markdown("#### Photos for current visit")
        st.metric("Registered photos", len(photos))
        enough = len(photos) >= min_photos if observations else False
        if observations:
            if enough:
                st.success(f"Legacy rule satisfied: {len(photos)}/{min_photos} photos.")
            else:
                st.warning(f"Legacy rule not satisfied: {len(photos)}/{min_photos} photos.")
        st.dataframe(photos, use_container_width=True)
        st.file_uploader(
            "Upload photos (legacy flow preview)",
            type=["jpg", "jpeg", "png"],
            accept_multiple_files=True,
            disabled=True,
            key="legacy_a_uploader",
        )
        st.caption("Upload is disabled here. This panel is a visual recovery of the old operational layout.")


def _render_legacy_panel_b(context: dict) -> None:
    st.markdown("### Legacy Panel B — Structured Data Entry Panel")
    st.caption("Recreated from `hrevn_panel_data_entry.py`. Shows the old staged flow: client -> asset -> visit -> observation -> photo -> emit S1.")

    visit = context["selected_visit"] or {}
    asset = context["selected_asset"] or {}
    observations = context["selected_observations"]
    photos = context["selected_photos"]

    client_name = asset.get("client_name") or visit.get("client_name") or ""
    client_id = f"CLI-{str(asset.get('asset_id') or '0000').split('-')[-1]}" if asset else "CLI-0000"

    s1, s2, s3 = st.columns(3)
    with s1:
        st.markdown("#### 1. Client")
        st.text_input("client_id", value=client_id, disabled=True, key="legacy_b_client_id")
        st.text_input("client_name", value=str(client_name), disabled=True, key="legacy_b_client_name")
    with s2:
        st.markdown("#### 2. Asset")
        st.text_input("asset_id", value=str(asset.get("asset_id") or ""), disabled=True, key="legacy_b_asset_id")
        st.text_input("asset_public_id", value=str(asset.get("asset_public_id") or ""), disabled=True, key="legacy_b_asset_public_id")
        st.text_input("asset_template_type", value=str(asset.get("asset_template_type") or ""), disabled=True, key="legacy_b_asset_template_type")
    with s3:
        st.markdown("#### 3. Visit")
        st.text_input("visit_id", value=str(visit.get("visit_id") or ""), disabled=True, key="legacy_b_visit_id")
        st.text_input("visit_date_utc", value=str(visit.get("visit_date_utc") or ""), disabled=True, key="legacy_b_visit_date")
        st.text_input("inspector_name", value=str(visit.get("inspector_name") or ""), disabled=True, key="legacy_b_inspector")

    o1, o2 = st.columns(2)
    with o1:
        st.markdown("#### 4. Observation creation")
        st.metric("Observations in selected visit", len(observations))
        st.dataframe(observations, use_container_width=True)
    with o2:
        st.markdown("#### 5. Photo registration")
        st.metric("Photos in selected visit", len(photos))
        st.dataframe(photos, use_container_width=True)

    st.markdown("#### 6. Emit S1")
    st.write(
        {
            "emit_ready": bool(visit) and len(observations) > 0,
            "legacy_expected_outputs": ["baseline_log", "visit_report", "certificate"],
            "mode": "read_only_comparison",
        }
    )


def _render_legacy_panel_c(context: dict) -> None:
    st.markdown("### Legacy Panel C — Excel UI Panel v2")
    st.caption("Recreated from `hrevn_make_ui_panel_v2.py`. This was a workbook-embedded lightweight capture sheet.")

    visit = context["selected_visit"] or {}
    observations = context["selected_observations"]
    first_observation = observations[0] if observations else {}
    lpi_display = str(first_observation.get("lpi_code") or "")

    c1, c2 = st.columns([1, 1])
    with c1:
        st.text_input("visit_id", value=str(visit.get("visit_id") or ""), disabled=True, key="legacy_c_visit_id")
        st.text_input("LPI", value=lpi_display, disabled=True, key="legacy_c_lpi")
        st.number_input(
            "Severity (0-5)",
            min_value=0,
            max_value=5,
            value=int(first_observation.get("severity_0_5") or 0),
            disabled=True,
            key="legacy_c_severity",
        )
    with c2:
        st.text_area(
            "Descripción",
            value=str(first_observation.get("observation_description") or ""),
            height=130,
            disabled=True,
            key="legacy_c_description",
        )
        photo_name = context["selected_photos"][0].get("photo_filename") if context["selected_photos"] else ""
        st.text_input("photo_filename (optional)", value=str(photo_name or ""), disabled=True, key="legacy_c_photo_name")

    st.write(
        {
            "lpi_code_auto": lpi_display,
            "dirty_flag": 1 if visit or first_observation or photo_name else 0,
            "panel_model": "excel_embedded_ui",
        }
    )


def _controlled_actions_status_label(value: str) -> str:
    labels = {
        "pending_review": "Pending review",
        "approved_for_execution": "Approved for execution",
        "executed_sealed": "Executed and sealed",
        "rejected": "Rejected",
    }
    return labels.get(value, value.replace("_", " ").title())


def _render_panel_section_title(label: str) -> None:
    st.markdown(
        f"<div style='font-size:0.72rem;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;margin:0 0 6px 0;color:#1f2937;'>{label}</div>",
        unsafe_allow_html=True,
    )




def render_controlled_actions_vertical() -> None:
    snapshot = load_agent_operations_snapshot(AGENT_OPERATIONS_SQLITE_PATH)
    records = list(snapshot.records)
    records.sort(key=lambda item: (item["status"] != "pending_review", -int(item.get("risk_rank") or 0), item["record_id"]))

    if not records:
        st.info("No records available.")
        return

    def risk_color(value: str) -> str:
        colors = {"LOW": "#2f855a", "MEDIUM": "#b7791f", "HIGH": "#c53030", "CRITICAL": "#9b2c2c"}
        return colors.get(str(value or "").upper(), "#4a5568")

    def status_color(value: str) -> str:
        colors = {
            "pending_review": "#b7791f",
            "approved_for_execution": "#2b6cb0",
            "executed_sealed": "#2f855a",
            "rejected": "#c53030",
        }
        return colors.get(value, "#4a5568")

    def policy_color(value: str) -> str:
        if "CISO" in str(value or "").upper() or "LEGAL" in str(value or "").upper():
            return "#c53030"
        if "DUAL" in str(value or "").upper():
            return "#b7791f"
        return "#2b6cb0"

    if "agent_ops_selected_id" not in st.session_state:
        st.session_state["agent_ops_selected_id"] = records[0]["record_id"]

    pending_count = sum(1 for item in records if item["status"] == "pending_review")
    accepted_count = sum(1 for item in records if item["status"] in {"approved_for_execution", "executed_sealed"})
    rejected_count = sum(1 for item in records if item["status"] == "rejected")

    st.markdown(
        """
        <style>
        .agent-ops-header {display:grid;grid-template-columns:minmax(320px,1fr) minmax(320px,1fr);gap:14px;margin:0 0 18px 0;align-items:start;}
        .agent-ops-counter-row {display:flex;gap:8px;flex-wrap:nowrap;justify-content:flex-start;align-items:stretch;}
        .agent-ops-counter {flex:1;min-width:0;padding:8px 10px;border-radius:10px;background:#edf2f7;color:#1a202c;border:1px solid #e2e8f0;}
        .agent-ops-counter-label {font-size:0.66rem;opacity:0.9;text-transform:uppercase;letter-spacing:0.04em;font-family:'SFMono-Regular',Menlo,Consolas,monospace;}
        .agent-ops-counter-value {font-size:0.88rem;font-weight:700;line-height:1.2;margin-top:4px;font-family:'SFMono-Regular',Menlo,Consolas,monospace;}
        .agent-ops-status-row {display:flex;gap:8px;flex-wrap:nowrap;justify-content:flex-end;align-items:stretch;}
        .agent-ops-chip {flex:1;min-width:0;padding:8px 10px;border-radius:10px;color:#fff;}
        .agent-ops-chip-label {font-size:0.66rem;opacity:0.92;text-transform:uppercase;letter-spacing:0.04em;font-family:'SFMono-Regular',Menlo,Consolas,monospace;}
        .agent-ops-chip-value {font-size:0.8rem;font-weight:700;line-height:1.2;margin-top:4px;font-family:'SFMono-Regular',Menlo,Consolas,monospace;}
        </style>
        """,
        unsafe_allow_html=True,
    )

    current_selected_id = st.session_state.get("agent_ops_selected_id", records[0]["record_id"])
    header_container = st.container()
    row_top_left, row_top_right = st.columns([1.36, 1.0])
    with row_top_left:
        _render_panel_section_title("Records")
        table_rows = [
            {
                "OPEN": item["record_id"] == current_selected_id,
                "RECORD": item["record_id"],
                "AGENT": item["agent_name"],
                "RISK": item["risk_level"],
                "STATUS": _controlled_actions_status_label(item["status"]),
            }
            for item in records
        ]
        edited_rows = st.data_editor(
            pd.DataFrame(table_rows),
            use_container_width=True,
            hide_index=True,
            key=f"agent_ops_records_editor_{current_selected_id}",
            disabled=["RECORD", "AGENT", "RISK", "STATUS"],
            column_config={
                "OPEN": st.column_config.CheckboxColumn("OPEN", help="Select record", width="small"),
                "RECORD": st.column_config.TextColumn("RECORD", width="medium"),
                "AGENT": st.column_config.TextColumn("AGENT", width="medium"),
                "RISK": st.column_config.TextColumn("RISK", width="small"),
                "STATUS": st.column_config.TextColumn("STATUS", width="medium"),
            },
        )
        selected_indices = [idx for idx, row in edited_rows.iterrows() if bool(row.get("OPEN"))]
        multi_open_conflict = len(selected_indices) > 1
        if selected_indices:
            chosen_idx = selected_indices[-1]
            new_selected_id = records[int(chosen_idx)]["record_id"]
            if new_selected_id != current_selected_id and not multi_open_conflict:
                st.session_state["agent_ops_selected_id"] = new_selected_id
                st.rerun()

    current_selected_id = st.session_state.get("agent_ops_selected_id", records[0]["record_id"])
    selected = next((item for item in records if item["record_id"] == current_selected_id), records[0])

    with header_container:
        right_block = ""
        if not multi_open_conflict:
            right_block = f"""
              <div class="agent-ops-status-row">
                <div class="agent-ops-chip" style="background:{risk_color(selected['risk_level'])}">
                  <div class="agent-ops-chip-label">Risk Level</div>
                  <div class="agent-ops-chip-value">{selected['risk_level']}</div>
                </div>
                <div class="agent-ops-chip" style="background:{policy_color(selected['approval_policy'])}">
                  <div class="agent-ops-chip-label">Approval Policy</div>
                  <div class="agent-ops-chip-value">{selected['approval_policy']}</div>
                </div>
                <div class="agent-ops-chip" style="background:{status_color(selected['status'])}">
                  <div class="agent-ops-chip-label">Status</div>
                  <div class="agent-ops-chip-value">{_controlled_actions_status_label(selected['status'])}</div>
                </div>
              </div>
            """
        st.markdown(
            f"""
            <div class="agent-ops-header">
              <div class="agent-ops-counter-row">
                <div class="agent-ops-counter">
                  <div class="agent-ops-counter-label">Pending Review</div>
                  <div class="agent-ops-counter-value">{pending_count}</div>
                </div>
                <div class="agent-ops-counter">
                  <div class="agent-ops-counter-label">Accepted</div>
                  <div class="agent-ops-counter-value">{accepted_count}</div>
                </div>
                <div class="agent-ops-counter">
                  <div class="agent-ops-counter-label">Rejected</div>
                  <div class="agent-ops-counter-value">{rejected_count}</div>
                </div>
              </div>
              {right_block}
            </div>
            """,
            unsafe_allow_html=True,
        )
        if multi_open_conflict:
            st.caption("Leave only one OPEN row to view Risk Level, Approval Policy and Status.")

    with row_top_right:
        _render_panel_section_title("Proposed Operation")
        st.dataframe(
            [
                {"FIELD": "Record ID", "VALUE": selected["record_id"]},
                {"FIELD": "Submitted at", "VALUE": selected["submitted_at_utc"]},
                {"FIELD": "Agent", "VALUE": selected["agent_name"]},
                {"FIELD": "Operation type", "VALUE": selected["operation_type"]},
                {"FIELD": "Approval policy", "VALUE": selected["approval_policy"]},
                {"FIELD": "Operation", "VALUE": selected["intent"]},
                {"FIELD": "Tool", "VALUE": selected["tool_name"]},
                {"FIELD": "Review reason", "VALUE": selected["review_reason"]},
                {"FIELD": "Workflow ID", "VALUE": selected.get("workflow_id") or "-"},
                {"FIELD": "Agent role", "VALUE": selected.get("agent_role") or "-"},
            ],
            use_container_width=True,
            hide_index=True,
        )

    rationale_key = f"agent_ops_rationale_{selected['record_id']}"
    rationale = st.session_state.get(rationale_key, selected.get("decision_rationale") or "")
    signing_cfg = _load_aer_signing_config()
    package_payload = build_agent_operation_aer_package(selected, signing_cfg) if selected["status"] != "pending_review" else None

    mid_left, mid_center, mid_right = st.columns([1.0, 1.0, 1.0])
    with mid_left:
        _render_panel_section_title("Operation Parameters")
        st.dataframe(
            [{"PARAMETER": row["field"], "VALUE": row["value"], "TYPE": row["type"]} for row in selected["parameters"]],
            use_container_width=True,
            hide_index=True,
        )

    with mid_center:
        _render_panel_section_title("Human Authorization")
        st.dataframe(
            [
                {"CONTROL": "Decision", "STATE": selected["human_action"].replace("_", " ").title()},
                {"CONTROL": "Reviewer", "STATE": selected.get("reviewer_name") or "Pending reviewer"},
                {"CONTROL": "Reviewer role", "STATE": selected.get("reviewer_role") or "Pending reviewer role"},
                {"CONTROL": "Approval required", "STATE": "Yes" if selected.get("human_approval_required") else "No"},
            ],
            use_container_width=True,
            hide_index=True,
        )

    with mid_right:
        _render_panel_section_title("Execution Record")
        if package_payload is None:
            st.dataframe(
                [
                    {"FIELD": "AER ID", "VALUE": f"AER-{selected['record_id']}"},
                    {"FIELD": "Seal reference", "VALUE": "Pending decision"},
                    {"FIELD": "Manifest hash", "VALUE": "Pending generation"},
                    {"FIELD": "Root hash", "VALUE": "Pending generation"},
                    {"FIELD": "Delivery hash", "VALUE": "Pending generation"},
                    {"FIELD": "ZIP package", "VALUE": "Pending generation"},
                ],
                use_container_width=True,
                hide_index=True,
            )
        else:
            delivery_bundle_filename = package_payload.get("delivery_bundle_filename") or "Pending bundle"
            delivery_bundle_bytes = package_payload.get("delivery_bundle_bytes")
            st.dataframe(
                [
                    {"FIELD": "AER ID", "VALUE": package_payload["aer_id"]},
                    {"FIELD": "Delivery ID", "VALUE": package_payload["package_delivery_id"]},
                    {"FIELD": "Seal reference", "VALUE": selected["seal_reference"] or "-"},
                    {"FIELD": "Manifest hash", "VALUE": package_payload["manifest_hash"][:20] + "..."},
                    {"FIELD": "Root hash", "VALUE": package_payload["root_hash"][:20] + "..."},
                    {"FIELD": "Signature", "VALUE": "Signed" if any(row["artifact"] == "SIGNATURE.json" for row in package_payload["artifacts"]) else "Unsigned demo"},
                    {"FIELD": "Delivery hash", "VALUE": package_payload["zip_sha256"][:20] + "..."},
                    {"FIELD": "Delivery bundle", "VALUE": delivery_bundle_filename},
                    {"FIELD": "ZIP package", "VALUE": package_payload["zip_filename"]},
                ],
                use_container_width=True,
                hide_index=True,
            )

    rationale_key = f"agent_ops_rationale_{selected['record_id']}"
    current_rationale = st.session_state.get(rationale_key, selected.get("decision_rationale") or "")
    reviewer_name = st.session_state.get("auth_email") or "admin@hrevn.local"
    reviewer_role = "Administrator" if st.session_state.get("auth_role") == "admin" else "Operator"

    lower_left, lower_right = st.columns([2.0, 1.0])
    with lower_left:
        _render_panel_section_title("Artifacts")
        if package_payload is None:
            pending_artifacts = [
                "operation_record.json",
                "approval_record.json",
                "execution_record.json",
                "manifest.json",
                "CHECKSUMS.sha256",
                "ROOT_HASH_SHA256.txt",
                "agent_operation_review_report.pdf",
            ]
            st.dataframe(
                [
                    {"ARTIFACT": name, "STATE": "Pending", "DETAIL": "Will be generated after approval or rejection"}
                    for name in pending_artifacts
                ],
                use_container_width=True,
                hide_index=True,
            )
            st.caption("AER package generation starts after approval or rejection.")
        else:
            st.dataframe(
                [
                    {"ARTIFACT": row["artifact"], "SHA256": row["sha256"][:16] + "...", "SIZE": row["size_bytes"]}
                    for row in package_payload["artifacts"]
                ],
                use_container_width=True,
                hide_index=True,
            )
            st.download_button(
                "Export AER package (.zip)",
                data=package_payload["zip_bytes"],
                file_name=package_payload["zip_filename"],
                mime="application/zip",
                use_container_width=True,
            )
            st.download_button(
                "Export delivery seal (.sha256.txt)",
                data=package_payload["delivery_seal_bytes"],
                file_name=package_payload["delivery_seal_filename"],
                mime="text/plain",
                use_container_width=True,
            )
            if delivery_bundle_bytes:
                st.download_button(
                    "Export delivery bundle (.zip)",
                    data=delivery_bundle_bytes,
                    file_name=delivery_bundle_filename,
                    mime="application/zip",
                    use_container_width=True,
                )
            st.caption("Download the ZIP package and the delivery seal together as a matched pair. The sidecar is valid only for the exact ZIP filename it names.")

    with lower_right:
        _render_panel_section_title("Decision Rationale")
        st.text_area(
            "Decision rationale",
            value=current_rationale,
            key=rationale_key,
            height=210,
            label_visibility="collapsed",
            placeholder="Add a brief rationale for approval or rejection.",
        )
        if selected["status"] == "pending_review":
            if st.button("Authorize and execute", type="primary", use_container_width=True, key=f"approve_{selected['record_id']}"):
                set_agent_operation_decision(
                    AGENT_OPERATIONS_SQLITE_PATH,
                    selected["record_id"],
                    "approved",
                    reviewer_name=reviewer_name,
                    reviewer_role=reviewer_role,
                    rationale=st.session_state.get(rationale_key, ""),
                )
                st.rerun()
            if st.button("Reject", use_container_width=True, key=f"reject_{selected['record_id']}"):
                current_rationale = st.session_state.get(rationale_key, "")
                if not current_rationale.strip():
                    st.warning("Reject requires a short rationale.")
                else:
                    set_agent_operation_decision(
                        AGENT_OPERATIONS_SQLITE_PATH,
                        selected["record_id"],
                        "rejected",
                        reviewer_name=reviewer_name,
                        reviewer_role=reviewer_role,
                        rationale=current_rationale,
                    )
                    st.rerun()
        elif selected["status"] == "executed_sealed":
            st.success("Operation authorized, executed and sealed.")
        else:
            st.error("Operation rejected. Rejection record sealed.")

def render_real_estate_vertical() -> None:
    st.subheader("Real Estate Vertical")
    st.caption("Pilot workspace over the Real Estate SQLite snapshot, with improved operator view and legacy references.")

    if not REAL_ESTATE_SQLITE_PATH.exists():
        st.error("Real Estate SQLite snapshot not available.")
        return

    cfg = load_common_config()
    snapshot = load_real_estate_snapshot(REAL_ESTATE_SQLITE_PATH)
    visit_ids = [item.get("visit_id") for item in snapshot.visits if isinstance(item, dict) and item.get("visit_id")]
    if not visit_ids:
        st.warning("No visits available in the Real Estate snapshot.")
        return

    selected_visit = st.selectbox("Selected visit", options=visit_ids)
    context = _prepare_real_estate_context(snapshot, selected_visit)
    workspace = build_real_estate_workspace(snapshot, selected_visit)
    readiness = _build_real_estate_readiness(context, workspace)

    tab_overview, tab_workspace, tab_a, tab_b, tab_c = st.tabs(
        [
            "Overview",
            "Workspace",
            "Legacy A",
            "Legacy B",
            "Legacy C",
        ]
    )

    with tab_overview:
        _render_real_estate_overview(snapshot, context, readiness)
    with tab_workspace:
        _render_real_estate_workspace(context, workspace, readiness, cfg)
    with tab_a:
        _render_legacy_panel_a(context)
    with tab_b:
        _render_legacy_panel_b(context)
    with tab_c:
        _render_legacy_panel_c(context)


def render_schema_explorer() -> None:
    st.subheader("Schema Explorer")
    st.caption("Read-only browsing of schema artifacts inside the sandbox.")

    schema_files = _list_files(
        SCHEMA_DIR,
        (
            "*.md",
            "*.yaml",
            "*.yml",
            "*.json",
            "*.sql",
        ),
    )

    if not schema_files:
        st.warning("No schema files found.")
        return

    selected = st.selectbox(
        "Select schema artifact",
        options=schema_files,
        format_func=lambda p: p.name,
    )

    st.write(f"Path: `{selected}`")
    content = _safe_read(selected)

    if selected.suffix.lower() == ".json":
        try:
            parsed = json.loads(content)
            st.json(parsed)
            return
        except Exception:
            pass

    st.code(content, language="text")


def render_mapping_validator() -> None:
    st.subheader("Mapping Validator UI")
    st.caption("Lightweight structural checks for mapping files. No source DB access.")

    mapping_files = _list_files(MAPPINGS_DIR, ("*.yaml", "*.yml"))

    if not mapping_files:
        st.warning("No mapping files found.")
        return

    run_all = st.toggle("Validate all mapping files", value=True)

    targets = mapping_files
    if not run_all:
        one = st.selectbox(
            "Select mapping file",
            options=mapping_files,
            format_func=lambda p: p.name,
        )
        targets = [one]

    results = [_validate_mapping_file(path) for path in targets]

    summary = {
        "files_validated": len(results),
        "valid": sum(1 for r in results if r.ok),
        "invalid": sum(1 for r in results if not r.ok),
    }
    st.metric("Validated files", summary["files_validated"])
    st.write(summary)

    for r in results:
        with st.expander(f"{r.file_name} — {'OK' if r.ok else 'CHECK'}", expanded=not r.ok):
            st.write("Checks:")
            st.json(r.checks)
            if r.notes:
                st.write("Notes:")
                for note in r.notes:
                    st.write(f"- {note}")


def render_dry_run_dashboard() -> None:
    st.subheader("Dry-Run Convergence Dashboard")
    st.caption("High-level structural view of what is already built inside the sandbox. This is not an operational dashboard and it does not show live business data.")

    rows = []
    for name, path in _directory_snapshot():
        stats = _stats_for(path)
        rows.append(
            {
                "Area": name,
                "Total files": stats["files"],
                "Docs (.md)": stats["md"],
                "Mappings (.yaml/.yml)": stats["yaml_yml"],
                "Data contracts (.json)": stats["json"],
                "Utilities (.py)": stats["py"],
            }
        )

    st.markdown("### What this table means")
    st.write(
        "This table is a structural inventory of the sandbox. It tells us how much documentary, mapping and utility material exists in each internal area. It does not represent certificates issued, visits completed or any live operational KPI."
    )

    st.dataframe(rows, use_container_width=True, hide_index=True)

    st.markdown("### How to read it")
    st.dataframe(
        [
            {"Column": "Area", "Meaning": "Functional zone inside the sandbox such as docs, samples, schema or importers."},
            {"Column": "Total files", "Meaning": "How many files exist in that area overall."},
            {"Column": "Docs (.md)", "Meaning": "Narrative or architectural documentation files."},
            {"Column": "Mappings (.yaml/.yml)", "Meaning": "Mapping or interface definition files used for convergence work."},
            {"Column": "Data contracts (.json)", "Meaning": "JSON schemas, examples or contract artifacts stored in that area."},
            {"Column": "Utilities (.py)", "Meaning": "Python helpers, importers or support scripts present in that area."},
        ],
        use_container_width=True,
        hide_index=True,
    )

    st.info(
        "If a row looks sparse or empty, it usually means that area is not being used heavily yet or that its material lives in another family of documents."
    )


def main() -> None:
    st.set_page_config(page_title="HREVN Sandbox — Documentary Panels", layout="wide", initial_sidebar_state="collapsed")
    _render_global_table_style()
    _render_auth_shell()

    head_left, head_right = st.columns([0.84, 0.16])
    with head_left:
        st.title("HREVN UNIFIED V1 SANDBOX — Streamlit Panels")
        st.caption(
            "Documentary-only UI. No real source access, no SQLite writes, no real data reads outside the bundled sandbox snapshot."
        )
    with head_right:
        st.write("")
        st.write("")
        if st.button("Log out", use_container_width=True):
            _logout()
            st.rerun()

    target = st.session_state.get("main_tab_target")

    if target == "agent_operations":
        top_left, top_right = st.columns([0.86, 0.14])
        with top_left:
            st.subheader("Agent Operations")
        with top_right:
            if st.button("Back to all panels", use_container_width=True):
                st.session_state.pop("main_tab_target", None)
                st.rerun()
        render_controlled_actions_vertical()
        return

    if target == "central_console":
        top_left, top_right = st.columns([0.86, 0.14])
        with top_left:
            st.subheader("Central Console")
        with top_right:
            if st.button("Back to all panels", use_container_width=True):
                st.session_state.pop("main_tab_target", None)
                st.rerun()
        render_central_console()
        return

    if target == "access_security":
        top_left, top_right = st.columns([0.86, 0.14])
        with top_left:
            st.subheader("Access & Security")
        with top_right:
            if st.button("Back to all panels", use_container_width=True):
                st.session_state.pop("main_tab_target", None)
                st.rerun()
        render_access_security_panel()
        return

    tab_re, tab_actions, tab_schema, tab_mapping, tab_dryrun = st.tabs(
        [
            "Real Estate Vertical",
            "Agent Operations",
            "Schema Explorer",
            "Mapping Validator UI",
            "Dry-Run Convergence Dashboard",
        ]
    )

    with tab_re:
        render_real_estate_vertical()
    with tab_actions:
        render_controlled_actions_vertical()
    with tab_schema:
        render_schema_explorer()
    with tab_mapping:
        render_mapping_validator()
    with tab_dryrun:
        render_dry_run_dashboard()


if __name__ == "__main__":
    main()
