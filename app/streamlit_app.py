"""Streamlit app for documentary-only sandbox exploration.

No real access, no external source systems, documentary-safe views only.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from urllib.parse import quote_plus
from datetime import datetime, timezone
import pandas as pd
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import streamlit as st
import streamlit.components.v1 as components
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
    resolve_ip_locality,
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
    build_real_estate_end_to_end_preview,
    build_real_estate_workspace,
    load_real_estate_snapshot,
)
from common.services.real_estate_ai_review import review_real_estate_certification
from common.services.rwa_v1_schema import ensure_rwa_v1_schema
from common.services import rwa_v1_store as rwa_store

from common.services.real_estate_v2_store import (
    create_re_v2_account,
    create_re_v2_account_asset_link,
    create_re_v2_asset,
    create_re_v2_enterprise,
    get_re_v2_enterprise_assignment_detail,
    get_re_v2_summary,
    list_re_v2_accounts,
    list_re_v2_asset_demands_rows,
    list_re_v2_assets,
    list_re_v2_assets_for_enterprise,
    list_re_v2_enterprises,
    list_re_v2_observations_raw,
    list_re_v2_photos_raw,
    list_re_v2_visits,
    list_re_v2_visits_raw,
    reset_and_seed_re_v2_demo,
)
from common.services.communications_store import (
    ensure_communications_schema,
    load_communications_snapshot,
    get_latest_sync_run,
    sync_gmail_inbox,
)
from common.services.telegram_connector import (
    get_telegram_connector_status,
    send_controlled_test_message,
)
from common.tools.secret_hygiene_scan import run_secret_hygiene_scan


RWA_ASSET_CATEGORIES = rwa_store.RWA_ASSET_CATEGORIES
attach_rwa_v1_files_to_visit = rwa_store.attach_rwa_v1_files_to_visit
create_rwa_v1_observation = rwa_store.create_rwa_v1_observation
create_rwa_v1_visit = rwa_store.create_rwa_v1_visit
ensure_rwa_v1_demo_seed = rwa_store.ensure_rwa_v1_demo_seed
finalize_rwa_v1_capture_session = rwa_store.finalize_rwa_v1_capture_session
list_rwa_v1_assets = rwa_store.list_rwa_v1_assets
list_rwa_v1_attachments_raw = rwa_store.list_rwa_v1_attachments_raw
list_rwa_v1_observations_raw = rwa_store.list_rwa_v1_observations_raw
list_rwa_v1_photos_raw = rwa_store.list_rwa_v1_photos_raw
list_rwa_v1_visits_raw = rwa_store.list_rwa_v1_visits_raw
remove_rwa_v1_review_artifact = rwa_store.remove_rwa_v1_review_artifact
replace_rwa_v1_review_artifact = rwa_store.replace_rwa_v1_review_artifact
refresh_rwa_v1_capture_session = rwa_store.refresh_rwa_v1_capture_session
validate_and_issue_rwa_v1_visit = rwa_store.validate_and_issue_rwa_v1_visit


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
COMMUNICATIONS_SQLITE_PATH = APP_DATA_DIR / "communications" / "hrevn_communications.db"

LEGACY_REAL_ESTATE_ROOT = Path("/Users/miguelmiguel/CODEX/HREVN CODEX REAL ESTATE")
LEGACY_GOV_ROOT = Path("/Users/miguelmiguel/CODEX/HREVN CODEX GOV")
LEGACY_PTDG_ROOT = Path("/Users/miguelmiguel/CODEX/Physical-to-Digital Gap")


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _utc_now_datetime() -> datetime:
    return datetime.now(timezone.utc)


def _render_console_table_html(df: pd.DataFrame, *, total_row_index: int | None = None) -> None:
    styled = df.style.set_table_styles([
        {"selector": "table", "props": [("width", "100%"), ("border-collapse", "collapse"), ("table-layout", "fixed"), ("font-family", "Menlo, Monaco, monospace"), ("font-size", "12px")]},
        {"selector": "th", "props": [("background-color", "#e8edf2"), ("color", "#0f172a"), ("font-size", "11px"), ("font-weight", "700"), ("text-transform", "uppercase"), ("letter-spacing", "0.08em"), ("padding", "8px 10px"), ("border", "1px solid #d8e1e8"), ("text-align", "left")]},
        {"selector": "td", "props": [("padding", "8px 10px"), ("border", "1px solid #d8e1e8"), ("color", "#0f172a"), ("background-color", "#ffffff")]},
    ])
    if "VERTICAL" in df.columns:
        styled = styled.set_properties(subset=["VERTICAL"], **{"background-color": "#edf2f7", "font-weight": "700", "text-transform": "uppercase", "width": "140px"})
    if "LINE" in df.columns:
        styled = styled.set_properties(subset=["LINE"], **{"width": "220px"})
    if total_row_index is not None:
        styled = styled.apply(lambda row: ["background-color:#dbe7f0;font-weight:700;" if row.name == total_row_index else "" for _ in row], axis=1)
    html = styled.hide(axis="index").to_html()
    st.markdown(html, unsafe_allow_html=True)


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


_I18N = {
    "en": {
        "admin_space": "Admin space",
        "central_console": "Central Console",
        "access_security": "Access & Security",
        "verticals": "Verticals",
        "communications": "Communications",
        "real_estate": "Real Estate",
        "gov_photovoltaic": "GOV / Photovoltaic",
        "graphic_evidence": "Legal Evidence",
        "rwa": "RWA",
        "genius_operations": "GENIUS Operations",
        "agent_operations": "Agent Operations",
        "email": "Email",
        "telegram": "Telegram",
        "access_shell_title": "HREVN Unified V1 — Access Shell",
        "access_shell_caption": "Documentary-safe access shell for the unified pilot. No real source access is enabled here.",
        "interface_language": "Interface language",
        "tab_login": "Login",
        "tab_register": "Register",
        "tab_verify": "Verify Email",
        "tab_recovery": "Password Recovery",
        "field_email": "Email",
        "field_password": "Password",
        "field_recovery_email": "Recovery email (optional)",
        "field_preferred_language": "Preferred language",
        "field_confirm_password": "Confirm password",
        "button_access_workspace": "Access workspace",
        "button_demo_mode": "Continue in documentary demo mode",
        "button_create_account": "Create account",
        "button_verify_account": "Verify account",
        "field_verification_token": "Verification token",
        "production": "Production",
        "technical_architecture": "Technical Architecture",
    },
    "es": {
        "admin_space": "Espacio admin",
        "central_console": "Consola central",
        "access_security": "Acceso y seguridad",
        "verticals": "Verticales",
        "communications": "Comunicaciones",
        "real_estate": "Real Estate",
        "gov_photovoltaic": "GOV / Fotovoltaica",
        "graphic_evidence": "Evidencia legal",
        "genius_operations": "Operaciones GENIUS",
        "agent_operations": "Operaciones de agentes",
        "email": "Correo",
        "telegram": "Telegram",
        "access_shell_title": "HREVN Unified V1 — Acceso",
        "access_shell_caption": "Capa de acceso segura para el piloto unificado. No hay acceso real a sistemas fuente en esta vista.",
        "interface_language": "Idioma de la interfaz",
        "tab_login": "Login",
        "tab_register": "Registro",
        "tab_verify": "Verificar email",
        "tab_recovery": "Recuperar contraseña",
        "field_email": "Correo electrónico",
        "field_password": "Contraseña",
        "field_recovery_email": "Correo de recuperación (opcional)",
        "field_preferred_language": "Idioma preferido",
        "field_confirm_password": "Confirmar contraseña",
        "button_access_workspace": "Entrar al workspace",
        "button_demo_mode": "Continuar en modo demo documental",
        "button_create_account": "Crear cuenta",
        "button_verify_account": "Verificar cuenta",
        "field_verification_token": "Token de verificación",
        "production": "Producción",
        "technical_architecture": "Arquitectura técnica",
    },
}


def _lang() -> str:
    current = str(st.session_state.get("auth_language") or _secret_value("SANDBOX_DEFAULT_LANGUAGE", "en") or "en").strip().lower()
    return current if current in _I18N else "en"


def _t(key: str) -> str:
    lang = _lang()
    return _I18N.get(lang, _I18N["en"]).get(key, _I18N["en"].get(key, key))



def _openai_api_key_for(scope: str = "production") -> str:
    scope = (scope or "production").strip().lower()
    if scope == "demo":
        return _secret_value("OPENAI_API_KEY_DEMO", "")
    if scope in {"communications", "comms"}:
        return _secret_value("OPENAI_API_KEY_COMMS", "")
    return (
        _secret_value("OPENAI_API_KEY_PRODUCTION", "")
        or _secret_value("OPENAI_API_KEY_PROD", "")
        or _secret_value("OPENAI_API_KEY", "")
    )


def _real_estate_delivery_target_email(cfg) -> str:
    return (
        st.session_state.get("auth_email", "")
        or cfg.notify_email
        or _secret_value("SANDBOX_SECURITY_ALERT_EMAIL", "")
        or _secret_value("SANDBOX_ADMIN_EMAIL", "")
    ).strip()


def _is_admin_secret_email(cfg: AuthShellConfig, email: str) -> bool:
    candidate = (email or "").strip().lower()
    return bool(cfg.admin_email and candidate and candidate == cfg.admin_email.strip().lower())


def _reset_real_estate_v2_account_form() -> None:
    st.session_state["re_v2_account_form_nonce"] = int(st.session_state.get("re_v2_account_form_nonce", 0) or 0) + 1


def _send_real_estate_delivery_email(*, target_email: str, subject: str, body: str) -> dict[str, str]:
    smtp_enabled = _secret_bool("SMTP_ENABLED", False)
    smtp_host = _secret_value("SMTP_HOST", "")
    smtp_port = _secret_int("SMTP_PORT", 587)
    smtp_user = _secret_value("SMTP_USER", "")
    smtp_pass = _secret_value("SMTP_PASS", "")
    mail_from = _secret_value("MAIL_FROM", "") or _secret_value("SANDBOX_RECOVERY_NOTIFY_EMAIL", "")

    if not target_email:
        return {"delivery_status": "missing_target", "delivery_channel": "none"}
    if not (smtp_enabled and smtp_host and smtp_user and smtp_pass and mail_from):
        return {"delivery_status": "not_configured", "delivery_channel": "none"}

    result = send_smtp_notification(
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_pass=smtp_pass,
        mail_from=mail_from,
        target_email=target_email,
        subject=subject,
        body=body,
        html_body=html_body,
    )
    payload = {
        "delivery_status": result.delivery_status,
        "delivery_channel": result.delivery_channel,
        "target_email": result.target_email,
        "error_detail": result.error_detail or "",
    }
    if payload["delivery_status"] != "sent":
        _send_telegram_security_alert(
            "email_delivery_failed",
            (
                "H-REVN operations alert: certification email delivery failed.\n"
                f"Target: {target_email}\n"
                f"Subject: {subject}\n"
                f"Status: {payload['delivery_status']}\n"
                f"Detail: {payload['error_detail'] or 'none'}"
            ),
        )
    return payload


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
            preferred_language=_secret_value("SANDBOX_DEFAULT_LANGUAGE", "en"),
        )
    if cfg.user_email:
        upsert_auth_account(
            AUTH_ACCESS_SQLITE_PATH,
            user_email=cfg.user_email.strip().lower(),
            user_role="operator",
            account_source="streamlit_secrets",
            preferred_language=_secret_value("SANDBOX_DEFAULT_LANGUAGE", "en"),
        )
    upsert_auth_account(
        AUTH_ACCESS_SQLITE_PATH,
        user_email="demo@hrevn.local",
        user_role="demo",
        account_source="built_in_demo",
        preferred_language=_secret_value("SANDBOX_DEFAULT_LANGUAGE", "en"),
    )


def _record_access_outbound_email(
    *,
    related_user_email: str | None,
    event_type: str,
    target_email: str | None,
    subject: str,
    body: str,
    delivery_channel: str,
    delivery_status: str,
    from_email: str | None,
) -> None:
    ensure_communications_schema(COMMUNICATIONS_SQLITE_PATH)
    with sqlite3.connect(str(COMMUNICATIONS_SQLITE_PATH)) as conn:
        conn.execute(
            "INSERT INTO comm_outbound_emails(related_entity_type,related_entity_id,to_email,subject,body_text,delivery_channel,delivery_status,provider_message_id,source_thread_id,from_email,from_name,sent_at_utc) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "auth_notification",
                event_type[:120] if event_type else None,
                (target_email or None),
                (subject[:500] if subject else None),
                (body[:12000] if body else None),
                delivery_channel[:40] if delivery_channel else "none",
                delivery_status[:40] if delivery_status else "queued",
                None,
                None,
                (from_email or None),
                "H-REVN",
                datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z') if delivery_status == "sent" else None,
            ),
        )


def _send_access_notification(
    *,
    related_user_email: str | None,
    target_email: str | None,
    event_type: str,
    subject: str,
    body: str,
    html_body: str | None = None,
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
        _record_access_outbound_email(
            related_user_email=related_user_email,
            event_type=event_type,
            target_email=target_email,
            subject=subject,
            body=body,
            delivery_channel="none",
            delivery_status="not_configured",
            from_email=None,
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
        _record_access_outbound_email(
            related_user_email=related_user_email,
            event_type=event_type,
            target_email=target_email,
            subject=subject,
            body=body,
            delivery_channel="none",
            delivery_status="not_configured",
            from_email=mail_from or None,
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
        html_body=html_body,
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
    _record_access_outbound_email(
        related_user_email=related_user_email,
        event_type=event_type,
        target_email=result.target_email,
        subject=result.subject,
        body=body,
        delivery_channel=result.delivery_channel,
        delivery_status=result.delivery_status,
        from_email=mail_from or None,
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


def _ip_locality_label(ip_public: str) -> str:
    locality = resolve_ip_locality(AUTH_ACCESS_SQLITE_PATH, ip_public=ip_public or "unknown")
    label = str(locality.get("locality_label") or "").strip()
    return label or "unknown"




def _auth_app_base_url() -> str:
    explicit = _secret_value("SANDBOX_PUBLIC_BASE_URL", "").strip()
    if explicit:
        return explicit.rstrip("/")
    try:
        headers = dict(getattr(st.context, "headers", {}) or {})
    except Exception:
        headers = {}
    origin = str(headers.get("Origin") or headers.get("origin") or "").strip()
    if origin:
        return origin.rstrip("/")
    host = str(headers.get("Host") or headers.get("host") or "").strip()
    proto = str(headers.get("X-Forwarded-Proto") or headers.get("x-forwarded-proto") or "https").strip() or "https"
    if host:
        return f"{proto}://{host}".rstrip("/")
    return "https://hrevn-unified-sandbox-cloud-jxjaxxv5kw3zjehm6pgjjj.streamlit.app"


def _build_verify_email_link(user_email: str, token: str) -> str:
    return f"{_auth_app_base_url()}/?verify_email={quote_plus(user_email)}&verify_token={quote_plus(token)}"


def _build_welcome_verify_email_text(*, user_email: str, verify_link: str) -> str:
    return (
        "Welcome to H-REVN\n\n"
        "Your account has been successfully created.\n\n"
        "Verify your email to activate the account:\n"
        f"{verify_link}\n\n"
        f"Access the platform: {_auth_app_base_url()}\n\n"
        "If you did not create this account, please ignore this message.\n\n"
        "H-REVN Protocol"
    )


def _build_welcome_verify_email_html(*, user_email: str, verify_link: str) -> str:
    login_link = _auth_app_base_url()
    return f"""<!doctype html>
<html>
  <head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Welcome to H-REVN</title></head>
  <body style="margin:0;padding:0;background:#f4f6f8;font-family:Arial,Helvetica,sans-serif;color:#102a43">
    <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="background:#f4f6f8;padding:24px 12px">
      <tr><td align="center">
        <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="max-width:600px;background:#ffffff;border-radius:14px;border:1px solid #d9e2ec;overflow:hidden">
          <tr><td style="padding:28px 24px 8px 24px"><h1 style="margin:0;font-size:28px;line-height:1.2;color:#102a43">Welcome to H-REVN</h1></td></tr>
          <tr><td style="padding:12px 24px 0 24px;font-size:16px;line-height:1.6;color:#334e68">Your account has been successfully created.</td></tr>
          <tr><td style="padding:18px 24px 0 24px"><table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="background:#f7fafc;border:1px solid #d9e2ec;border-radius:10px"><tr><td style="padding:12px 14px;font-size:14px;line-height:1.6;color:#243b53"><strong>Email:</strong> {user_email}</td></tr></table></td></tr>
          <tr><td style="padding:22px 24px 6px 24px;text-align:center"><a href="{verify_link}" style="display:inline-block;background:#0b6e4f;color:#ffffff;text-decoration:none;font-weight:700;font-size:15px;padding:12px 22px;border-radius:8px">Verify Email</a></td></tr>
          <tr><td style="padding:8px 24px 0 24px;font-size:13px;line-height:1.6;color:#627d98;text-align:center">After verification, access the platform here: <a href="{login_link}" style="color:#0b6e4f;text-decoration:underline">Access the Platform</a></td></tr>
          <tr><td style="padding:16px 24px 0 24px;font-size:13px;line-height:1.6;color:#7b8794">If you did not create this account, please ignore this message.</td></tr>
          <tr><td style="padding:20px 24px 24px 24px;font-size:11px;line-height:1.5;color:#9aa5b1;text-align:center">H-REVN Protocol</td></tr>
        </table>
      </td></tr>
    </table>
  </body>
</html>"""


def _issue_or_refresh_verification_token(user_email: str) -> str | None:
    user_email = (user_email or "").strip().lower()
    if not user_email:
        return None
    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
    token = str(uuid.uuid4())
    with sqlite3.connect(AUTH_ACCESS_SQLITE_PATH) as conn:
        row = conn.execute("SELECT email_verified_flag FROM auth_local_credentials WHERE lower(user_email)=lower(?)", (user_email,)).fetchone()
        if row is None or int(row[0] or 0) == 1:
            return None
        conn.execute("UPDATE auth_local_credentials SET verification_token=?, verification_token_expires_at_utc=?, updated_at_utc=? WHERE lower(user_email)=lower(?)", (token, now_iso, now_iso, user_email))
    return token


def _handle_email_verification_link() -> None:
    params = st.query_params
    email_param = str(params.get("verify_email") or "").strip()
    token_param = str(params.get("verify_token") or "").strip()
    if not email_param or not token_param:
        return
    handled_key = f"verify-link:{email_param}:{token_param}"
    if st.session_state.get("_handled_verify_link") == handled_key:
        return
    context = _get_auth_request_context()
    verified = verify_email_token(AUTH_ACCESS_SQLITE_PATH, user_email=email_param, token=token_param, context=context)
    if verified:
        log_auth_event(AUTH_ACCESS_SQLITE_PATH, user_email=email_param or None, user_role="operator", identifier_attempted=email_param or None, event_type="email_verified", success_flag=True, failure_reason=None, context=context)
        st.success("Email verified. The account is now active.")
    else:
        log_auth_event(AUTH_ACCESS_SQLITE_PATH, user_email=email_param or None, user_role="operator", identifier_attempted=email_param or None, event_type="email_verification_failure", success_flag=False, failure_reason="invalid_verification_link", context=context)
        st.error("Verification link is invalid or expired.")
    st.session_state["_handled_verify_link"] = handled_key
    try:
        st.query_params.clear()
    except Exception:
        pass

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
    st.session_state.setdefault("auth_language", _secret_value("SANDBOX_DEFAULT_LANGUAGE", "en") or "en")
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


def _latest_notification_timestamp(*event_types: str) -> str:
    snapshot = get_recent_auth_snapshot(AUTH_ACCESS_SQLITE_PATH, limit=200)
    for row in snapshot.get("notifications", []):
        if str(row.get("event_type") or "") in event_types:
            return str(row.get("created_at_utc") or "")
    return ""


def _should_emit_recovery_alert(failure_event: str, recovery_event: str) -> bool:
    latest_failure = _latest_notification_timestamp(failure_event)
    if not latest_failure:
        return False
    latest_recovery = _latest_notification_timestamp(recovery_event)
    return not latest_recovery or latest_recovery < latest_failure


def _send_admin_security_email_alert(event_type: str, subject: str, body: str) -> None:
    target_email = (
        _secret_value("SANDBOX_SECURITY_ALERT_EMAIL", "")
        or _secret_value("SANDBOX_RECOVERY_NOTIFY_EMAIL", "")
        or _secret_value("SANDBOX_ADMIN_EMAIL", "")
    ).strip()
    if not target_email:
        log_auth_notification_event(
            AUTH_ACCESS_SQLITE_PATH,
            related_user_email=None,
            target_email=target_email or None,
            event_type=event_type,
            delivery_channel="email_admin",
            delivery_status="not_configured",
            subject=subject,
            error_detail=None,
            details_json=json.dumps({"reason": "missing_admin_security_target", "body": body}),
        )
        return
    _send_access_notification(
        related_user_email=None,
        target_email=target_email,
        event_type=event_type,
        subject=subject,
        body=body,
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
    st.session_state["auth_language"] = _secret_value("SANDBOX_DEFAULT_LANGUAGE", "en") or "en"


def _render_auth_shell() -> None:
    cfg = _load_auth_shell_config()
    _sync_auth_accounts(cfg)
    _init_auth_state()
    _handle_email_verification_link()

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
            st.sidebar.markdown(f"### {_t("admin_space")}")
            if st.sidebar.button(_t("central_console"), use_container_width=True):
                st.session_state["main_tab_target"] = "central_console"
                st.rerun()
            if st.sidebar.button(_t("access_security"), use_container_width=True):
                st.session_state["main_tab_target"] = "access_security"
                st.rerun()
            st.sidebar.markdown(f"#### {_t("verticals")}")
            human_verticals = st.sidebar.container(border=True)
            with human_verticals:
                st.markdown("<div style='background:#dbeafe;color:#0f172a;border-radius:10px;padding:8px 10px;margin-bottom:8px;font-size:12px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase'>Human verticals</div>", unsafe_allow_html=True)
                if st.button(_t("real_estate"), use_container_width=True, key="sidebar_real_estate"):
                    st.session_state["main_tab_target"] = "real_estate"
                    st.rerun()
                if st.button(_t("gov_photovoltaic"), use_container_width=True, key="sidebar_gov"):
                    st.session_state["main_tab_target"] = "gov_photovoltaic"
                    st.rerun()
                if st.button(_t("graphic_evidence"), use_container_width=True, key="sidebar_graphic"):
                    st.session_state["main_tab_target"] = "graphic_evidence"
                    st.rerun()
                if st.button(_t("rwa"), use_container_width=True, key="sidebar_rwa"):
                    st.session_state["main_tab_target"] = "rwa"
                    st.rerun()
            system_verticals = st.sidebar.container(border=True)
            with system_verticals:
                st.markdown("<div style='background:#ede9fe;color:#1f2937;border-radius:10px;padding:8px 10px;margin-bottom:8px;font-size:12px;font-weight:700;letter-spacing:0.06em;text-transform:uppercase'>System verticals</div>", unsafe_allow_html=True)
                if st.button(_t("genius_operations"), use_container_width=True, key="sidebar_genius"):
                    st.session_state["main_tab_target"] = "genius_operations"
                    st.rerun()
                if st.button(_t("agent_operations"), use_container_width=True, key="sidebar_agent_ops"):
                    st.session_state["main_tab_target"] = "agent_operations"
                    st.rerun()
            st.sidebar.markdown(f"#### {_t("communications")}")
            if st.sidebar.button(_t("email"), use_container_width=True):
                st.session_state["main_tab_target"] = "email"
                st.rerun()
            if st.sidebar.button(_t("telegram"), use_container_width=True):
                st.session_state["main_tab_target"] = "telegram"
                st.rerun()
        return

    language_left, _language_spacer = st.columns([0.4, 0.6])
    with language_left:
        current_language = _lang()
        selected_ui_language = st.selectbox(
            _t("interface_language"),
            options=["en", "es"],
            index=0 if current_language == "en" else 1,
            format_func=lambda value: {"en": "English", "es": "Español"}.get(value, value),
            key="access_shell_language_selector",
        )
        if selected_ui_language != current_language:
            st.session_state["auth_language"] = selected_ui_language
            st.rerun()

    st.title(_t("access_shell_title"))
    st.caption(_t("access_shell_caption"))

    login_tab, signup_tab, verify_tab, recovery_tab = st.tabs([_t("tab_login"), _t("tab_register"), _t("tab_verify"), _t("tab_recovery")])

    with login_tab:
        left, right = st.columns([1.1, 0.9])
        with left:
            email = st.text_input(_t("field_email"), key="login_email")
            password = st.text_input(_t("field_password"), type="password", key="login_password")
            if st.button(_t("button_access_workspace"), type="primary"):
                context = _get_auth_request_context()
                is_admin_attempt = _is_admin_secret_email(cfg, email)
                ip_record = None if is_admin_attempt else get_ip_control_record(AUTH_ACCESS_SQLITE_PATH, context.ip_public)
                if ip_record and ip_is_blocked(ip_record):
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
                if ip_record and ip_is_in_cooldown(ip_record):
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
                    if (not _is_admin_secret_email(cfg, matched[1])) and is_account_temporarily_locked(account_record):
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
                        if (not _is_admin_secret_email(cfg, matched[1])) and current_active_sessions >= max_sessions:
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
                        if not _is_admin_secret_email(cfg, matched[1]):
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
                        selected_language = str((account_record or {}).get("preferred_language") or _secret_value("SANDBOX_DEFAULT_LANGUAGE", "en") or "en").lower()
                        st.session_state["auth_logged_in"] = True
                        st.session_state["auth_role"] = matched[0]
                        st.session_state["auth_email"] = matched[1]
                        st.session_state["auth_session_id"] = session_id
                        st.session_state["auth_language"] = selected_language
                        st.success("Access granted.")
                        st.rerun()
                elif cfg.auth_enabled and cfg.has_configured_accounts:
                    if is_admin_attempt:
                        log_auth_event(
                            AUTH_ACCESS_SQLITE_PATH,
                            user_email=cfg.admin_email.strip().lower() if cfg.admin_email else None,
                            user_role="admin",
                            identifier_attempted=email.strip().lower() or None,
                            event_type="login_failure",
                            success_flag=False,
                            failure_reason="invalid_admin_credentials",
                            context=context,
                        )
                        st.error("Invalid administrator credentials.")
                        st.stop()
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
                            _send_admin_security_email_alert(
                                "user_lockout",
                                "H-REVN security alert: user lockout",
                                f"A temporary user lockout was triggered for {email.strip().lower()} from IP {context.ip_public}.\n\nEvent: user_lockout",
                            )
                    else:
                        failure_reason = "invalid_credentials_or_password"
                    if ip_is_blocked(ip_result):
                        failure_reason = "ip_blocked"
                        _send_telegram_security_alert(
                            "ip_blocked",
                            f"H-REVN security alert: IP blocked after repeated failed attempts. IP={context.ip_public}.",
                        )
                        _send_admin_security_email_alert(
                            "ip_blocked",
                            "H-REVN security alert: IP blocked",
                            f"An IP address has been blocked after repeated failed attempts.\n\nIP: {context.ip_public}\nEvent: ip_blocked",
                        )
                    elif ip_is_in_cooldown(ip_result):
                        failure_reason = "ip_cooldown"
                        _send_telegram_security_alert(
                            "ip_cooldown",
                            f"H-REVN security alert: IP cooldown triggered. IP={context.ip_public}.",
                        )
                        _send_admin_security_email_alert(
                            "ip_cooldown",
                            "H-REVN security alert: IP cooldown",
                            f"An IP address entered cooldown after repeated failed attempts.\n\nIP: {context.ip_public}\nEvent: ip_cooldown",
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
                if st.button(_t("button_demo_mode")):
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
                        st.session_state["auth_language"] = _secret_value("SANDBOX_DEFAULT_LANGUAGE", "en") or "en"
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
            register_email = st.text_input(_t("field_email"), key="register_email")
            register_recovery = st.text_input(_t("field_recovery_email"), key="register_recovery_email")
            register_language = st.selectbox(
                _t("field_preferred_language"),
                options=["en", "es"],
                format_func=lambda value: {"en": "English", "es": "Español"}.get(value, value),
                key="register_preferred_language",
            )
            register_password = st.text_input(_t("field_password"), type="password", key="register_password")
            register_password_2 = st.text_input(_t("field_confirm_password"), type="password", key="register_password_confirm")
            if st.button(_t("button_create_account"), type="primary"):
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
                            preferred_language=register_language,
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
                        verify_link = _build_verify_email_link(email_value, verification_token)
                        _send_access_notification(
                            related_user_email=email_value,
                            target_email=email_value,
                            event_type="verification_email_sent",
                            subject="Verify your H-REVN access account",
                            body=_build_welcome_verify_email_text(user_email=email_value, verify_link=verify_link),
                            html_body=_build_welcome_verify_email_html(user_email=email_value, verify_link=verify_link),
                        )
                        st.success("Account created. Check your email to verify the account.")
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
                    {"Rule": "Language", "Requirement": "preferred language is saved to the account profile"},
                    {"Rule": "Audit trail", "Requirement": "signup, verification, IP and session events are recorded"},
                ],
                use_container_width=True,
                hide_index=True,
            )

    with verify_tab:
        verify_left, verify_right = st.columns([1.1, 0.9])
        with verify_left:
            verify_email = st.text_input(_t("field_email"), key="verify_email")
            if st.button("Resend verification email", type="primary"):
                context = _get_auth_request_context()
                email_value = verify_email.strip().lower()
                verification_token = _issue_or_refresh_verification_token(email_value)
                if not verification_token:
                    st.error("No pending verification account was found for that email.")
                else:
                    verify_link = _build_verify_email_link(email_value, verification_token)
                    _send_access_notification(
                        related_user_email=email_value,
                        target_email=email_value,
                        event_type="verification_email_sent",
                        subject="Verify your H-REVN access account",
                        body=_build_welcome_verify_email_text(user_email=email_value, verify_link=verify_link),
                        html_body=_build_welcome_verify_email_html(user_email=email_value, verify_link=verify_link),
                    )
                    log_auth_event(
                        AUTH_ACCESS_SQLITE_PATH,
                        user_email=email_value or None,
                        user_role="operator",
                        identifier_attempted=email_value or None,
                        event_type="verification_email_resent",
                        success_flag=True,
                        failure_reason=None,
                        context=context,
                    )
                    st.success("Verification email sent again.")
        with verify_right:
            st.markdown("#### Verification flow")
            st.dataframe(
                [
                    {"Step": "1", "Action": "Create account"},
                    {"Step": "2", "Action": "Receive verification email"},
                    {"Step": "3", "Action": "Click the Verify Email button"},
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
                    _send_admin_security_email_alert(
                        "account_suspended",
                        "H-REVN security alert: account suspended",
                        f"An account has been suspended.\n\nUser: {selected_account}\nReason: {(action_reason or '').strip() or 'No reason provided'}",
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
                    _send_admin_security_email_alert(
                        "account_closed",
                        "H-REVN security alert: account closed",
                        f"An account has been closed.\n\nUser: {selected_account}\nReason: {(action_reason or '').strip()}",
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
                    _send_admin_security_email_alert(
                        "account_reactivated",
                        "H-REVN security alert: account reactivated",
                        f"An account has been reactivated.\n\nUser: {selected_account}\nReason: {(action_reason or '').strip() or 'No reason provided'}",
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
                    _send_admin_security_email_alert(
                        "sessions_revoked_by_admin",
                        "H-REVN security alert: sessions revoked",
                        f"An administrator revoked active sessions for an account.\n\nUser: {selected_account}\nRevoked sessions: {revoked}",
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
                            "LOCATION": _ip_locality_label(row.get("ip_public") or "unknown"),
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
                            "LOCATION": _ip_locality_label(row.get("ip_public") or "unknown"),
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
                            "CHANNEL": "email_admin" if row.get("delivery_channel") == "smtp" and (row.get("target_email") or "").lower() == (_secret_value("SANDBOX_SECURITY_ALERT_EMAIL", "") or _secret_value("SANDBOX_RECOVERY_NOTIFY_EMAIL", "") or _secret_value("SANDBOX_ADMIN_EMAIL", "")).strip().lower() and (row.get("event_type") or "") in {"user_lockout", "ip_cooldown", "ip_blocked", "account_suspended", "account_closed", "account_reactivated", "sessions_revoked_by_admin", "ip_unblocked_by_admin"} else (row.get("delivery_channel") or "-"),
                            "STATUS": row.get("delivery_status") or "-",
                        }
                        for row in related_notifications
                    ]
                    st.dataframe(notification_rows, use_container_width=True, hide_index=True)
                else:
                    st.info("No notification history for this account yet.")
            if related_ips:
                st.markdown("##### Related IPs")
                st.dataframe([{"IP": ip_value, "LOCATION": _ip_locality_label(ip_value)} for ip_value in related_ips], use_container_width=True, hide_index=True)
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
                    "LOCATION": _ip_locality_label(row.get("ip_public") or "unknown"),
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
                    "LOCATION": _ip_locality_label(row.get("ip_public") or "unknown"),
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
                    "CHANNEL": "email_admin" if row.get("delivery_channel") == "smtp" and (row.get("target_email") or "").lower() == (_secret_value("SANDBOX_SECURITY_ALERT_EMAIL", "") or _secret_value("SANDBOX_RECOVERY_NOTIFY_EMAIL", "") or _secret_value("SANDBOX_ADMIN_EMAIL", "")).strip().lower() and (row.get("event_type") or "") in {"user_lockout", "ip_cooldown", "ip_blocked", "account_suspended", "account_closed", "account_reactivated", "sessions_revoked_by_admin", "ip_unblocked_by_admin"} else (row.get("delivery_channel") or "-"),
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
                    "LOCATION": _ip_locality_label(row.get("ip_public") or "unknown"),
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
                    _send_admin_security_email_alert(
                        "ip_unblocked_by_admin",
                        "H-REVN security alert: IP unblocked",
                        f"An administrator unblocked an IP address.\n\nIP: {selected_ip}",
                    )
                    st.success("IP unblocked.")
                    st.rerun()


def render_central_console() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stAppViewContainer"] {background:#eef4f8;}
        [data-testid="stHeader"] {background:transparent;}
        </style>
        """,
        unsafe_allow_html=True,
    )

    production_tab, technical_tab = st.tabs([_t("production"), _t("technical_architecture")])

    with production_tab:
        agent_snapshot = load_agent_operations_snapshot(AGENT_OPERATIONS_SQLITE_PATH)

        re_accounts = list_re_v2_accounts()
        re_visits = list_re_v2_visits_raw()
        re_assets = list_re_v2_assets()

        building_admin_account_ids = {
            str(row.get("account_id") or "") for row in re_accounts if str(row.get("subgroup") or "") == "building_admin"
        }
        property_manager_account_ids = {
            str(row.get("account_id") or "") for row in re_accounts if str(row.get("subgroup") or "") == "property_manager"
        }

        building_admin_visits = [
            row for row in re_visits if str(row.get("created_by_account_id") or "") in building_admin_account_ids
        ]
        property_manager_visits = [
            row for row in re_visits if str(row.get("created_by_account_id") or "") in property_manager_account_ids
        ]

        def _review_count(rows: list[dict]) -> int:
            return len([
                row for row in rows
                if str(row.get("review_status") or "").upper() not in {"", "APPROVED", "FINAL", "DONE"}
            ])

        production_rows = [
            {"VERTICAL": "REAL ESTATE", "LINE": "Administradores de fincas", "EVENTS / VISITS": len(building_admin_visits), "IN REVIEW": _review_count(building_admin_visits), "CERTIFICATES": 0, "ZIPS": 0, "EMAILS": 0, "VERIFY": 0, "ZIP DOWNLOADS": 0, "FACTURATION": 0},
            {"VERTICAL": "REAL ESTATE", "LINE": "Property Manager", "EVENTS / VISITS": len(property_manager_visits), "IN REVIEW": _review_count(property_manager_visits), "CERTIFICATES": 0, "ZIPS": 0, "EMAILS": 0, "VERIFY": 0, "ZIP DOWNLOADS": 0, "FACTURATION": 0},
            {"VERTICAL": "REAL ESTATE", "LINE": "Family Office", "EVENTS / VISITS": 0, "IN REVIEW": 0, "CERTIFICATES": 0, "ZIPS": 0, "EMAILS": 0, "VERIFY": 0, "ZIP DOWNLOADS": 0, "FACTURATION": 0},
            {"VERTICAL": "REAL ESTATE", "LINE": "Fondos de inversión", "EVENTS / VISITS": 0, "IN REVIEW": 0, "CERTIFICATES": 0, "ZIPS": 0, "EMAILS": 0, "VERIFY": 0, "ZIP DOWNLOADS": 0, "FACTURATION": 0},
            {"VERTICAL": "ADMINISTRATION", "LINE": "Fotovoltaica", "EVENTS / VISITS": 0, "IN REVIEW": 0, "CERTIFICATES": 0, "ZIPS": 0, "EMAILS": 0, "VERIFY": 0, "ZIP DOWNLOADS": 0, "FACTURATION": 0},
        ]

        prod_df = pd.DataFrame(production_rows)
        prod_total = {"VERTICAL": "TOTAL", "LINE": "TOTAL"}
        for col in prod_df.columns:
            if col not in {"VERTICAL", "LINE"}:
                prod_total[col] = int(prod_df[col].sum())
        prod_df = pd.concat([prod_df, pd.DataFrame([prod_total])], ignore_index=True)

        agent_records = agent_snapshot.records
        agent_rows = [
            {
                "VERTICAL": "AGENTS",
                "LINE": "Agent Operations",
                "EVENTS / RECORDS": len(agent_records),
                "PENDING": len([row for row in agent_records if (row.get("status") or "") == "pending_review"]),
                "EXECUTED": len([row for row in agent_records if (row.get("status") or "") == "executed_sealed"]),
                "REJECTED": len([row for row in agent_records if (row.get("status") or "") == "rejected"]),
                "SEALED": len([row for row in agent_records if (row.get("seal_status") or "") == "sealed"]),
                "SIGNED": len([row for row in agent_records if (row.get("status") or "") == "executed_sealed"]),
            }
        ]
        agent_df = pd.DataFrame(agent_rows)
        agent_total = {"VERTICAL": "TOTAL", "LINE": "TOTAL"}
        for col in agent_df.columns:
            if col not in {"VERTICAL", "LINE"}:
                agent_total[col] = int(agent_df[col].sum())
        agent_df = pd.concat([agent_df, pd.DataFrame([agent_total])], ignore_index=True)

        email_rows = [
            {
                "VERTICAL": "EMAIL",
                "NEW": 0,
                "SENT": 0,
                "SUPPORT TICKETS": 0,
                "BUSINESS OPPORTUNITIES": 0,
                "GENERAL": 0,
            }
        ]
        email_df = pd.DataFrame(email_rows)
        email_total = {"VERTICAL": "TOTAL"}
        for col in email_df.columns:
            if col != "VERTICAL":
                email_total[col] = int(email_df[col].sum())
        email_df = pd.concat([email_df, pd.DataFrame([email_total])], ignore_index=True)

        _render_console_table_html(prod_df, total_row_index=len(prod_df) - 1)
        _render_console_table_html(agent_df, total_row_index=len(agent_df) - 1)
        _render_console_table_html(email_df, total_row_index=len(email_df) - 1)

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


def _render_real_estate_workspace(snapshot, context: dict, workspace: dict | None, readiness: RealEstateReadiness, cfg) -> None:
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
        provider_choice = choose_ai_provider(cfg)
        review_key = f"real_estate_ai_review::{visit.get('visit_id')}"
        quality_state = "ready_for_review" if photos else "blocked_no_photos"
        existing_review = st.session_state.get(review_key)
        st.write(
            {
                "provider": provider_choice.selected,
                "key_profile": getattr(cfg, "openai_key_profile", "production"),
                "review_state": existing_review.get("decision") if isinstance(existing_review, dict) else quality_state,
                "blocking_policy": "block_if_inconsistencies_detected",
                "semantic_titles_mode": "ai_generated_at_issuance",
                "delivery_mode": "async_like_after_validate_and_certify",
                "blockchain_target": cfg.blockchain_target,
            }
        )
        if photos:
            sample_title = f"Proposed title: {asset.get('asset_public_id') or visit.get('asset_id') or 'asset'} / visit evidence / first image"
            st.text_area("Semantic title preview", value=sample_title, height=100, disabled=True)

    st.markdown("### Certification gate")
    action_col, result_col = st.columns([0.9, 1.1])
    review_key = f"real_estate_ai_review::{visit.get('visit_id')}"
    existing_review = st.session_state.get(review_key)
    delivery_target = _real_estate_delivery_target_email(cfg)

    with action_col:
        st.caption("The AI review starts only when the operator validates and certifies the visit. It does not run during photo capture.")
        st.text_input("Delivery target email", value=delivery_target or "", disabled=True)
        trigger_disabled = not readiness.issuance_ready
        if st.button("Validate and certify", use_container_width=True, disabled=trigger_disabled, key=f"re_validate_certify_{visit.get('visit_id')}"):
            try:
                with st.spinner("Certification request accepted. Running AI review and issuance checks in the background..."):
                    review_result = review_real_estate_certification(
                        workspace=workspace or {},
                        provider=provider_choice.selected,
                        model=cfg.openai_model,
                        openai_api_key=_openai_api_key_for("production"),
                        openai_api_base_url=cfg.openai_api_base_url,
                        blockchain_target=cfg.blockchain_target,
                        blockchain_enabled=cfg.blockchain_enabled,
                    ).to_dict()
                    preview = build_real_estate_end_to_end_preview(snapshot, str(visit.get("visit_id") or "")) if review_result.get("approved") else None
                    review_result["preview"] = preview
                    review_result["delivery_target_email"] = delivery_target
                    if review_result.get("approved"):
                        success_body = (
                            "Your certification request has passed H-REVN AI review and integrity checks.\n\n"
                            f"Visit: {visit.get('visit_id')}\n"
                            f"Asset: {asset.get('asset_public_id') or asset.get('asset_id')}\n"
                            f"Root hash: {(preview or {}).get('certificate_preview', {}).get('root_hash_sha256', 'pending')}\n"
                            f"Blockchain target: {cfg.blockchain_target} ({review_result.get('anchor_status')})\n\n"
                            "A confirmation package can now be delivered from the operator workspace."
                        )
                        review_result["email_result"] = _send_real_estate_delivery_email(
                            target_email=delivery_target,
                            subject=f"H-REVN certification ready | {visit.get('visit_id')}",
                            body=success_body,
                        )
                    else:
                        reasons = review_result.get("blocking_reasons") or ["AI review flagged the request for manual revision."]
                        failure_body = (
                            "Your certification request could not be completed automatically.\n\n"
                            f"Visit: {visit.get('visit_id')}\n"
                            f"Asset: {asset.get('asset_public_id') or asset.get('asset_id')}\n\n"
                            "Please review the case in the operator workspace. Blocking reasons:\n- "
                            + "\n- ".join(str(item) for item in reasons)
                        )
                        review_result["email_result"] = _send_real_estate_delivery_email(
                            target_email=delivery_target,
                            subject=f"H-REVN certification needs review | {visit.get('visit_id')}",
                            body=failure_body,
                        )
                    st.session_state[review_key] = review_result
                st.rerun()
            except Exception as exc:
                _send_telegram_security_alert(
                    "ai_review_failed",
                    (
                        "H-REVN operations alert: AI review failed during certification.\n"
                        f"Visit: {visit.get('visit_id')}\n"
                        f"Asset: {asset.get('asset_public_id') or asset.get('asset_id')}\n"
                        f"Detail: {exc}"
                    ),
                )
                st.error(f"AI review failed: {exc}")

        if trigger_disabled:
            st.warning("This visit must satisfy the issuance readiness rules before AI review can start.")

    with result_col:
        if not existing_review:
            st.info("No AI certification review has been executed yet for this visit. When the operator validates and certifies, H-REVN will run the AI review, evaluate the evidence set, and either continue to issuance or stop with an exact cause.")
        else:
            if existing_review.get("approved"):
                st.success("AI review passed. The certification request cleared the pre-issuance gate.")
            else:
                st.error("AI review blocked the certification request. Manual revision is required before issuance.")
            st.dataframe(
                [
                    {"FIELD": "Decision", "VALUE": existing_review.get("decision")},
                    {"FIELD": "Execution mode", "VALUE": existing_review.get("execution_mode")},
                    {"FIELD": "Review mode", "VALUE": existing_review.get("review_mode")},
                    {"FIELD": "Reviewed at", "VALUE": existing_review.get("reviewed_at_utc")},
                    {"FIELD": "Anchor status", "VALUE": existing_review.get("anchor_status")},
                    {"FIELD": "Anchor target", "VALUE": existing_review.get("anchor_target")},
                    {"FIELD": "Delivery email", "VALUE": (existing_review.get("email_result") or {}).get("delivery_status", "not_run")},
                ],
                use_container_width=True,
                hide_index=True,
            )
            st.text_area("Review summary", value=str(existing_review.get("summary") or ""), height=110, disabled=True)
            blockers = existing_review.get("blocking_reasons") or []
            if blockers:
                st.dataframe(pd.DataFrame({"BLOCKING REASONS": blockers}), use_container_width=True, hide_index=True)
            semantic_titles = existing_review.get("semantic_titles") or []
            if semantic_titles:
                st.dataframe(pd.DataFrame(semantic_titles), use_container_width=True, hide_index=True)
            preview = existing_review.get("preview") or {}
            cert_preview = preview.get("certificate_preview") or {}
            if cert_preview:
                st.dataframe(
                    pd.DataFrame(
                        [
                            {"FIELD": "Sequence", "VALUE": cert_preview.get("global_sequence_id")},
                            {"FIELD": "Root hash", "VALUE": cert_preview.get("root_hash_sha256")},
                            {"FIELD": "PDF hash", "VALUE": cert_preview.get("pdf_hash_sha256")},
                            {"FIELD": "Verification URL", "VALUE": cert_preview.get("verification_url")},
                        ]
                    ),
                    use_container_width=True,
                    hide_index=True,
                )
            if existing_review.get("ai_error"):
                st.caption(f"AI execution note: {existing_review.get('ai_error')}")

    st.markdown("### Finalization behavior")
    st.info(
        "When the operator validates and certifies, the system now runs the AI pre-issuance gate against the production OpenAI key path, reviews the evidence set, and either confirms delivery by email or stops the case with an explicit review cause. Blockchain anchoring remains configured against the selected target and is marked pending until wallet/provider execution is connected."
    )


def _resolve_re_v2_lpi_options_for_asset(asset_row: dict | None) -> list[str]:
    if not asset_row:
        return [""]

    try:
        snapshot = load_real_estate_snapshot(REAL_ESTATE_SQLITE_PATH)
        lpi_dictionary = snapshot.lpi_dictionary
    except Exception:
        return [""]

    asset_type = str(asset_row.get("asset_type") or "").strip().lower()
    group_map = {
        "residential": "BUILDING_STANDARD",
        "tertiary": "BUILDING_STANDARD",
        "industrial": "BUILDING_STANDARD",
        "urban_land": "LAND_URBAN",
        "rural_land": "LAND_RUSTIC",
        "rustic_land": "LAND_RUSTIC",
    }
    target_group = group_map.get(asset_type, "BUILDING_STANDARD")

    filtered = [
        item for item in lpi_dictionary
        if str(item.get("lpi_group") or "").strip().upper() == target_group
    ]
    labels = [
        f"{str(item.get('lpi_code') or '').strip()} | {str(item.get('lpi_title') or '').strip()}"
        for item in filtered
        if str(item.get('lpi_code') or '').strip()
    ]
    return labels or [""]


def _render_legacy_panel_a(context: dict, *, key_prefix: str = "legacy_a") -> None:
    lpi_options = context["lpi_options"]
    all_assets = list_re_v2_assets()
    all_visits = list_re_v2_visits_raw()
    all_observations = list_re_v2_observations_raw()
    all_photos = list_re_v2_photos_raw()

    mode_key = f"{key_prefix}_mode"
    asset_key = f"{key_prefix}_asset"
    asset_selectbox_key = f"{key_prefix}_asset_selectbox"
    visit_draft_key = f"{key_prefix}_visit_draft_id"
    observation_draft_key = f"{key_prefix}_observation_draft_id"

    if mode_key not in st.session_state:
        st.session_state[mode_key] = "new_visit"
    if asset_key not in st.session_state:
        st.session_state[asset_key] = ""
    if visit_draft_key not in st.session_state:
        st.session_state[visit_draft_key] = ""
    if observation_draft_key not in st.session_state:
        st.session_state[observation_draft_key] = ""
    if capture_enabled_key not in st.session_state:
        st.session_state[capture_enabled_key] = False
    if staged_captures_key not in st.session_state:
        st.session_state[staged_captures_key] = []
    if camera_nonce_key not in st.session_state:
        st.session_state[camera_nonce_key] = 0

    asset_options = {"Select asset": ""}
    asset_options.update(
        {
            f"{item.get('asset_name') or item.get('asset_public_id') or item.get('asset_id')} ({item.get('asset_public_id') or item.get('asset_id')})": str(item.get("asset_id") or "")
            for item in all_assets
            if item.get("asset_id")
        }
    )
    asset_labels = list(asset_options.keys())

    def _build_visit_draft_id(asset_id: str) -> str:
        asset_visits = [item for item in all_visits if str(item.get("asset_id") or "") == asset_id]
        next_visit_number = len(asset_visits) + 1 if asset_id else 0
        return f"RVI-DRAFT-{asset_id}-{next_visit_number:04d}" if asset_id else ""

    def _build_observation_draft_id(visit_id: str) -> str:
        visit_observations = [item for item in all_observations if str(item.get("visit_id") or "") == visit_id]
        next_observation_number = len(visit_observations) + 1 if visit_id else 0
        return f"ROB-DRAFT-{visit_id}-{next_observation_number:03d}" if visit_id else ""

    current_asset_id = st.session_state.get(asset_key, "")
    current_asset_label = next((label for label, value in asset_options.items() if value == current_asset_id), asset_labels[0])

    current_visit_id_for_refresh = st.session_state.get(visit_draft_key, "")
    if current_visit_id_for_refresh:
        refreshed_visit = refresh_rwa_v1_capture_session(current_visit_id_for_refresh)
        if refreshed_visit:
            all_visits = list_rwa_v1_visits_raw()

    left, right = st.columns(2)
    with left:
        action_left, action_right = st.columns(2)
        if action_left.button("Nueva visita", key=f"{key_prefix}_new_visit", use_container_width=True):
            st.session_state[mode_key] = "new_visit"
            st.session_state[asset_key] = ""
            st.session_state[visit_draft_key] = ""
            st.session_state[observation_draft_key] = ""
            st.session_state[asset_selectbox_key] = asset_labels[0]
            st.session_state[capture_enabled_key] = False
            st.session_state[staged_captures_key] = []
            st.session_state[camera_nonce_key] = st.session_state.get(camera_nonce_key, 0) + 1
            st.rerun()
        if action_right.button("Nueva observación", key=f"{key_prefix}_new_observation", use_container_width=True):
            st.session_state[mode_key] = "new_observation"
            asset_id_for_observation = st.session_state.get(asset_key, "")
            if asset_id_for_observation and not st.session_state.get(visit_draft_key):
                st.session_state[visit_draft_key] = _build_visit_draft_id(asset_id_for_observation)
            st.session_state[observation_draft_key] = _build_observation_draft_id(st.session_state.get(visit_draft_key, ""))
            st.session_state[capture_enabled_key] = True
            st.session_state[staged_captures_key] = []
            st.session_state[camera_nonce_key] = st.session_state.get(camera_nonce_key, 0) + 1
            st.rerun()

        field_a, field_b = st.columns(2)
        with field_a:
            chosen_asset_label = st.selectbox(
                "Asset",
                asset_labels,
                index=asset_labels.index(current_asset_label) if current_asset_label in asset_labels else 0,
                key=asset_selectbox_key,
            )
            chosen_asset_id = asset_options[chosen_asset_label]
            previous_asset_id = st.session_state.get(asset_key, "")
            st.session_state[asset_key] = chosen_asset_id
            if chosen_asset_id != previous_asset_id:
                st.session_state[visit_draft_key] = _build_visit_draft_id(chosen_asset_id)
                if st.session_state.get(mode_key) == "new_observation":
                    st.session_state[observation_draft_key] = _build_observation_draft_id(st.session_state[visit_draft_key])
                else:
                    st.session_state[observation_draft_key] = ""

        current_asset_id = st.session_state.get(asset_key, "")
        current_asset_row = next((item for item in all_assets if str(item.get("asset_id") or "") == current_asset_id), None)
        display_visit_id = st.session_state.get(visit_draft_key, "") if st.session_state.get(mode_key) in {"new_visit", "new_observation"} else ""
        with field_b:
            st.text_input("Número de visita", value=display_visit_id, disabled=True)

        observation_display = st.session_state.get(observation_draft_key, "") if st.session_state.get(mode_key) == "new_observation" else ""
        st.text_input("Número de observación", value=observation_display, disabled=True)
        selected_observation = {}

        asset_lpi_options = _resolve_re_v2_lpi_options_for_asset(current_asset_row)
        st.selectbox(
            "LPI code (official)",
            options=asset_lpi_options,
            index=0,
            disabled=not bool(current_asset_row),
            key=f"{key_prefix}_lpi",
        )
        severity = int(selected_observation.get("severity_0_5") or 0)
        st.selectbox(
            "Severity (0-5)",
            options=[0, 1, 2, 3, 4, 5],
            index=[0, 1, 2, 3, 4, 5].index(severity),
            disabled=True,
            key=f"{key_prefix}_severity",
        )
        min_photos = 3 if severity >= 3 else 1
        st.info(f"Legacy auto rule: minimum photos required = {min_photos}")
        st.text_area(
            "Description",
            value=str(selected_observation.get("observation_description") or ""),
            height=140,
            disabled=True,
            key=f"{key_prefix}_description",
        )
        st.text_area(
            "Coordinator notes",
            value=str(selected_observation.get("coordinator_notes") or ""),
            height=120,
            disabled=True,
            key=f"{key_prefix}_notes",
        )

    with right:
        st.markdown("#### Photos for current visit")
        current_visit_photos = [item for item in all_photos if str(item.get("visit_id") or "") == display_visit_id]
        st.metric("Registered photos", len(current_visit_photos))
        enough = len(current_visit_photos) >= min_photos if current_visit_photos else False
        if current_visit_photos:
            if enough:
                st.success(f"Legacy rule satisfied: {len(current_visit_photos)}/{min_photos} photos.")
            else:
                st.warning(f"Legacy rule not satisfied: {len(current_visit_photos)}/{min_photos} photos.")
        else:
            st.info("No V2 photos registered for this visit yet.")
        st.dataframe(current_visit_photos, use_container_width=True)
        st.file_uploader(
            "Upload photos (legacy flow preview)",
            type=["jpg", "jpeg", "png"],
            accept_multiple_files=True,
            disabled=True,
            key=f"{key_prefix}_uploader",
        )
        st.caption("Upload is disabled here. This panel is a visual recovery of the old operational layout.")


def _resolve_rwa_lpi_options_for_category(asset_category: str) -> list[str]:
    try:
        snapshot = load_real_estate_snapshot(REAL_ESTATE_SQLITE_PATH)
        lpi_dictionary = snapshot.lpi_dictionary
    except Exception:
        return [""]

    group_map = {
        "residential": "BUILDING_STANDARD",
        "tertiary": "BUILDING_STANDARD",
        "industrial": "BUILDING_STANDARD",
        "urban_land": "LAND_URBAN",
        "rural_land": "LAND_RUSTIC",
        "rustic_land": "LAND_RUSTIC",
    }
    target_group = group_map.get(str(asset_category or '').strip().lower(), 'BUILDING_STANDARD')
    filtered = [
        item for item in lpi_dictionary
        if str(item.get('lpi_group') or '').strip().upper() == target_group
    ]
    labels = [
        f"{str(item.get('lpi_code') or '').strip()} | {str(item.get('lpi_title') or '').strip()}"
        for item in filtered
        if str(item.get('lpi_code') or '').strip()
    ]
    return labels or [""]


def _render_rwa_placeholder() -> None:
    ensure_rwa_v1_schema()
    ensure_rwa_v1_demo_seed()

    mode_key = "rwa_mode"
    asset_key = "rwa_asset"
    asset_selectbox_key = "rwa_asset_selectbox"
    visit_draft_key = "rwa_visit_draft_id"
    observation_draft_key = "rwa_observation_draft_id"
    capture_enabled_key = "rwa_capture_enabled"
    staged_captures_key = "rwa_staged_captures"
    camera_nonce_key = "rwa_camera_nonce"

    if mode_key not in st.session_state:
        st.session_state[mode_key] = "new_visit"
    if asset_key not in st.session_state:
        st.session_state[asset_key] = ""
    if visit_draft_key not in st.session_state:
        st.session_state[visit_draft_key] = ""
    if observation_draft_key not in st.session_state:
        st.session_state[observation_draft_key] = ""
    if capture_enabled_key not in st.session_state:
        st.session_state[capture_enabled_key] = False
    if staged_captures_key not in st.session_state:
        st.session_state[staged_captures_key] = []
    if camera_nonce_key not in st.session_state:
        st.session_state[camera_nonce_key] = 0

    all_assets = list_rwa_v1_assets()
    all_visits = list_rwa_v1_visits_raw()
    all_observations = list_rwa_v1_observations_raw()
    all_photos = list_rwa_v1_photos_raw()
    all_attachments = list_rwa_v1_attachments_raw()

    rwa_tab_capture, rwa_tab_review = st.tabs(["Capture", "Review / Validate / Sign"])

    asset_options = {"Select asset": ""}
    asset_options.update(
        {
            f"{item.get('asset_name') or item.get('asset_public_id') or item.get('asset_id')} ({item.get('asset_public_id') or item.get('asset_id')})": str(item.get('asset_id') or '')
            for item in all_assets if item.get('asset_id')
        }
    )
    asset_labels = list(asset_options.keys())

    def _build_visit_draft_id(asset_id: str) -> str:
        asset_visits = [item for item in all_visits if str(item.get('asset_id') or '') == asset_id]
        next_visit_number = len(asset_visits) + 1 if asset_id else 0
        return f"RWA-VIS-{asset_id}-{next_visit_number:03d}" if asset_id else ""

    def _build_observation_draft_id(visit_id: str) -> str:
        visit_observations = [item for item in all_observations if str(item.get('visit_id') or '') == visit_id]
        next_observation_number = len(visit_observations) + 1 if visit_id else 0
        return f"RWA-OBS-{visit_id}-{next_observation_number:03d}" if visit_id else ""

    with rwa_tab_capture:
        st.markdown(
            """
            <style>
            .rwa-capture-card {background:#ffffff;border:1px solid #e5e7eb;border-radius:14px;padding:14px 16px;margin:0 0 12px 0;}
            .rwa-capture-label {font-family:Menlo,Monaco,Consolas,monospace;font-size:0.68rem;font-weight:700;letter-spacing:0.06em;text-transform:uppercase;color:#475569;}
            .rwa-capture-value {font-family:Menlo,Monaco,Consolas,monospace;font-size:1rem;font-weight:700;color:#0f172a;margin-top:4px;word-break:break-word;}
            .rwa-capture-subtle {font-family:Menlo,Monaco,Consolas,monospace;font-size:0.76rem;color:#64748b;}
            .rwa-photo-status {display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin:0 0 12px 0;}
            .rwa-photo-box {background:#f8fafc;border:1px solid #e2e8f0;border-radius:14px;padding:14px 12px;}
            .rwa-photo-box.ok {background:#ecfdf5;border-color:#86efac;}
            .rwa-photo-box.warn {background:#fff7ed;border-color:#fdba74;}
            .rwa-photo-box.bad {background:#fef2f2;border-color:#fca5a5;}
            .rwa-staged-card {background:#ffffff;border:1px solid #e5e7eb;border-radius:12px;padding:10px 12px;margin:0 0 8px 0;}
            </style>
            """,
            unsafe_allow_html=True,
        )

        current_asset_label = st.session_state.get(asset_selectbox_key, asset_labels[0] if asset_labels else '')
        if asset_labels and current_asset_label not in asset_labels:
            current_asset_label = asset_labels[0]
            st.session_state[asset_selectbox_key] = current_asset_label

        action_left, action_right = st.columns(2)
        if action_left.button("Nueva visita", key="rwa_new_visit_button", use_container_width=True):
            st.session_state[visit_draft_key] = ""
            st.session_state[observation_draft_key] = ""
            st.session_state[capture_enabled_key] = False
            st.session_state[staged_captures_key] = []
            st.session_state[camera_nonce_key] = st.session_state.get(camera_nonce_key, 0) + 1
            st.rerun()
        if action_right.button("Nueva observación", key="rwa_new_observation_button", use_container_width=True):
            if st.session_state.get(asset_key):
                draft_visit_id = st.session_state.get(visit_draft_key) or _build_visit_draft_id(st.session_state.get(asset_key, ""))
                st.session_state[visit_draft_key] = draft_visit_id
                st.session_state[observation_draft_key] = _build_observation_draft_id(draft_visit_id)
                st.session_state[capture_enabled_key] = False
                st.session_state[staged_captures_key] = []
                st.session_state[camera_nonce_key] = st.session_state.get(camera_nonce_key, 0) + 1
                st.rerun()

        chosen_asset_label = st.selectbox(
            "Asset",
            asset_labels,
            index=asset_labels.index(current_asset_label) if current_asset_label in asset_labels else 0,
            key=asset_selectbox_key,
        )
        chosen_asset_id = asset_options[chosen_asset_label]
        previous_asset_id = st.session_state.get(asset_key, "")
        st.session_state[asset_key] = chosen_asset_id
        if chosen_asset_id != previous_asset_id:
            st.session_state[visit_draft_key] = _build_visit_draft_id(chosen_asset_id)
            st.session_state[observation_draft_key] = _build_observation_draft_id(st.session_state[visit_draft_key])
            st.session_state[capture_enabled_key] = False
            st.session_state[staged_captures_key] = []
            st.session_state[camera_nonce_key] = st.session_state.get(camera_nonce_key, 0) + 1

        current_asset_id = st.session_state.get(asset_key, "")
        current_asset_row = next((item for item in all_assets if str(item.get('asset_id') or '') == current_asset_id), None)
        display_visit_id = st.session_state.get(visit_draft_key, "")
        observation_display = st.session_state.get(observation_draft_key, "")

        top_info = st.columns(3)
        top_pairs = [
            ("Asset", str((current_asset_row or {}).get('asset_name') or chosen_asset_label or '-')),
            ("Visita", display_visit_id or '-'),
            ("Observación", observation_display or '-'),
        ]
        for col, (label, value) in zip(top_info, top_pairs):
            col.markdown(
                f"<div class='rwa-capture-card'><div class='rwa-capture-label'>{label}</div><div class='rwa-capture-value'>{value}</div></div>",
                unsafe_allow_html=True,
            )

        current_asset_category = str(current_asset_row.get('asset_type') or '') if current_asset_row else ''
        asset_lpi_options = _resolve_rwa_lpi_options_for_category(current_asset_category)
        selected_lpi = st.selectbox(
            "LPI code (official)",
            options=asset_lpi_options,
            index=0,
            disabled=not bool(current_asset_row),
            key="rwa_lpi",
        )
        severity = st.radio(
            "Severity (0-5)",
            options=[0, 1, 2, 3, 4, 5],
            horizontal=True,
            key="rwa_severity",
        )
        observation_description = st.text_area(
            "Description",
            value="",
            height=120,
            key="rwa_description",
            placeholder="Describe de forma breve la observación...",
        )

        current_visit_photos = [item for item in all_photos if str(item.get('visit_id') or '') == display_visit_id]
        existing_photo_names = {str(item.get('photo_filename') or '').strip().lower() for item in current_visit_photos}
        current_visit_row = next((item for item in all_visits if str(item.get('visit_id') or '') == display_visit_id), None)
        capture_status = str((current_visit_row or {}).get('direct_capture_session_status') or 'open')
        capture_window_minutes = int((current_visit_row or {}).get('direct_capture_window_minutes') or 0)
        direct_capture_count = len([item for item in current_visit_photos if str(item.get('ingest_mode') or '') == 'direct_capture'])
        manual_upload_count = len([item for item in current_visit_photos if str(item.get('ingest_mode') or '') == 'manual_upload'])
        staged_captures = list(st.session_state.get(staged_captures_key, []))
        staged_names = [str(item.get('filename') or '').strip() for item in staged_captures]
        all_candidate_names = [name for name in staged_names if name]
        normalized_names = [name.lower() for name in all_candidate_names]
        duplicate_inside_upload = sorted({name for name in normalized_names if normalized_names.count(name) > 1})
        duplicate_against_existing = sorted({name for name in normalized_names if name in existing_photo_names})
        has_duplicate_names = bool(duplicate_inside_upload or duplicate_against_existing)
        uploaded_count = len(staged_captures)
        min_photos = 3 if int(severity) >= 3 else 1
        total_current_count = uploaded_count
        status_class = 'ok' if (not has_duplicate_names and total_current_count >= min_photos) else ('bad' if has_duplicate_names else 'warn')

        st.markdown(
            f"""
            <div class='rwa-photo-status'>
              <div class='rwa-photo-box {status_class}'>
                <div class='rwa-capture-label'>Fotos requeridas</div>
                <div class='rwa-capture-value'>{min_photos}</div>
              </div>
              <div class='rwa-photo-box {status_class}'>
                <div class='rwa-capture-label'>Fotos registradas</div>
                <div class='rwa-capture-value'>{total_current_count}</div>
              </div>
              <div class='rwa-photo-box {('ok' if capture_status == 'open' else 'warn')}'>
                <div class='rwa-capture-label'>Sesión</div>
                <div class='rwa-capture-value'>{'Activa' if capture_status == 'open' else 'Cerrada'}</div>
                <div class='rwa-capture-subtle'>Ventana: {capture_window_minutes} min</div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        if has_duplicate_names:
            duplicate_messages = []
            if duplicate_inside_upload:
                duplicate_messages.append("duplicadas en la selección actual: " + ", ".join(duplicate_inside_upload))
            if duplicate_against_existing:
                duplicate_messages.append("ya registradas en la visita: " + ", ".join(duplicate_against_existing))
            st.error("Foto duplicada detectada: " + " | ".join(duplicate_messages))
        elif total_current_count >= min_photos:
            st.success(f"Fotos: {total_current_count}/{min_photos}")
        else:
            st.warning(f"Fotos: {total_current_count}/{min_photos}. Debes completar el mínimo antes de guardar.")

        if capture_status == 'open':
            st.info(f"La captura directa sigue abierta. Se cerrará automáticamente tras 10 minutos sin nueva captura. Ventana actual: {capture_window_minutes} min.")
        else:
            st.warning("La captura directa ya está cerrada. Esta visita queda preparada para revisión, validación y firma en el panel siguiente.")

        capture_controls = st.columns(2)
        if capture_controls[0].button("Iniciar captura", key="rwa_start_capture", use_container_width=True):
            st.session_state[capture_enabled_key] = True
            st.rerun()
        if capture_controls[1].button("Finalizar captura", key="rwa_finish_capture", use_container_width=True):
            if display_visit_id:
                create_rwa_v1_visit(
                    asset_id=current_asset_id,
                    visit_id=display_visit_id,
                    visit_data={"created_from": "rwa_intake_panel", "asset_category": current_asset_category},
                )
                rwa_store.finalize_rwa_v1_capture_session(display_visit_id)
            st.session_state[capture_enabled_key] = False
            st.rerun()

        if st.session_state.get(capture_enabled_key, False):
            camera_capture = st.camera_input("Capturar foto", key=f"rwa_camera_capture::{st.session_state.get(camera_nonce_key, 0)}")
            if camera_capture is not None:
                staged_captures = list(st.session_state.get(staged_captures_key, []))
                filename = str(camera_capture.name or f"capture_{len(staged_captures)+1}.jpg").strip()
                normalized_filename = filename.lower()
                staged_names = {str(item.get('filename') or '').lower() for item in staged_captures}
                if normalized_filename in existing_photo_names or normalized_filename in staged_names:
                    st.error(f"Duplicate photo name detected for camera capture: {filename}")
                else:
                    staged_captures.append({
                        'filename': filename,
                        'payload': camera_capture.getvalue(),
                        'mime': getattr(camera_capture, 'type', ''),
                        'ingest_mode': 'direct_capture' if capture_status == 'open' else 'manual_upload',
                    })
                    st.session_state[staged_captures_key] = staged_captures
                    st.session_state[camera_nonce_key] = st.session_state.get(camera_nonce_key, 0) + 1
                    st.rerun()

        if staged_captures:
            st.markdown("#### Fotos capturadas en esta observación")
            for item in staged_captures:
                st.markdown(
                    f"<div class='rwa-staged-card'><div class='rwa-capture-value' style='font-size:0.95rem'>{str(item.get('filename') or '')}</div><div class='rwa-capture-subtle'>Modo: {'captura directa' if str(item.get('ingest_mode') or '') == 'direct_capture' else 'carga manual'}</div></div>",
                    unsafe_allow_html=True,
                )

        primary_actions = st.columns(2)
        if primary_actions[0].button("Guardar observación", type="primary", key="rwa_save_observation", use_container_width=True):
            if not current_asset_id:
                st.warning("Select an asset first.")
            elif not display_visit_id:
                st.warning("The visit id could not be generated yet.")
            elif not observation_display:
                st.warning("The observation id could not be generated yet.")
            elif has_duplicate_names:
                st.warning("Duplicate photo names must be resolved before saving the observation.")
            elif uploaded_count < min_photos:
                st.warning(f"You must upload at least {min_photos} photo(s) before saving this observation.")
            else:
                file_entries = []
                for staged in staged_captures:
                    file_entries.append({
                        'filename': staged['filename'],
                        'payload': staged['payload'],
                        'mime': staged.get('mime', ''),
                        'ingest_mode': staged.get('ingest_mode', 'direct_capture'),
                    })
                create_rwa_v1_visit(
                    asset_id=current_asset_id,
                    visit_id=display_visit_id,
                    visit_data={"created_from": "rwa_intake_panel", "asset_category": current_asset_category},
                )
                create_rwa_v1_observation(
                    observation_id=observation_display,
                    visit_id=display_visit_id,
                    asset_id=current_asset_id,
                    lpi_code=(selected_lpi.split('|')[0].strip() if selected_lpi else ''),
                    severity_0_5=int(severity),
                    observation_description=observation_description,
                    coordinator_notes="",
                    file_entries=file_entries,
                )
                st.success(f"Observation saved: {observation_display}")
                st.session_state[observation_draft_key] = _build_observation_draft_id(display_visit_id)
                st.session_state[staged_captures_key] = []
                st.session_state[camera_nonce_key] = st.session_state.get(camera_nonce_key, 0) + 1
                st.rerun()
        if primary_actions[1].button("Finalizar visita", key="rwa_finish_visit_hint", use_container_width=True):
            st.info("La visita queda preparada para revisión y validación en el panel siguiente.")

        st.markdown("#### Fotos registradas en la visita")
        if current_visit_photos:
            st.dataframe(current_visit_photos, use_container_width=True, hide_index=True)
        else:
            st.info("No RWA photos registered for this visit yet.")

    with rwa_tab_review:
        review_rows = []
        for visit in all_visits:
            asset_row = next((item for item in all_assets if str(item.get('asset_id') or '') == str(visit.get('asset_id') or '')), {})
            photos_for_visit = [item for item in all_photos if str(item.get('visit_id') or '') == str(visit.get('visit_id') or '')]
            review_rows.append({
                'OPEN': False,
                'VISIT DATE': str(visit.get('visit_date_utc') or ''),
                'VISIT ID': str(visit.get('visit_id') or ''),
                'ASSET': str(asset_row.get('asset_name') or asset_row.get('asset_public_id') or ''),
                'CAPTURE STATUS': str(visit.get('direct_capture_session_status') or ''),
                'VISIT STATUS': str(visit.get('visit_status') or ''),
                'ISSUANCE STATUS': str(visit.get('issuance_status') or ''),
                'PHOTOS': len(photos_for_visit),
            })
        review_df = pd.DataFrame(review_rows)
        review_event = st.data_editor(
            review_df,
            use_container_width=True,
            hide_index=True,
            key='rwa_review_visits_editor',
            column_config={'OPEN': st.column_config.CheckboxColumn('OPEN')},
            disabled=['VISIT DATE','VISIT ID','ASSET','CAPTURE STATUS','VISIT STATUS','ISSUANCE STATUS','PHOTOS'],
        )
        selected_rows = review_event[review_event['OPEN'] == True] if isinstance(review_event, pd.DataFrame) else pd.DataFrame()
        if len(selected_rows) == 1:
            selected_visit_id = str(selected_rows.iloc[0].get('VISIT ID') or '')
            selected_visit = next((item for item in all_visits if str(item.get('visit_id') or '') == selected_visit_id), None)
            selected_asset = next((item for item in all_assets if str(item.get('asset_id') or '') == str((selected_visit or {}).get('asset_id') or '')), None)
            visit_data = {}
            try:
                visit_data = json.loads(str((selected_visit or {}).get('visit_data_json') or '{}'))
            except Exception:
                visit_data = {}
            selected_visit_observations = [
                item for item in all_observations if str(item.get('visit_id') or '') == selected_visit_id
            ]
            selected_visit_photos = [
                item for item in all_photos if str(item.get('visit_id') or '') == selected_visit_id
            ]
            selected_visit_attachments = [
                item for item in all_attachments if str(item.get('visit_id') or '') == selected_visit_id
            ]
            st.markdown('### Pending review details')
            c1, c2, c3 = st.columns(3)
            c1.metric('Visit', selected_visit_id)
            c2.metric('Capture status', str((selected_visit or {}).get('direct_capture_session_status') or ''))
            c3.metric('Issuance status', str((selected_visit or {}).get('issuance_status') or ''))
            st.text_input('Asset', value=str((selected_asset or {}).get('asset_name') or ''), disabled=True, key='rwa_review_asset_name')
            comment_widget_key = f"rwa_pre_issue_comments::{selected_visit_id}"
            stored_comments = str(visit_data.get('pre_issue_comments') or '')
            if st.session_state.get('rwa_review_current_visit') != selected_visit_id:
                st.session_state['rwa_review_current_visit'] = selected_visit_id
                st.session_state[comment_widget_key] = ""
            elif comment_widget_key not in st.session_state:
                st.session_state[comment_widget_key] = ""
            if st.session_state.get('rwa_review_reload_visit') == selected_visit_id:
                st.session_state[comment_widget_key] = ""
                st.session_state['rwa_review_reload_visit'] = ""
            current_comments = st.text_area('Comentarios antes de la emisión', key=comment_widget_key, height=120)
            review_uploads = st.file_uploader(
                'Añadir fotos o documentación adicional',
                type=['jpg','jpeg','png','heic','heif','webp','bmp','tif','tiff','pdf','doc','docx'],
                accept_multiple_files=True,
                key='rwa_review_uploader',
            ) or []
            review_left, review_right = st.columns(2)
            if review_left.button('Guardar comentarios y anexos', key='rwa_review_attach', use_container_width=True):
                inserted = attach_rwa_v1_files_to_visit(
                    visit_id=selected_visit_id,
                    uploaded_files=review_uploads,
                    pre_issue_comments=current_comments,
                )
                st.session_state['rwa_review_reload_visit'] = selected_visit_id
                st.success(f'Comentarios guardados y anexos añadidos: {inserted}')
                st.rerun()
            if review_right.button('Validar y firmar', key='rwa_review_issue', type='primary', use_container_width=True):
                try:
                    validate_and_issue_rwa_v1_visit(visit_id=selected_visit_id, pre_issue_comments=current_comments)
                    _send_telegram_security_alert(
                        "visit_validated_and_issued",
                        (
                            "H-REVN operations alert: RWA visit validated and issued.\n"
                            f"Visit: {selected_visit_id}\n"
                            f"Asset: {str((selected_asset or {}).get('asset_public_id') or (selected_asset or {}).get('asset_name') or '-')}"
                        ),
                    )
                    st.success(f'Visit validated and issued: {selected_visit_id}')
                    st.rerun()
                except Exception as exc:
                    _send_telegram_security_alert(
                        "certificate_issuance_failed",
                        (
                            "H-REVN operations alert: RWA visit issuance failed.\n"
                            f"Visit: {selected_visit_id}\n"
                            f"Asset: {str((selected_asset or {}).get('asset_public_id') or (selected_asset or {}).get('asset_name') or '-')}\n"
                            f"Detail: {exc}"
                        ),
                    )
                    st.error(f'Visit issuance failed: {exc}')
            summary_rows = []
            for observation in selected_visit_observations:
                observation_id = str(observation.get('observation_id') or '')
                observation_photos = [
                    item for item in selected_visit_photos if str(item.get('observation_id') or '') == observation_id
                ]
                added_manual_photos = [
                    item for item in observation_photos if str(item.get('ingest_mode') or '') == 'manual_upload'
                ]
                summary_rows.append({
                    'OBSERVATION': observation_id,
                    'LPI': str(observation.get('lpi_code') or ''),
                    'SEVERITY': int(observation.get('severity_0_5') or 0),
                    'CAPTURE PHOTOS': len([item for item in observation_photos if str(item.get('ingest_mode') or '') == 'direct_capture']),
                    'MANUAL PHOTOS': len(added_manual_photos),
                    'DESCRIPTION': str(observation.get('observation_description') or ''),
                })
            if summary_rows:
                st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)
            review_artifact_rows = []
            for photo in selected_visit_photos:
                if str(photo.get('ingest_mode') or '') == 'manual_upload':
                    review_artifact_rows.append({
                        'OPEN': False,
                        'REMOVE': False,
                        'TYPE': 'photo',
                        'ID': str(photo.get('photo_id') or ''),
                        'FILENAME': str(photo.get('photo_filename') or ''),
                        'ADDED AT': str(photo.get('added_to_record_at_utc') or ''),
                    })
            for attachment in selected_visit_attachments:
                review_artifact_rows.append({
                    'OPEN': False,
                    'REMOVE': False,
                    'TYPE': 'attachment',
                    'ID': str(attachment.get('attachment_id') or ''),
                    'FILENAME': str(attachment.get('attachment_filename') or ''),
                    'ADDED AT': str(attachment.get('added_to_record_at_utc') or ''),
                })
            st.markdown('#### Anexos y archivos añadidos en revisión')
            if review_artifact_rows:
                artifact_editor = st.data_editor(
                    pd.DataFrame(review_artifact_rows),
                    use_container_width=True,
                    hide_index=True,
                    key='rwa_review_artifacts_editor',
                    column_config={
                        'OPEN': st.column_config.CheckboxColumn('OPEN'),
                        'REMOVE': st.column_config.CheckboxColumn('REMOVE'),
                    },
                    disabled=['TYPE', 'ID', 'FILENAME', 'ADDED AT'],
                )
                selected_artifact_rows = artifact_editor[artifact_editor['OPEN'] == True] if isinstance(artifact_editor, pd.DataFrame) else pd.DataFrame()
                removable_rows = artifact_editor[artifact_editor['REMOVE'] == True] if isinstance(artifact_editor, pd.DataFrame) else pd.DataFrame()
                if len(selected_artifact_rows) == 1:
                    selected_artifact = selected_artifact_rows.iloc[0]
                    selected_artifact_comment = stored_comments
                    if str(selected_artifact.get('TYPE') or '') == 'photo':
                        selected_photo = next(
                            (
                                item for item in selected_visit_photos
                                if str(item.get('photo_id') or '') == str(selected_artifact.get('ID') or '')
                            ),
                            {},
                        )
                        try:
                            selected_photo_data = json.loads(str(selected_photo.get('photo_data_json') or '{}'))
                        except Exception:
                            selected_photo_data = {}
                        selected_artifact_comment = str(selected_photo_data.get('review_comment') or stored_comments)
                    elif str(selected_artifact.get('TYPE') or '') == 'attachment':
                        selected_attachment = next(
                            (
                                item for item in selected_visit_attachments
                                if str(item.get('attachment_id') or '') == str(selected_artifact.get('ID') or '')
                            ),
                            {},
                        )
                        try:
                            selected_attachment_data = json.loads(str(selected_attachment.get('attachment_data_json') or '{}'))
                        except Exception:
                            selected_attachment_data = {}
                        selected_artifact_comment = str(selected_attachment_data.get('review_comment') or stored_comments)
                    st.markdown('#### Detalle del anexo seleccionado')
                    d1, d2, d3 = st.columns(3)
                    d1.text_input('Tipo', value=str(selected_artifact.get('TYPE') or ''), disabled=True, key='rwa_selected_artifact_type')
                    d2.text_input('Archivo', value=str(selected_artifact.get('FILENAME') or ''), disabled=True, key='rwa_selected_artifact_filename')
                    d3.text_input('Añadido', value=str(selected_artifact.get('ADDED AT') or ''), disabled=True, key='rwa_selected_artifact_added')
                    st.text_area('Comentario guardado para este anexo', value=selected_artifact_comment, disabled=True, height=100, key='rwa_selected_artifact_comments')
                    replacement = st.file_uploader(
                        'Reemplazar archivo seleccionado',
                        type=['jpg','jpeg','png','heic','heif','webp','bmp','tif','tiff','pdf','doc','docx'],
                        accept_multiple_files=False,
                        key=f"rwa_replace_artifact::{selected_visit_id}::{selected_artifact.get('ID')}",
                    )
                    if st.button('Reemplazar archivo', key='rwa_replace_selected_artifact', use_container_width=True, disabled=replacement is None):
                        replaced = replace_rwa_v1_review_artifact(
                            visit_id=selected_visit_id,
                            artifact_kind=str(selected_artifact.get('TYPE') or ''),
                            artifact_id=str(selected_artifact.get('ID') or ''),
                            replacement_file=replacement,
                        )
                        if replaced:
                            st.success('Archivo reemplazado.')
                            st.rerun()
                        else:
                            st.error('No se pudo reemplazar el archivo seleccionado.')
                if st.button('Eliminar seleccionados', key='rwa_review_remove_artifacts'):
                    removed = 0
                    for _, row in removable_rows.iterrows():
                        if remove_rwa_v1_review_artifact(
                            visit_id=selected_visit_id,
                                artifact_kind=str(row.get('TYPE') or ''),
                            artifact_id=str(row.get('ID') or ''),
                        ):
                            removed += 1
                    st.success(f'Elementos eliminados: {removed}')
                    st.rerun()
            else:
                st.info('No hay anexos ni archivos manuales añadidos en revisión.')
        elif len(selected_rows) > 1:
            st.info('Select exactly one visit to review and issue.')
        else:
            st.info('Select one visit from the table to review, add files, and validate/sign.')


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

def _render_real_estate_user_avatar(context: dict, workspace, readiness: RealEstateReadiness) -> None:
    st.dataframe([
        {"VIEW": "User avatar", "STATE": "individual customer"},
        {"ASSET": context.get("asset_public_id") or context.get("asset_id") or "-", "VISIT": context.get("visit_id") or "-"},
        {"ISSUANCE READY": "yes" if readiness.issuance_ready else "no", "IN REVIEW": "yes" if not readiness.issuance_ready else "no"},
    ], use_container_width=True, hide_index=True)
    left, right = st.columns([1.1, 0.9])
    with left:
        _render_panel_section_title("My visits")
        st.dataframe([
            {
                "VISIT": context.get("visit_id") or "-",
                "ASSET": context.get("asset_public_id") or context.get("asset_id") or "-",
                "OBS": readiness.observation_count,
                "PHOTOS": readiness.photo_count,
                "STATUS": "Ready" if readiness.issuance_ready else "In review",
            }
        ], use_container_width=True, hide_index=True)
    with right:
        _render_panel_section_title("My delivery")
        st.dataframe([
            {"ARTIFACT": "Certificate", "STATE": "available" if readiness.already_issued else "pending"},
            {"ARTIFACT": "ZIP package", "STATE": "available" if readiness.already_issued else "pending"},
            {"ARTIFACT": "Verification", "STATE": "available" if readiness.already_issued else "pending"},
        ], use_container_width=True, hide_index=True)


def _render_real_estate_enterprise_avatar(snapshot, context: dict, workspace, readiness: RealEstateReadiness) -> None:
    total_visits = len(snapshot.visits)
    total_assets = len(snapshot.assets)
    issued_visits = 1 if readiness.already_issued else 0
    in_review = 0 if readiness.issuance_ready else 1
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("PORTFOLIO ASSETS", total_assets)
    c2.metric("VISITS", total_visits)
    c3.metric("ISSUED", issued_visits)
    c4.metric("IN REVIEW", in_review)
    left, right = st.columns([1.15, 0.85])
    with left:
        _render_panel_section_title("Enterprise account")
        st.dataframe([
            {
                "ACCOUNT": "Enterprise avatar",
                "CURRENT ASSET": context.get("asset_public_id") or context.get("asset_id") or "-",
                "CURRENT VISIT": context.get("visit_id") or "-",
                "STATUS": "Ready" if readiness.issuance_ready else "In review",
            }
        ], use_container_width=True, hide_index=True)
        _render_panel_section_title("Portfolio visits")
        visit_rows = []
        for row in snapshot.visits[:8]:
            visit_rows.append({
                "VISIT": row.get("visit_id") or "-",
                "ASSET": row.get("asset_public_id") or row.get("asset_id") or "-",
                "DATE": row.get("visit_date_utc") or row.get("inspection_date_utc") or "-",
            })
        st.dataframe(visit_rows, use_container_width=True, hide_index=True)
    with right:
        _render_panel_section_title("Enterprise delivery")
        st.dataframe([
            {"CHANNEL": "Certificates", "COUNT": issued_visits},
            {"CHANNEL": "ZIP packages", "COUNT": issued_visits},
            {"CHANNEL": "Verifications", "COUNT": issued_visits},
            {"CHANNEL": "Review backlog", "COUNT": in_review},
        ], use_container_width=True, hide_index=True)


def render_real_estate_vertical() -> None:
    if not REAL_ESTATE_SQLITE_PATH.exists():
        st.error("Real Estate SQLite snapshot not available.")
        return

    cfg = load_common_config()
    snapshot = load_real_estate_snapshot(REAL_ESTATE_SQLITE_PATH)
    visit_ids = [item.get("visit_id") for item in snapshot.visits if isinstance(item, dict) and item.get("visit_id")]
    if not visit_ids:
        st.warning("No visits available in the Real Estate snapshot.")
        return

    selected_visit = st.session_state.get("real_estate_selected_visit")
    if selected_visit not in visit_ids:
        selected_visit = visit_ids[0]
        st.session_state["real_estate_selected_visit"] = selected_visit
    context = _prepare_real_estate_context(snapshot, selected_visit)
    workspace = build_real_estate_workspace(snapshot, selected_visit)
    readiness = _build_real_estate_readiness(context, workspace)

    admin_tab, user_tab, enterprise_tab = st.tabs(["Admin", "User", "Enterprise"])

    with admin_tab:
        _render_real_estate_v2_builder()

    with user_tab:
        _render_real_estate_user_avatar(context, workspace, readiness)

    with enterprise_tab:
        _render_real_estate_enterprise_avatar(snapshot, context, workspace, readiness)


def _render_real_estate_v2_builder() -> None:
    summary = get_re_v2_summary()
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("ACCOUNTS", summary["accounts"])
    c2.metric("ENTERPRISES", summary["enterprises"])
    c3.metric("ASSETS", summary["assets"])
    c4.metric("ASSIGNMENTS", summary["assignments"])
    c5.metric("VISITS", summary["visits"])

    subgroup_counts = summary.get("subgroups", {})
    st.dataframe([
        {"SUBGROUP": "building_admin", "ACCOUNTS": subgroup_counts.get("building_admin", 0)},
        {"SUBGROUP": "property_manager", "ACCOUNTS": subgroup_counts.get("property_manager", 0)},
    ], use_container_width=True, hide_index=True)

    action_col, _ = st.columns([1, 3])
    with action_col:
        if st.button("Reset and seed demo set", key="re_v2_seed_demo", use_container_width=True):
            reset_and_seed_re_v2_demo()
            st.success("Real Estate V2 demo set regenerated.")
            st.rerun()

    tab_account, tab_enterprise, tab_recent = st.tabs([
        "Account", "Enterprise", "Recent"
    ])

    with tab_account:
        account_form_nonce = int(st.session_state.get("re_v2_account_form_nonce", 0) or 0)
        enterprises = list_re_v2_enterprises()
        enterprise_rows_by_id = {row["enterprise_id"]: row for row in enterprises}
        enterprise_options = {"Standalone / no enterprise": ""}
        enterprise_options.update({f"{row['enterprise_name']} ({row['enterprise_id']})": row['enterprise_id'] for row in enterprises})
        reset_clicked = st.button("New account", key="re_v2_new_account", use_container_width=True)
        if reset_clicked:
            _reset_real_estate_v2_account_form()
            st.rerun()

        subgroup = st.selectbox("Subgroup", ["building_admin", "property_manager"], key=f"re_v2_account_subgroup::{account_form_nonce}")
        col1, col2 = st.columns(2)
        with col1:
            user_email = st.text_input("User email", key=f"re_v2_user_email::{account_form_nonce}")
            first_name = st.text_input("First name", key=f"re_v2_user_first_name::{account_form_nonce}")
            last_name = st.text_input("Last name", key=f"re_v2_user_last_name::{account_form_nonce}")
            display_name = st.text_input("Display name (optional)", key=f"re_v2_user_display_name::{account_form_nonce}")
            user_phone = st.text_input("User phone (optional)", key=f"re_v2_user_phone::{account_form_nonce}")
            preferred_language = st.selectbox("Preferred language", ["en", "es"], key=f"re_v2_user_lang::{account_form_nonce}")
        with col2:
            enterprise_labels = list(enterprise_options.keys())
            selected_enterprise_label = st.selectbox(
                "Enterprise",
                enterprise_labels,
                key=f"re_v2_user_enterprise_select::{account_form_nonce}",
            )
            enterprise_id = enterprise_options[selected_enterprise_label]
            selected_enterprise_row = enterprise_rows_by_id.get(enterprise_id)
            selected_enterprise_data = json.loads(selected_enterprise_row.get("enterprise_data_json") or "{}") if selected_enterprise_row else {}
            enterprise_assets = list_re_v2_assets_for_enterprise(enterprise_id) if enterprise_id else []
            asset_options = (
                {f"{row['asset_name']} ({row['asset_public_id']})": row["asset_id"] for row in enterprise_assets}
                if enterprise_assets
                else {"No asset linked": ""}
            )
            valid_asset_labels = list(asset_options.keys())
            selected_asset_label = st.selectbox(
                "Asset",
                valid_asset_labels,
                key=f"re_v2_user_asset_select::{account_form_nonce}",
                disabled=not enterprise_id,
            )
            asset_id = asset_options[selected_asset_label]
            selected_asset_row = next((row for row in enterprise_assets if row["asset_id"] == asset_id), None)
            selected_asset_data = json.loads(selected_asset_row.get("asset_data_json") or "{}") if selected_asset_row else {}

            derived_asset_category = (
                (selected_asset_row or {}).get("asset_type")
                or selected_enterprise_data.get("asset_category")
                or "-"
            )
            derived_portfolio_segment = (
                selected_asset_data.get("portfolio_segment")
                or selected_enterprise_data.get("portfolio_segment")
                or "-"
            )
            derived_property_reference = (
                selected_asset_data.get("property_reference_code")
                or (selected_asset_row or {}).get("asset_public_id")
                or "-"
            )

            st.dataframe(
                [
                    {
                        "ASSET CATEGORY": str(derived_asset_category),
                        "PORTFOLIO SEGMENT": str(derived_portfolio_segment),
                        "PROPERTY REFERENCE CODE": str(derived_property_reference),
                    }
                ],
                use_container_width=True,
                hide_index=True,
            )
            st.caption("These values are auto-filled from the selected enterprise and asset.")
            if subgroup == "building_admin" and enterprise_id and not asset_id:
                st.caption("Select an asset to bind this building administrator to a concrete property context.")
            elif enterprise_id and not asset_id:
                st.caption("Select an asset to bind this property manager to a concrete property context.")

        create_clicked = st.button("Create account", type="primary", key=f"re_v2_create_account::{account_form_nonce}", use_container_width=True)

        if create_clicked:
            if not user_email.strip():
                st.warning("User email is required.")
            elif not first_name.strip() or not last_name.strip():
                st.warning("First name and last name are required.")
            else:
                profile_data = {
                    "asset_category": derived_asset_category if derived_asset_category != "-" else "",
                    "portfolio_segment": derived_portfolio_segment if derived_portfolio_segment != "-" else "",
                    "reference_code": derived_property_reference if derived_property_reference != "-" else "",
                    "selected_asset_id": asset_id or "",
                }
                account_id = create_re_v2_account(
                    user_email=user_email,
                    first_name=first_name,
                    last_name=last_name,
                    display_name=display_name,
                    user_phone=user_phone,
                    user_role="operator",
                    subgroup=subgroup,
                    enterprise_id=enterprise_id,
                    preferred_language=preferred_language,
                    profile_data=profile_data,
                )
                if asset_id:
                    create_re_v2_account_asset_link(
                        account_id=account_id,
                        asset_id=asset_id,
                        assignment_role="primary_asset_owner_view" if subgroup == "property_manager" else "building_administrator_scope",
                        link_data={"created_from": "v2_builder_account_form"},
                    )
                _reset_real_estate_v2_account_form()
                st.success(f"Account created: {account_id}")
                st.rerun()

    with tab_enterprise:
        asset_category_options = [
            "residential",
            "tertiary",
            "industrial",
            "urban_land",
            "rural_land",
        ]
        col1, col2 = st.columns(2)
        with col1:
            enterprise_name = st.text_input("Enterprise name", key="re_v2_enterprise_name")
            contact_email = st.text_input("Contact email", key="re_v2_enterprise_email")
            asset_category = st.selectbox("Asset category", asset_category_options, key="re_v2_enterprise_asset_category")
        with col2:
            enterprise_type = st.text_input("Enterprise type", value="real_estate", key="re_v2_enterprise_type")
            contact_phone = st.text_input("Contact phone", key="re_v2_enterprise_phone")
            portfolio_segment = st.text_input("Portfolio segment", key="re_v2_enterprise_portfolio_segment")
        initial_asset_name = st.text_input("Initial asset name", key="re_v2_initial_asset_name", placeholder="If empty, the system will create a default primary asset name.")
        if st.button("Create enterprise", type="primary", key="re_v2_create_enterprise"):
            if not enterprise_name.strip():
                st.warning("Enterprise name is required.")
            elif any((row.get("enterprise_name") or "").strip().lower() == enterprise_name.strip().lower() for row in enterprises):
                st.warning("An enterprise with that name already exists. Use the existing one instead of creating a duplicate.")
            else:
                enterprise_data = {
                    "asset_category": asset_category,
                    "portfolio_segment": portfolio_segment.strip(),
                }
                enterprise_id = create_re_v2_enterprise(
                    enterprise_name=enterprise_name,
                    enterprise_type=enterprise_type,
                    contact_email=contact_email,
                    contact_phone=contact_phone,
                    enterprise_data=enterprise_data,
                )
                asset_public_id = f"RE2-PUB-{uuid.uuid4().hex[:8].upper()}"
                asset_name = initial_asset_name.strip() or f"{enterprise_name.strip()} - Primary asset"
                asset_id = create_re_v2_asset(
                    enterprise_id=enterprise_id,
                    asset_public_id=asset_public_id,
                    asset_type=asset_category,
                    asset_name=asset_name,
                    address_line="",
                    city="",
                    province="",
                    postal_code="",
                    country="ES",
                    asset_data={
                        "portfolio_segment": portfolio_segment.strip(),
                        "property_reference_code": asset_public_id,
                        "created_from": "enterprise_setup",
                    },
                )
                st.success(f"Enterprise created: {enterprise_id} | Initial asset created: {asset_public_id} ({asset_id})")
                st.rerun()

    with tab_recent:
        t_enterprises, t_accounts, t_assets, t_links, t_visits = st.tabs([
            "Enterprises", "Accounts", "Assets", "Asset Demands", "Visits"
        ])
        with t_enterprises:
            enterprise_rows = list_re_v2_enterprises()
            enterprise_options = {row["enterprise_name"]: row["enterprise_id"] for row in enterprise_rows}
            if enterprise_options:
                selected_enterprise_name = st.selectbox(
                    "Enterprise Name",
                    list(enterprise_options.keys()),
                    key="re_v2_recent_enterprise_filter",
                    label_visibility="collapsed",
                )
                filtered_enterprise_rows = [row for row in enterprise_rows if row["enterprise_name"] == selected_enterprise_name]
                st.dataframe(filtered_enterprise_rows, use_container_width=True, hide_index=True)
                selected_enterprise_id = enterprise_options[selected_enterprise_name]
                detail_rows = get_re_v2_enterprise_assignment_detail(selected_enterprise_id)
                st.dataframe(detail_rows, use_container_width=True, hide_index=True)
            else:
                st.dataframe([], use_container_width=True, hide_index=True)
        with t_accounts:
            account_rows = list_re_v2_accounts()
            account_query = st.text_input(
                "Search accounts",
                key="re_v2_recent_accounts_search",
                label_visibility="collapsed",
                placeholder="Search by email, subgroup, enterprise or account id",
            ).strip().lower()
            if account_query:
                filtered_account_rows = [
                    row for row in account_rows
                    if account_query in str(row.get("user_email", "")).lower()
                    or account_query in str(row.get("subgroup", "")).lower()
                    or account_query in str(row.get("enterprise_id", "")).lower()
                    or account_query in str(row.get("account_id", "")).lower()
                ]
            else:
                filtered_account_rows = account_rows
            st.dataframe(filtered_account_rows, use_container_width=True, hide_index=True)
        with t_assets:
            asset_rows = list_re_v2_assets()
            asset_query = st.text_input(
                "Search assets",
                key="re_v2_recent_assets_search",
                label_visibility="collapsed",
                placeholder="Search by asset name, public id, city or enterprise",
            ).strip().lower()
            if asset_query:
                filtered_asset_rows = [
                    row for row in asset_rows
                    if asset_query in str(row.get("asset_name", "")).lower()
                    or asset_query in str(row.get("asset_public_id", "")).lower()
                    or asset_query in str(row.get("city", "")).lower()
                    or asset_query in str(row.get("enterprise_id", "")).lower()
                    or asset_query in str(row.get("asset_id", "")).lower()
                ]
            else:
                filtered_asset_rows = asset_rows
            st.dataframe(filtered_asset_rows, use_container_width=True, hide_index=True)
        with t_links:
            demand_rows = list_re_v2_asset_demands_rows()
            demand_query = st.text_input(
                "Search asset demands",
                key="re_v2_asset_demands_search",
                label_visibility="collapsed",
                placeholder="Search by enterprise, user, reference, asset or public id",
            ).strip().lower()
            if demand_query:
                filtered_demand_rows = [
                    row for row in demand_rows
                    if demand_query in str(row.get("enterprise_name", "")).lower()
                    or demand_query in str(row.get("property_or_user", "")).lower()
                    or demand_query in str(row.get("user_reference", "")).lower()
                    or demand_query in str(row.get("asset_name", "")).lower()
                    or demand_query in str(row.get("asset_public_id", "")).lower()
                ]
            else:
                filtered_demand_rows = demand_rows

            selected_enterprise_id = st.session_state.get("re_v2_asset_demand_selected_enterprise_id")
            table_rows = []
            for row in filtered_demand_rows:
                table_rows.append({
                    "OPEN": row.get("enterprise_id") == selected_enterprise_id,
                    "ENTERPRISE": row.get("enterprise_name", ""),
                    "PROPERTY / USER": row.get("property_or_user", ""),
                    "USER REF": row.get("user_reference", ""),
                    "ASSET": row.get("asset_name", ""),
                    "ASSET REF": row.get("asset_public_id", ""),
                    "EVENTS": int(row.get("event_visit_count") or 0),
                    "CERTIFICATES": int(row.get("certificate_count") or 0),
                    "_ENTERPRISE_ID": row.get("enterprise_id"),
                })

            if table_rows:
                editor_df = pd.DataFrame(table_rows)
                edited_df = st.data_editor(
                    editor_df,
                    hide_index=True,
                    use_container_width=True,
                    key="re_v2_asset_demands_editor",
                    column_config={
                        "OPEN": st.column_config.CheckboxColumn(required=False),
                        "_ENTERPRISE_ID": None,
                    },
                    disabled=["ENTERPRISE", "PROPERTY / USER", "USER REF", "ASSET", "ASSET REF", "EVENTS", "CERTIFICATES", "_ENTERPRISE_ID"],
                )
                open_rows = edited_df[edited_df["OPEN"] == True]
                if not open_rows.empty:
                    chosen_enterprise_id = str(open_rows.iloc[-1]["_ENTERPRISE_ID"])
                    if chosen_enterprise_id != selected_enterprise_id:
                        st.session_state["re_v2_asset_demand_selected_enterprise_id"] = chosen_enterprise_id
                        st.rerun()
                elif selected_enterprise_id and selected_enterprise_id not in {row.get("enterprise_id") for row in filtered_demand_rows}:
                    st.session_state.pop("re_v2_asset_demand_selected_enterprise_id", None)
                    st.rerun()

                selected_enterprise_id = st.session_state.get("re_v2_asset_demand_selected_enterprise_id")
                selected_rows = [row for row in filtered_demand_rows if row.get("enterprise_id") == selected_enterprise_id]
                if not selected_rows:
                    selected_rows = [row for row in filtered_demand_rows if row.get("enterprise_id")] 
                    if selected_rows:
                        st.session_state["re_v2_asset_demand_selected_enterprise_id"] = selected_rows[0].get("enterprise_id")
                        selected_enterprise_id = selected_rows[0].get("enterprise_id")
                        selected_rows = [row for row in filtered_demand_rows if row.get("enterprise_id") == selected_enterprise_id]

                if selected_rows:
                    enterprise_label = str(selected_rows[0].get("enterprise_name") or "Enterprise")
                    branch_cards = "".join(
                        f"""
                        <div style='display:grid;grid-template-columns:54px minmax(220px,1fr) 54px minmax(240px,1fr);gap:10px;align-items:center;margin-top:10px;'>
                          <div style='display:flex;align-items:center;justify-content:center;height:100%;'>
                            <div style='position:relative;width:100%;height:2px;background:#64748b;'>
                              <div style='position:absolute;right:-1px;top:-4px;width:0;height:0;border-top:5px solid transparent;border-bottom:5px solid transparent;border-left:8px solid #64748b;'></div>
                            </div>
                          </div>
                          <div style='background:#ffffff;border:1px solid #bbf7d0;border-radius:10px;padding:10px;min-width:0;'>
                            <div style='font-family:Menlo,Monaco,monospace;font-size:14px;font-weight:700;color:#0f172a;overflow-wrap:anywhere;word-break:break-word;'>{row.get('property_or_user','')}</div>
                            <div style='font-family:Menlo,Monaco,monospace;font-size:12px;color:#334155;margin-top:4px;overflow-wrap:anywhere;word-break:break-word;'>Reference: {row.get('user_reference') or '-'}</div>
                          </div>
                          <div style='display:flex;align-items:center;justify-content:center;height:100%;'>
                            <div style='position:relative;width:100%;height:2px;background:#94a3b8;'>
                              <div style='position:absolute;right:-1px;top:-4px;width:0;height:0;border-top:5px solid transparent;border-bottom:5px solid transparent;border-left:8px solid #94a3b8;'></div>
                            </div>
                          </div>
                          <div style='background:#ffffff;border:1px solid #fde68a;border-radius:10px;padding:10px;min-width:0;'>
                            <div style='font-family:Menlo,Monaco,monospace;font-size:14px;font-weight:700;color:#0f172a;overflow-wrap:anywhere;word-break:break-word;'>{row.get('asset_name','')}</div>
                            <div style='font-family:Menlo,Monaco,monospace;font-size:12px;color:#334155;margin-top:4px;overflow-wrap:anywhere;word-break:break-word;'>Reference: {row.get('asset_public_id') or '-'}</div>
                            <div style='font-family:Menlo,Monaco,monospace;font-size:12px;color:#475569;margin-top:4px;'>Events: {int(row.get('event_visit_count') or 0)} | Certificates: {int(row.get('certificate_count') or 0)}</div>
                          </div>
                        </div>
                        """
                        for row in selected_rows
                    )
                    total_events = sum(int(row.get("event_visit_count") or 0) for row in selected_rows)
                    total_certs = sum(int(row.get("certificate_count") or 0) for row in selected_rows)
                    relationship_html = f"""
                    <div style='width:100%;margin-top:14px;border:1px solid #d8e1e8;border-radius:14px;background:#ffffff;padding:16px;'>
                      <div style='display:grid;grid-template-columns:minmax(220px,280px) minmax(620px,1fr) minmax(180px,220px);gap:14px;align-items:flex-start;'>
                        <div style='background:#dbeafe;border:1px solid #93c5fd;border-radius:12px;padding:14px;'>
                          <div style='font-family:Menlo,Monaco,monospace;font-size:11px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:#1e3a8a;'>Enterprise</div>
                          <div style='font-family:Menlo,Monaco,monospace;font-size:18px;font-weight:700;color:#0f172a;margin-top:8px;overflow-wrap:anywhere;word-break:break-word;'>{enterprise_label}</div>
                          <div style='font-family:Menlo,Monaco,monospace;font-size:12px;color:#334155;margin-top:8px;'>Branches shown: {len(selected_rows)}</div>
                        </div>
                        <div style='background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:14px;'>
                          <div style='font-family:Menlo,Monaco,monospace;font-size:11px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:#334155;'>Enterprise branches</div>
                          {branch_cards}
                        </div>
                        <div style='background:#f1f5f9;border:1px solid #cbd5e1;border-radius:12px;padding:14px;'>
                          <div style='font-family:Menlo,Monaco,monospace;font-size:11px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:#334155;'>Operational summary</div>
                          <div style='display:flex;gap:10px;margin-top:10px;flex-wrap:wrap;'>
                            <div style='flex:1 1 90px;background:#ffffff;border:1px solid #d8e1e8;border-radius:10px;padding:10px;'>
                              <div style='font-family:Menlo,Monaco,monospace;font-size:11px;color:#475569;text-transform:uppercase;'>Events</div>
                              <div style='font-family:Menlo,Monaco,monospace;font-size:22px;font-weight:700;color:#0f172a;'>{total_events}</div>
                            </div>
                            <div style='flex:1 1 90px;background:#ffffff;border:1px solid #d8e1e8;border-radius:10px;padding:10px;'>
                              <div style='font-family:Menlo,Monaco,monospace;font-size:11px;color:#475569;text-transform:uppercase;'>Certificates</div>
                              <div style='font-family:Menlo,Monaco,monospace;font-size:22px;font-weight:700;color:#0f172a;'>{total_certs}</div>
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                    """
                    components.html(relationship_html, height=max(320, 220 + len(selected_rows) * 130), scrolling=False)
            else:
                st.dataframe([], use_container_width=True, hide_index=True)
        with t_visits:
            _render_panel_section_title("Recent visits")
            st.dataframe(list_re_v2_visits(), use_container_width=True, hide_index=True)


def _count_sqlite_rows(db_path: Path, table_name: str) -> int:
    if not db_path.exists():
        return 0
    try:
        with sqlite3.connect(str(db_path)) as conn:
            row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            return int(row[0] or 0) if row else 0
    except Exception:
        return 0


def render_email_panel() -> None:
    st.subheader("Email")
    ensure_communications_schema(COMMUNICATIONS_SQLITE_PATH)
    cfg = load_common_config()
    mail_status = get_mail_connector_status(cfg)

    sync_notice = None
    sync_error = None
    latest_sync = get_latest_sync_run(COMMUNICATIONS_SQLITE_PATH)
    should_auto_sync = False
    if mail_status.inbound_sync_ready:
        if not latest_sync or not latest_sync.get("created_at_utc"):
            should_auto_sync = True
        else:
            last_dt = _parse_iso_datetime(str(latest_sync.get("created_at_utc") or ""))
            should_auto_sync = not last_dt or (_utc_now_datetime() - last_dt).total_seconds() >= 1800
    if should_auto_sync:
        try:
            sync_result = sync_gmail_inbox(
                COMMUNICATIONS_SQLITE_PATH,
                gmail_client_id=_secret_value("GMAIL_CLIENT_ID", ""),
                gmail_client_secret=_secret_value("GMAIL_CLIENT_SECRET", ""),
                gmail_refresh_token=_secret_value("GMAIL_REFRESH_TOKEN", ""),
                gmail_mailbox_user=_secret_value("GMAIL_MAILBOX_USER", "me") or "me",
                gmail_sync_query=_secret_value("GMAIL_SYNC_QUERY", "is:unread") or "is:unread",
                max_results=20,
            )
            sync_notice = f"Auto-sync ok. inbox fetched={sync_result.fetched}, inbox inserted={sync_result.inserted}, sent fetched={sync_result.sent_fetched}, sent inserted={sync_result.sent_inserted}"
            if _should_emit_recovery_alert("gmail_sync_failed", "gmail_sync_recovered"):
                _send_telegram_security_alert(
                    "gmail_sync_recovered",
                    (
                        "H-REVN communications alert: Gmail sync recovered.\n"
                        f"Inbox fetched: {sync_result.fetched}\n"
                        f"Inbox inserted: {sync_result.inserted}\n"
                        f"Sent fetched: {sync_result.sent_fetched}\n"
                        f"Sent inserted: {sync_result.sent_inserted}"
                    ),
                )
        except Exception as exc:
            sync_error = f"Auto-sync failed: {exc}"
            _send_telegram_security_alert(
                "gmail_sync_failed",
                f"H-REVN communications alert: Gmail auto-sync failed.\nDetail: {exc}",
            )

    top_left, top_right = st.columns([0.72, 0.28])
    with top_left:
        st.dataframe([
            {"FIELD": "Inbound sync", "VALUE": "ready" if mail_status.inbound_sync_ready else "not_ready"},
            {"FIELD": "Outbound delivery", "VALUE": "ready" if mail_status.outbound_ready else "not_ready"},
            {"FIELD": "Preferred channel", "VALUE": mail_status.preferred_channel},
            {"FIELD": "Recovery-ready", "VALUE": "yes" if mail_status.recovery_ready else "no"},
            {"FIELD": "Source", "VALUE": str(COMMUNICATIONS_SQLITE_PATH)},
            {"FIELD": "Last sync", "VALUE": (latest_sync or {}).get("created_at_utc") or "never"},
        ], use_container_width=True, hide_index=True)
        if sync_notice:
            st.success(sync_notice)
        if sync_error:
            st.error(sync_error)
    with top_right:
        if st.button("Sync Gmail inbox", use_container_width=True, disabled=not mail_status.inbound_sync_ready):
            try:
                sync_result = sync_gmail_inbox(
                    COMMUNICATIONS_SQLITE_PATH,
                    gmail_client_id=_secret_value("GMAIL_CLIENT_ID", ""),
                    gmail_client_secret=_secret_value("GMAIL_CLIENT_SECRET", ""),
                    gmail_refresh_token=_secret_value("GMAIL_REFRESH_TOKEN", ""),
                    gmail_mailbox_user=_secret_value("GMAIL_MAILBOX_USER", "me") or "me",
                    gmail_sync_query=_secret_value("GMAIL_SYNC_QUERY", "is:unread") or "is:unread",
                    max_results=20,
                )
                st.success(
                    f"Gmail sync ok. inbox fetched={sync_result.fetched}, inbox inserted={sync_result.inserted}, sent fetched={sync_result.sent_fetched}, sent inserted={sync_result.sent_inserted}, support={sync_result.support_tickets}, business={sync_result.sales_leads}, general={sync_result.general_emails}"
                )
                if _should_emit_recovery_alert("gmail_sync_failed", "gmail_sync_recovered"):
                    _send_telegram_security_alert(
                        "gmail_sync_recovered",
                        (
                            "H-REVN communications alert: Gmail sync recovered.\n"
                            f"Inbox fetched: {sync_result.fetched}\n"
                            f"Inbox inserted: {sync_result.inserted}\n"
                            f"Sent fetched: {sync_result.sent_fetched}\n"
                            f"Sent inserted: {sync_result.sent_inserted}"
                        ),
                    )
            except Exception as exc:
                st.error(f"Gmail sync failed: {exc}")
                _send_telegram_security_alert(
                    "gmail_sync_failed",
                    f"H-REVN communications alert: Gmail sync failed.\nDetail: {exc}",
                )

    snapshot = load_communications_snapshot(COMMUNICATIONS_SQLITE_PATH)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("RECEIVED", snapshot.total_received)
    c2.metric("SENT", snapshot.total_sent)
    c3.metric("SUPPORT", snapshot.total_support)
    c4.metric("BUSINESS", snapshot.total_business)
    c5.metric("GENERAL", snapshot.total_general)
    c6.metric("INBOUND READY", "yes" if mail_status.inbound_sync_ready else "no")

    tab_all, tab_support, tab_business, tab_general, tab_outbound = st.tabs(["All", "Support", "Business", "General", "Outbound"])

    with tab_all:
        rows = snapshot.inbound_emails
        st.dataframe([
            {
                "FROM": (r.get("from_name") or r.get("from_email") or "-"),
                "EMAIL": r.get("from_email") or "-",
                "SUBJECT": r.get("subject") or "-",
                "CLASS": r.get("classification") or "general",
                "STATUS": r.get("status") or "open",
                "RECEIVED": r.get("received_at_utc") or "-",
            }
            for r in rows
        ], use_container_width=True, hide_index=True)

    with tab_support:
        rows = snapshot.support_tickets
        st.dataframe([
            {
                "TICKET": r.get("ticket_code") or "-",
                "TITLE": r.get("title") or "-",
                "CUSTOMER": r.get("customer_email") or "-",
                "TOPIC": r.get("topic") or "technical_support",
                "STATUS": r.get("status") or "open",
                "OPENED": r.get("opened_at_utc") or "-",
            }
            for r in rows
        ], use_container_width=True, hide_index=True)

    with tab_business:
        rows = snapshot.sales_leads
        st.dataframe([
            {
                "LEAD": r.get("lead_code") or "-",
                "COMPANY": r.get("company_name") or "-",
                "CONTACT": r.get("contact_email") or "-",
                "SUBJECT": r.get("subject") or "-",
                "STATUS": r.get("status") or "new",
                "CREATED": r.get("created_at_utc") or "-",
            }
            for r in rows
        ], use_container_width=True, hide_index=True)

    with tab_general:
        rows = [r for r in snapshot.inbound_emails if (r.get("classification") or "general") == "general"]
        st.dataframe([
            {
                "FROM": (r.get("from_name") or r.get("from_email") or "-"),
                "EMAIL": r.get("from_email") or "-",
                "SUBJECT": r.get("subject") or "-",
                "STATUS": r.get("status") or "open",
                "RECEIVED": r.get("received_at_utc") or "-",
            }
            for r in rows
        ], use_container_width=True, hide_index=True)

    with tab_outbound:
        rows = snapshot.outbound_emails
        st.dataframe([
            {
                "TO": r.get("to_email") or "-",
                "SUBJECT": r.get("subject") or "-",
                "CHANNEL": r.get("delivery_channel") or "smtp",
                "STATUS": r.get("delivery_status") or "queued",
                "SENT": r.get("sent_at_utc") or r.get("created_at_utc") or "-",
            }
            for r in rows
        ], use_container_width=True, hide_index=True)


def render_gov_photovoltaic_vertical() -> None:
    st.subheader("GOV / Photovoltaic")
    gov_db = LEGACY_GOV_ROOT / "hrevn_gov.db"
    panel_file = LEGACY_GOV_ROOT / "python" / "hrevn_panel.py"
    assets = _count_sqlite_rows(gov_db, "assets")
    visits = _count_sqlite_rows(gov_db, "visits")
    observations = _count_sqlite_rows(gov_db, "observations")
    photos = _count_sqlite_rows(gov_db, "photos")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("ASSETS", assets)
    c2.metric("VISITS", visits)
    c3.metric("OBSERVATIONS", observations)
    c4.metric("PHOTOS", photos)
    left, right = st.columns([1.3, 1.0])
    with left:
        _render_panel_section_title("Recovered GOV workflow")
        st.dataframe([
            {"BLOCK": "Asset selection", "DETAIL": "Asset -> visit -> photovoltaic milestone flow"},
            {"BLOCK": "Evidence milestones", "DETAIL": "Panels, inverter, electrical board, location, technical plate"},
            {"BLOCK": "Geo capture", "DETAIL": "Last-known position, age and accuracy warnings"},
            {"BLOCK": "AI review", "DETAIL": "Gemini/OpenAI review hook before issuance"},
            {"BLOCK": "Delivery", "DETAIL": "Baseline, certificate, manifest, bundle and email"},
        ], use_container_width=True, hide_index=True)
    with right:
        _render_panel_section_title("Legacy source")
        st.dataframe([
            {"FILE": "python/hrevn_panel.py", "STATE": "recovered" if panel_file.exists() else "missing"},
            {"FILE": "python/hrevn_generate_baseline_gov.py", "STATE": "recovered" if (LEGACY_GOV_ROOT / "python" / "hrevn_generate_baseline_gov.py").exists() else "missing"},
            {"FILE": "python/hrevn_generate_certificate_gov.py", "STATE": "recovered" if (LEGACY_GOV_ROOT / "python" / "hrevn_generate_certificate_gov.py").exists() else "missing"},
        ], use_container_width=True, hide_index=True)
    st.markdown("##### Operational parameters")
    st.dataframe([
        {"PARAMETER": "Required milestones", "VALUE": "Panels / inverter / electrical board / location"},
        {"PARAMETER": "Photos per milestone", "VALUE": "Up to 10"},
        {"PARAMETER": "Geo warnings", "VALUE": "Stale position and low accuracy"},
        {"PARAMETER": "Emission mode", "VALUE": "SQLite backend with issuance bundle outputs"},
    ], use_container_width=True, hide_index=True)


def render_graphic_evidence_vertical() -> None:
    st.subheader("Legal Evidence")
    evidence_root = LEGACY_PTDG_ROOT / "storage" / "evidence"
    evidence_dirs = [p for p in evidence_root.iterdir() if p.is_dir()] if evidence_root.exists() else []
    photo_count = 0
    certificate_count = 0
    manifest_count = 0
    bundle_count = 0
    for case_dir in evidence_dirs:
        photo_count += len(list((case_dir / "photos").glob("*"))) if (case_dir / "photos").exists() else 0
        certificate_count += 1 if (case_dir / "certificate.pdf").exists() else 0
        manifest_count += 1 if (case_dir / "manifest.json").exists() else 0
        bundle_count += 1 if (case_dir / "bundle.zip").exists() else 0
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("CASES", len(evidence_dirs))
    c2.metric("PHOTOS", photo_count)
    c3.metric("CERTIFICATES", certificate_count)
    c4.metric("BUNDLES", bundle_count)
    left, right = st.columns([1.2, 1.1])
    with left:
        _render_panel_section_title("Recovered capture flow")
        st.dataframe([
            {"STEP": "Capture", "DETAIL": "Simple photo-first evidence intake"},
            {"STEP": "Review", "DETAIL": "Case-level review before certificate issue"},
            {"STEP": "Package", "DETAIL": "Manifest + certificate + bundle.zip"},
            {"STEP": "Delivery", "DETAIL": "Customer-facing download and verification flow"},
        ], use_container_width=True, hide_index=True)
    with right:
        _render_panel_section_title("Recent evidence cases")
        case_rows = []
        for case_dir in sorted(evidence_dirs, reverse=True)[:8]:
            case_rows.append({
                "CASE": case_dir.name,
                "PHOTOS": len(list((case_dir / "photos").glob("*"))) if (case_dir / "photos").exists() else 0,
                "CERT": "yes" if (case_dir / "certificate.pdf").exists() else "no",
                "BUNDLE": "yes" if (case_dir / "bundle.zip").exists() else "no",
            })
        st.dataframe(case_rows, use_container_width=True, hide_index=True)


def render_genius_operations_placeholder() -> None:
    st.subheader("GENIUS Operations")
    st.info("No recovered panel is linked here yet. This vertical remains pending first operational draft.")
    st.dataframe([
        {"STATE": "pending", "NEXT": "Define regulated operations scope"},
        {"STATE": "pending", "NEXT": "Attach issuance and evidence profile"},
        {"STATE": "pending", "NEXT": "Design operator and enterprise views"},
    ], use_container_width=True, hide_index=True)


def render_telegram_panel() -> None:
    cfg = load_common_config()
    status = get_telegram_connector_status(cfg)
    snapshot = get_recent_auth_snapshot(AUTH_ACCESS_SQLITE_PATH, limit=200)
    telegram_rows = [
        row for row in snapshot.get("notifications", [])
        if (row.get("delivery_channel") or "").lower() == "telegram"
    ]

    def _telegram_type(row: dict) -> str:
        event_type = (row.get("event_type") or "").strip()
        if event_type == "manual_test":
            return "manual_test"
        if "digest" in event_type:
            return "digest"
        return "security_alert"

    def _telegram_category(row: dict) -> str:
        return (row.get("event_type") or "unknown").strip() or "unknown"

    def _detail_text(row: dict) -> str:
        if row.get("error_detail"):
            return str(row.get("error_detail"))
        if row.get("subject"):
            return str(row.get("subject"))
        return "-"

    telegram_rows_sorted = sorted(
        telegram_rows,
        key=lambda r: str(r.get("created_at_utc") or ""),
        reverse=True,
    )
    latest_row = telegram_rows_sorted[0] if telegram_rows_sorted else None
    last_sent_row = next((r for r in telegram_rows_sorted if (r.get("delivery_status") or "").lower() == "sent"), None)
    sent_24h = 0
    failed_24h = 0
    security_alerts_24h = 0
    manual_tests_24h = 0
    digests_24h = 0
    now_dt = _utc_now_datetime()
    for row in telegram_rows_sorted:
        created_dt = _parse_iso_datetime(str(row.get("created_at_utc") or ""))
        if not created_dt or (now_dt - created_dt).total_seconds() > 86400:
            continue
        delivery_status = (row.get("delivery_status") or "").lower()
        row_type = _telegram_type(row)
        if delivery_status == "sent":
            sent_24h += 1
        elif delivery_status in {"failed", "not_configured"}:
            failed_24h += 1
        if row_type == "security_alert":
            security_alerts_24h += 1
        elif row_type == "manual_test":
            manual_tests_24h += 1
        elif row_type == "digest":
            digests_24h += 1

    st.markdown("#### Telegram")
    top_left, top_right = st.columns(2)
    with top_left:
        tl_a, tl_b, tl_c = st.columns(3)
        tl_a.metric("BOT STATUS", "READY" if status.ready else ("PARTIAL" if status.enabled else "OFF"))
        tl_b.metric("DELIVERY STATUS", (last_sent_row.get("delivery_status") if last_sent_row else "no_activity").upper())
        tl_c.metric("TARGET CHAT", "SET" if status.chat_id_set else "MISSING")
    with top_right:
        tr_a, tr_b, tr_c = st.columns(3)
        tr_a.metric("LAST SENT", str(last_sent_row.get("created_at_utc") or "-") if last_sent_row else "-")
        tr_b.metric("LAST ALERT", str(latest_row.get("created_at_utc") or "-") if latest_row else "-")
        tr_c.metric("MODE", "INTERNAL ALERTS" if status.ready else ("NOT CONFIGURED" if not status.enabled else "PARTIAL"))

    mid_left, mid_right = st.columns([1, 1])
    with mid_left:
        ma, mb, mc, md = st.columns(4)
        ma.metric("SENT 24H", sent_24h)
        mb.metric("FAILED 24H", failed_24h)
        mc.metric("SECURITY ALERTS", security_alerts_24h)
        md.metric("MANUAL TESTS", manual_tests_24h)
        st.dataframe(
            [
                {"METRIC": "DIGESTS 24H", "VALUE": digests_24h},
                {"METRIC": "RECENT EVENTS", "VALUE": len(telegram_rows_sorted)},
            ],
            use_container_width=True,
            hide_index=True,
        )
    with mid_right:
        _render_panel_section_title("Configuration")
        st.dataframe(
            [
                {"FIELD": "Telegram enabled", "VALUE": "yes" if status.enabled else "no"},
                {"FIELD": "Bot token present", "VALUE": "yes" if status.bot_token_set else "no"},
                {"FIELD": "Chat ID present", "VALUE": "yes" if status.chat_id_set else "no"},
                {"FIELD": "Alert routing active", "VALUE": "yes" if status.ready else "no"},
            ],
            use_container_width=True,
            hide_index=True,
        )

    _render_panel_section_title("Recent activity")
    if telegram_rows_sorted:
        activity_rows = [
            {
                "TIME": row.get("created_at_utc") or "-",
                "TYPE": _telegram_type(row).upper(),
                "CATEGORY": _telegram_category(row).upper(),
                "STATUS": (row.get("delivery_status") or "-").upper(),
                "TARGET": row.get("target_email") or "telegram_admin_channel",
                "DETAIL": _detail_text(row),
            }
            for row in telegram_rows_sorted[:20]
        ]
        st.dataframe(activity_rows, use_container_width=True, hide_index=True)
    else:
        st.info("No Telegram activity recorded yet.")

    _render_panel_section_title("Controlled test")
    with st.form("telegram_controlled_test_form", clear_on_submit=False):
        test_message = st.text_input(
            "Test message",
            value="H-REVN Telegram controlled test",
            key="telegram_controlled_test_message",
        )
        confirmed = st.checkbox(
            "I confirm this is a manual Telegram test",
            value=False,
            key="telegram_controlled_test_confirm",
        )
        send_clicked = st.form_submit_button("Send test message", use_container_width=True)

    if send_clicked:
        if not confirmed:
            st.warning("Confirm the manual test before sending.")
        elif not status.ready:
            st.error("Telegram is not ready.")
        else:
            ok, detail = send_controlled_test_message(test_message.strip() or "H-REVN Telegram controlled test")
            log_auth_notification_event(
                AUTH_ACCESS_SQLITE_PATH,
                related_user_email=str(st.session_state.get("auth_user_email") or ""),
                target_email="telegram_admin_channel",
                event_type="manual_test",
                delivery_channel="telegram",
                delivery_status="sent" if ok else "failed",
                subject=test_message.strip() or "H-REVN Telegram controlled test",
                error_detail="" if ok else detail,
            )
            if ok:
                st.success("Telegram test sent.")
            else:
                st.error(f"Telegram test failed: {detail}")
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

    top_right = st.columns([0.84, 0.16])[1]
    with top_right:
        if st.button("Log out", use_container_width=True):
            _logout()
            st.rerun()

    target = st.session_state.get("main_tab_target")

    if target == "gov_photovoltaic":
        render_gov_photovoltaic_vertical()
        return

    if target == "graphic_evidence":
        render_graphic_evidence_vertical()
        return

    if target == "rwa":
        _render_rwa_placeholder()
        return

    if target == "genius_operations":
        render_genius_operations_placeholder()
        return

    if target == "email":
        render_email_panel()
        return

    if target == "telegram":
        render_telegram_panel()
        return

    if target == "central_console":
        render_central_console()
        return

    if target == "access_security":
        render_access_security_panel()
        return

    tab_re, tab_actions, tab_rwa, tab_arquitectura_status = st.tabs(
        [
            "Real Estate Vertical",
            "Agent Operations",
            "RWA",
            "Arquitectura Status",
        ]
    )

    with tab_re:
        render_real_estate_vertical()
    with tab_actions:
        render_controlled_actions_vertical()
    with tab_rwa:
        _render_rwa_placeholder()
    with tab_arquitectura_status:
        render_dry_run_dashboard()


if __name__ == "__main__":
    main()
