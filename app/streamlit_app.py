"""Streamlit app for documentary-only sandbox exploration.

No real access, no external source systems, documentary-safe views only.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import streamlit as st
from common.config import load_common_config
from common.security import evaluate_secret_posture, redact_config_for_ui
from common.services.ai_router import choose_ai_provider
from common.services.gmail_connector import get_mail_connector_status
from common.services.github_connector import get_github_connector_status
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


def _init_auth_state() -> None:
    st.session_state.setdefault("auth_logged_in", False)
    st.session_state.setdefault("auth_role", "guest")
    st.session_state.setdefault("auth_email", "")
    st.session_state.setdefault("recovery_requests", [])


def _logout() -> None:
    st.session_state["auth_logged_in"] = False
    st.session_state["auth_role"] = "guest"
    st.session_state["auth_email"] = ""


def _render_auth_shell() -> None:
    cfg = _load_auth_shell_config()
    _init_auth_state()

    if st.session_state.get("auth_logged_in"):
        if st.session_state.get("auth_role") == "admin":
            st.sidebar.markdown("### Admin space")
            st.sidebar.caption("Collapsed admin navigation placeholder. Functional routing will be designed later.")
            st.sidebar.button("Central Console", disabled=True, use_container_width=True)
            st.sidebar.markdown("#### Verticals")
            st.sidebar.button("Real Estate", disabled=True, use_container_width=True)
            st.sidebar.button("GOV / Photovoltaic", disabled=True, use_container_width=True)
            st.sidebar.button("Graphic Evidence", disabled=True, use_container_width=True)
            st.sidebar.button("GENIUS Operations", disabled=True, use_container_width=True)
            st.sidebar.button("Agent Operations", disabled=True, use_container_width=True)
        return

    st.title("HREVN Unified V1 — Access Shell")
    st.caption("Documentary-safe access shell for the unified pilot. No real source access is enabled here.")

    login_tab, recovery_tab = st.tabs(["Login", "Password Recovery"])

    with login_tab:
        left, right = st.columns([1.1, 0.9])
        with left:
            email = st.text_input("Email", key="login_email")
            password = st.text_input("Password", type="password", key="login_password")
            if st.button("Access workspace", type="primary"):
                matched = None
                if cfg.admin_email and cfg.admin_password and email.strip().lower() == cfg.admin_email.strip().lower() and password == cfg.admin_password:
                    matched = ("admin", cfg.admin_email)
                elif cfg.user_email and cfg.user_password and email.strip().lower() == cfg.user_email.strip().lower() and password == cfg.user_password:
                    matched = ("operator", cfg.user_email)

                if matched:
                    st.session_state["auth_logged_in"] = True
                    st.session_state["auth_role"] = matched[0]
                    st.session_state["auth_email"] = matched[1]
                    st.success("Access granted.")
                    st.rerun()
                elif cfg.auth_enabled and cfg.has_configured_accounts:
                    st.error("Invalid credentials.")
                else:
                    st.warning("Auth shell is not fully configured yet. Use documentary demo access below.")

            if (not cfg.auth_enabled) or (not cfg.has_configured_accounts):
                if st.button("Continue in documentary demo mode"):
                    st.session_state["auth_logged_in"] = True
                    st.session_state["auth_role"] = "demo"
                    st.session_state["auth_email"] = "demo@hrevn.local"
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
            st.info(
                "This layer gives us the structure for login and recovery now. Real credential hardening can be attached later through Streamlit secrets."
            )

    with recovery_tab:
        recovery_email = st.text_input("Recovery email", key="recovery_email")
        st.text_area(
            "Recovery message preview",
            value="We have received your password recovery request. If the account is registered, the recovery flow will continue through the configured secure channel.",
            height=120,
            disabled=True,
        )
        if st.button("Request recovery"):
            requests = list(st.session_state.get("recovery_requests", []))
            requests.append(
                {
                    "email": recovery_email.strip(),
                    "status": "received",
                    "delivery_channel": "configured_secure_channel" if cfg.recovery_notify_email else "not_configured",
                }
            )
            st.session_state["recovery_requests"] = requests[-20:]
            if cfg.recovery_notify_email:
                st.success("Recovery request recorded. Delivery is configured for the secure notification path.")
            else:
                st.success("Recovery request recorded in documentary mode. No outbound reset path is configured yet.")

        if st.session_state.get("recovery_requests"):
            st.markdown("#### Recovery queue snapshot")
            st.dataframe(st.session_state["recovery_requests"], use_container_width=True)

    st.stop()


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


def _init_controlled_actions_state() -> None:
    if "controlled_actions_events" in st.session_state:
        return
    st.session_state["controlled_actions_events"] = {
        "CAR-2026-001": {
            "record_id": "CAR-2026-001",
            "submitted_at_utc": "2026-03-12T09:10:00Z",
            "agent_name": "Treasury_Bot_v2",
            "intent": "Outgoing payment release",
            "tool_name": "Swift_Transfer_API",
            "risk_level": "HIGH",
            "approval_policy": "Treasury dual approval",
            "review_reason": "Regulated outbound payment exceeds policy threshold and requires human authorization.",
            "status": "pending_review",
            "human_action": "pending",
            "seal_status": "not_sealed",
            "seal_reference": "",
            "recommended_for_execution": False,
            "parameters": [
                {"field": "amount", "value": "5000"},
                {"field": "currency", "value": "USD"},
                {"field": "destination", "value": "Vendor_Acct_8832"},
                {"field": "reference", "value": "INV-505"},
            ],
        },
        "CAR-2026-002": {
            "record_id": "CAR-2026-002",
            "submitted_at_utc": "2026-03-11T18:05:00Z",
            "agent_name": "Access_Bot_v1",
            "intent": "Admin password reset",
            "tool_name": "IAM_Admin_API",
            "risk_level": "CRITICAL",
            "approval_policy": "CISO approval required",
            "review_reason": "Privileged access action over an administrative identity requires formal authorization.",
            "status": "executed_sealed",
            "human_action": "approved",
            "seal_status": "sealed",
            "seal_reference": "sha256:8f3c4a1b9d9a0f7d2b11",
            "recommended_for_execution": True,
            "parameters": [
                {"field": "target_user", "value": "j.doe@company.com"},
                {"field": "action", "value": "force_reset"},
            ],
        },
        "CAR-2026-003": {
            "record_id": "CAR-2026-003",
            "submitted_at_utc": "2026-03-10T13:42:00Z",
            "agent_name": "Legal_Bot_v3",
            "intent": "Wallet precautionary freeze",
            "tool_name": "Chain_Freeze_API",
            "risk_level": "HIGH",
            "approval_policy": "Legal counsel review",
            "review_reason": "Potentially irreversible regulated action over client funds requires legal review.",
            "status": "rejected",
            "human_action": "rejected",
            "seal_status": "sealed_rejection",
            "seal_reference": "sha256:rejected_no_action_7721",
            "recommended_for_execution": False,
            "parameters": [
                {"field": "wallet_address", "value": "0x7a59...8f3c"},
                {"field": "reason_code", "value": "SEC_INQUIRY"},
            ],
        },
    }


def _controlled_actions_status_label(value: str) -> str:
    labels = {
        "pending_review": "Pending review",
        "executed_sealed": "Executed and sealed",
        "rejected": "Rejected",
    }
    return labels.get(value, value.replace("_", " ").title())


def _controlled_actions_risk_rank(value: str) -> int:
    order = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
    return order.get(str(value or "").upper(), 0)


def _approve_controlled_action(record_id: str) -> None:
    event = st.session_state["controlled_actions_events"][record_id]
    raw = f"{record_id}|approved|{event['agent_name']}|{event['intent']}|{event['submitted_at_utc']}"
    event["status"] = "executed_sealed"
    event["human_action"] = "approved"
    event["seal_status"] = "sealed"
    event["recommended_for_execution"] = True
    event["seal_reference"] = f"sha256:{hashlib.sha256(raw.encode()).hexdigest()[:20]}"


def _reject_controlled_action(record_id: str) -> None:
    event = st.session_state["controlled_actions_events"][record_id]
    raw = f"{record_id}|rejected|{event['agent_name']}|{event['intent']}|{event['submitted_at_utc']}"
    event["status"] = "rejected"
    event["human_action"] = "rejected"
    event["seal_status"] = "sealed_rejection"
    event["recommended_for_execution"] = False
    event["seal_reference"] = f"sha256:{hashlib.sha256(raw.encode()).hexdigest()[:20]}"


def render_controlled_actions_vertical() -> None:
    _init_controlled_actions_state()
    st.subheader("Agent Operations")
    st.caption("Review-ready records for regulated AI operations that require human approval before execution.")

    events = st.session_state["controlled_actions_events"]
    records = list(events.values())
    records.sort(key=lambda item: (item["status"] != "pending_review", -_controlled_actions_risk_rank(item["risk_level"]), item["record_id"]))

    metric_1, metric_2, metric_3, metric_4 = st.columns(4)
    metric_1.metric("Records", len(records))
    metric_2.metric("Pending review", sum(1 for item in records if item["status"] == "pending_review"))
    metric_3.metric("Approved", sum(1 for item in records if item["human_action"] == "approved"))
    metric_4.metric("Rejected", sum(1 for item in records if item["human_action"] == "rejected"))

    list_col, detail_col = st.columns([1.05, 1.95])

    with list_col:
        st.markdown("### Recent records")
        table_rows = [
            {
                "Record": item["record_id"],
                "Agent": item["agent_name"],
                "Operation": item["intent"],
                "Risk": item["risk_level"],
                "Status": _controlled_actions_status_label(item["status"]),
            }
            for item in records
        ]
        st.dataframe(table_rows, use_container_width=True, hide_index=True)
        labels = [f"{item['record_id']} | {_controlled_actions_status_label(item['status'])} | {item['intent']}" for item in records]
        selected_label = st.radio("Select operation record", labels, key="controlled_actions_selected")
        selected_id = selected_label.split(" | ")[0]
        selected = events[selected_id]

    with detail_col:
        st.markdown("### Regulated operation review record")
        c1, c2, c3 = st.columns(3)
        c1.metric("Risk level", selected["risk_level"])
        c2.metric("Approval policy", selected["approval_policy"])
        c3.metric("Current status", _controlled_actions_status_label(selected["status"]))

        st.markdown("### Why regulatory review is required")
        st.info(selected["review_reason"])

        block_a, block_b = st.columns([1.1, 0.9])
        with block_a:
            st.markdown("### Proposed operation")
            st.dataframe(
                [
                    {"Field": "Record ID", "Value": selected["record_id"]},
                    {"Field": "Submitted at", "Value": selected["submitted_at_utc"]},
                    {"Field": "Agent", "Value": selected["agent_name"]},
                    {"Field": "Operation", "Value": selected["intent"]},
                    {"Field": "Tool", "Value": selected["tool_name"]},
                ],
                use_container_width=True,
                hide_index=True,
            )
            st.markdown("#### Operation parameters")
            st.dataframe(
                [{"Parameter": row["field"], "Value": row["value"]} for row in selected["parameters"]],
                use_container_width=True,
                hide_index=True,
            )
        with block_b:
            st.markdown("### Human authorization")
            decision_rows = [
                {"Control": "Human authorization", "State": selected["human_action"].replace("_", " ").title()},
                {"Control": "Recommended for execution", "State": "Yes" if selected["recommended_for_execution"] else "No"},
                {"Control": "Regulated review path", "State": "Required"},
                {"Control": "Seal status", "State": selected["seal_status"].replace("_", " ").title()},
            ]
            st.dataframe(decision_rows, use_container_width=True, hide_index=True)
            if selected["status"] == "pending_review":
                btn1, btn2 = st.columns(2)
                with btn1:
                    if st.button("Authorize and execute", type="primary", use_container_width=True, key=f"approve_{selected_id}"):
                        _approve_controlled_action(selected_id)
                        st.rerun()
                with btn2:
                    if st.button("Reject", use_container_width=True, key=f"reject_{selected_id}"):
                        _reject_controlled_action(selected_id)
                        st.rerun()
            elif selected["status"] == "executed_sealed":
                st.success("Operation authorized, executed and sealed.")
            else:
                st.error("Operation rejected. Rejection record sealed.")

        st.markdown("### Execution record and verification seal")
        seal_rows = [
            {"Field": "Seal reference", "Value": selected["seal_reference"] or "Pending decision"},
            {"Field": "Export package", "Value": "Ready" if selected["status"] != "pending_review" else "Waiting for human decision"},
            {"Field": "Record type", "Value": "Regulated AI operation review record"},
        ]
        st.dataframe(seal_rows, use_container_width=True, hide_index=True)

        if selected["status"] != "pending_review":
            export_text = "\n".join([
                f"Record: {selected['record_id']}",
                f"Agent: {selected['agent_name']}",
                f"Intent: {selected['intent']}",
                f"Decision: {selected['human_action']}",
                f"Seal: {selected['seal_reference']}",
            ])
            st.download_button(
                "Export regulated operation package",
                data=export_text.encode("utf-8"),
                file_name=f"{selected_id}_review_package.txt",
                mime="text/plain",
                use_container_width=True,
            )
        else:
            st.caption("Package export becomes available after human approval or rejection.")

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
    st.caption("Static documentary/project signals only. No execution against real systems.")

    rows = []
    for name, path in _directory_snapshot():
        stats = _stats_for(path)
        rows.append(
            {
                "area": name,
                "files": stats["files"],
                "md": stats["md"],
                "yaml_yml": stats["yaml_yml"],
                "json": stats["json"],
                "py": stats["py"],
            }
        )

    st.dataframe(rows, use_container_width=True)

    st.markdown("### Sandbox Safety State")
    st.write(
        {
            "real_access_enabled": False,
            "sqlite_connections": False,
            "real_data_reads": False,
            "mode": "documentation_only",
        }
    )

    st.markdown("### Unified Common Layer (Local-Only)")
    cfg = load_common_config()
    posture = evaluate_secret_posture(cfg)
    provider = choose_ai_provider(cfg)
    mail = get_mail_connector_status(cfg)
    gh = get_github_connector_status(cfg)
    tg = get_telegram_connector_status(cfg)

    c1, c2 = st.columns(2)
    with c1:
        st.write("Redacted connector config")
        st.json(redact_config_for_ui(cfg))
    with c2:
        st.write("Operational selection")
        st.json(
            {
                "ai_provider_selected": provider.selected,
                "ai_provider_reason": provider.reason,
                "mail_preferred_channel": mail.preferred_channel,
                "github_ready_for_push_ops": gh.ready_for_push_ops,
                "telegram_ready": tg.ready,
            }
        )

    st.write("Secret posture checks")
    st.json(
        {
            "openai_ok": posture.openai_ok,
            "gemini_ok": posture.gemini_ok,
            "gmail_oauth_ok": posture.gmail_oauth_ok,
            "smtp_ok": posture.smtp_ok,
            "github_ok": posture.github_ok,
            "telegram_ok": posture.telegram_ok,
        }
    )

    st.write("Connector readiness")
    st.json(
        {
            "gmail_oauth_ready": mail.gmail_oauth_ready,
            "smtp_ready": mail.smtp_ready,
            "outbound_ready": mail.outbound_ready,
            "recovery_ready": mail.recovery_ready,
            "inbound_sync_ready": mail.inbound_sync_ready,
            "github_repo_ref_set": gh.repo_ref_set,
            "github_token_set": gh.token_set,
            "github_branch": gh.branch,
            "telegram_enabled": tg.enabled,
            "telegram_bot_token_set": tg.bot_token_set,
            "telegram_chat_id_set": tg.chat_id_set,
            "telegram_ready": tg.ready,
        }
    )

    st.write("Platform operations readiness")
    st.json(
        {
            "mail_from_configured": bool(cfg.mail_from),
            "notify_email_configured": bool(cfg.notify_email),
            "gmail_sync_query": cfg.gmail_sync_query,
            "blockchain_enabled": cfg.blockchain_enabled,
            "blockchain_network": cfg.blockchain_network,
            "blockchain_target": cfg.blockchain_target,
        }
    )

    st.markdown("### Telegram Controlled Test")
    st.caption("One explicit send per click. Use only for controlled validation.")
    test_message = st.text_input(
        "Test message",
        value="[HREVN CLOUD] Controlled Telegram test",
        key="telegram_test_message",
    )
    confirm_send = st.checkbox(
        "I confirm sending one controlled Telegram test message now.",
        key="telegram_test_confirm",
    )
    if st.button("Send Telegram Controlled Test", key="telegram_test_send"):
        if not confirm_send:
            st.warning("Please confirm before sending the test message.")
        elif not tg.ready:
            st.error("Telegram is not ready. Check secrets and connector status first.")
        else:
            ok, detail = send_controlled_test_message(test_message)
            if ok:
                st.success(f"Telegram test sent successfully ({detail}).")
            else:
                st.error(f"Telegram test failed ({detail}).")

    findings = run_secret_hygiene_scan(ROOT)
    st.write(f"Secret hygiene scan findings (max 25): {len(findings)}")
    if findings:
        st.dataframe(
            [
                {
                    "file_path": f.file_path,
                    "line_number": f.line_number,
                    "category": f.category,
                    "snippet": f.snippet,
                }
                for f in findings
            ],
            use_container_width=True,
        )
    else:
        st.success("No obvious hardcoded secret patterns detected in scanned files.")


def main() -> None:
    st.set_page_config(page_title="HREVN Sandbox — Documentary Panels", layout="wide", initial_sidebar_state="collapsed")
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
