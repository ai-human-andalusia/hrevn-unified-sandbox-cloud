from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .real_estate_v2_schema import DEFAULT_DB_PATH, ensure_real_estate_v2_schema


SUBGROUPS = ("building_admin", "property_manager")


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _connect(db_path: Path | None = None) -> sqlite3.Connection:
    target = ensure_real_estate_v2_schema(db_path)
    conn = sqlite3.connect(target)
    conn.row_factory = sqlite3.Row
    return conn


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def get_re_v2_summary(db_path: Path | None = None) -> dict:
    with _connect(db_path) as conn:
        def count(table: str) -> int:
            row = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
            return int(row["c"] or 0)

        subgroup_rows = conn.execute(
            "SELECT subgroup, COUNT(*) AS c FROM re_accounts GROUP BY subgroup ORDER BY subgroup"
        ).fetchall()
        subgroup_counts = {row["subgroup"]: int(row["c"] or 0) for row in subgroup_rows}
        return {
            "accounts": count("re_accounts"),
            "enterprises": count("re_enterprises"),
            "assets": count("re_assets"),
            "visits": count("re_visits"),
            "observations": count("re_observations"),
            "photos": count("re_photos"),
            "attachments": count("re_attachments"),
            "issuances": count("re_issuances"),
            "deliveries": count("re_deliveries"),
            "subgroups": subgroup_counts,
        }


def list_re_v2_accounts(db_path: Path | None = None) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT account_id, user_email, user_phone, user_role, subgroup, enterprise_id, account_status, preferred_language, created_at_utc FROM re_accounts ORDER BY created_at_utc DESC LIMIT 50"
        ).fetchall()
        return [dict(row) for row in rows]


def list_re_v2_assets(db_path: Path | None = None) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT asset_id, asset_public_id, asset_name, asset_type, city, province, enterprise_id, asset_status, created_at_utc FROM re_assets ORDER BY created_at_utc DESC LIMIT 50"
        ).fetchall()
        return [dict(row) for row in rows]


def list_re_v2_visits(db_path: Path | None = None) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT visit_id, asset_id, created_by_account_id, visit_date_utc, visit_status, review_status, issuance_status, direct_capture_session_status, direct_capture_window_minutes, created_at_utc FROM re_visits ORDER BY created_at_utc DESC LIMIT 50"
        ).fetchall()
        return [dict(row) for row in rows]


def create_re_v2_account(*, user_email: str, user_phone: str, user_role: str, subgroup: str, enterprise_id: str, preferred_language: str, profile_data: dict, db_path: Path | None = None) -> str:
    account_id = _new_id("REA")
    now = _now_utc()
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO re_accounts (
              account_id, user_email, user_phone, user_role, subgroup, enterprise_id,
              account_status, preferred_language, profile_data_json, created_at_utc, updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?)
            """,
            (
                account_id,
                user_email.strip().lower(),
                user_phone.strip() or None,
                user_role.strip() or "operator",
                subgroup,
                enterprise_id.strip() or None,
                preferred_language.strip() or "en",
                json.dumps(profile_data or {}, ensure_ascii=True),
                now,
                now,
            ),
        )
        conn.commit()
    return account_id


def create_re_v2_enterprise(*, enterprise_name: str, enterprise_type: str, contact_email: str, contact_phone: str, enterprise_data: dict, db_path: Path | None = None) -> str:
    enterprise_id = _new_id("REE")
    now = _now_utc()
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO re_enterprises (
              enterprise_id, enterprise_name, enterprise_type, contact_email, contact_phone,
              enterprise_data_json, created_at_utc, updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                enterprise_id,
                enterprise_name.strip(),
                enterprise_type.strip() or "real_estate",
                contact_email.strip().lower() or None,
                contact_phone.strip() or None,
                json.dumps(enterprise_data or {}, ensure_ascii=True),
                now,
                now,
            ),
        )
        conn.commit()
    return enterprise_id


def create_re_v2_asset(*, enterprise_id: str, asset_public_id: str, asset_type: str, asset_name: str, address_line: str, city: str, province: str, postal_code: str, country: str, asset_data: dict, db_path: Path | None = None) -> str:
    asset_id = _new_id("RAS")
    now = _now_utc()
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO re_assets (
              asset_id, enterprise_id, asset_public_id, asset_type, asset_name, address_line,
              city, province, postal_code, country, asset_status, asset_data_json, created_at_utc, updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)
            """,
            (
                asset_id,
                enterprise_id.strip() or None,
                asset_public_id.strip(),
                asset_type.strip() or "residential",
                asset_name.strip(),
                address_line.strip() or None,
                city.strip() or None,
                province.strip() or None,
                postal_code.strip() or None,
                country.strip() or "ES",
                json.dumps(asset_data or {}, ensure_ascii=True),
                now,
                now,
            ),
        )
        conn.commit()
    return asset_id


def create_re_v2_visit(*, asset_id: str, created_by_account_id: str, visit_date_utc: str, visit_data: dict, db_path: Path | None = None) -> str:
    visit_id = _new_id("RVI")
    now = _now_utc()
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO re_visits (
              visit_id, asset_id, created_by_account_id, visit_date_utc, visit_status, review_status,
              issuance_status, delivery_status, direct_capture_session_status, direct_capture_started_at_utc,
              direct_capture_last_activity_at_utc, direct_capture_closed_at_utc, direct_capture_closed_reason,
              direct_capture_window_minutes, visit_data_json, created_at_utc, updated_at_utc
            ) VALUES (?, ?, ?, ?, 'work', 'pending', 'not_issued', 'not_delivered', 'open', ?, ?, NULL, NULL, 0, ?, ?, ?)
            """,
            (
                visit_id,
                asset_id.strip(),
                created_by_account_id.strip() or None,
                visit_date_utc.strip() or now,
                now,
                now,
                json.dumps(visit_data or {}, ensure_ascii=True),
                now,
                now,
            ),
        )
        conn.commit()
    return visit_id
