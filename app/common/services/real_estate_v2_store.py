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
            "assignments": count("re_account_asset_links"),
            "visits": count("re_visits"),
            "observations": count("re_observations"),
            "photos": count("re_photos"),
            "attachments": count("re_attachments"),
            "issuances": count("re_issuances"),
            "deliveries": count("re_deliveries"),
            "subgroups": subgroup_counts,
        }


def list_re_v2_enterprises(db_path: Path | None = None) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT enterprise_id, enterprise_name, enterprise_type, contact_email, contact_phone, created_at_utc FROM re_enterprises ORDER BY created_at_utc DESC LIMIT 50"
        ).fetchall()
        return [dict(row) for row in rows]

def get_re_v2_enterprise_assignment_detail(enterprise_id: str, db_path: Path | None = None) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
              a.user_email AS property_or_user,
              COALESCE(json_extract(a.profile_data_json, '$.reference_code'), '') AS user_reference,
              s.asset_name,
              s.asset_public_id,
              COUNT(DISTINCT v.visit_id) AS event_visit_count,
              COUNT(DISTINCT i.issuance_id) AS certificate_count
            FROM re_accounts a
            LEFT JOIN re_account_asset_links l ON l.account_id = a.account_id
            LEFT JOIN re_assets s ON s.asset_id = l.asset_id
            LEFT JOIN re_visits v ON v.asset_id = s.asset_id
            LEFT JOIN re_issuances i ON i.asset_id = s.asset_id AND i.certificate_status = 'issued'
            WHERE a.enterprise_id = ?
            GROUP BY a.account_id, s.asset_id
            ORDER BY a.user_email ASC, s.asset_public_id ASC
            """,
            (enterprise_id.strip(),),
        ).fetchall()
        return [dict(row) for row in rows]


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


def list_re_v2_account_asset_links(db_path: Path | None = None) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT l.link_id, l.account_id, a.user_email, l.asset_id, s.asset_public_id, l.assignment_role, l.created_at_utc
            FROM re_account_asset_links l
            JOIN re_accounts a ON a.account_id = l.account_id
            JOIN re_assets s ON s.asset_id = l.asset_id
            ORDER BY l.created_at_utc DESC LIMIT 50
            """
        ).fetchall()
        return [dict(row) for row in rows]

def list_re_v2_asset_demands_rows(db_path: Path | None = None) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
              l.link_id,
              e.enterprise_id,
              e.enterprise_name,
              a.account_id,
              a.user_email AS property_or_user,
              COALESCE(json_extract(a.profile_data_json, '$.reference_code'), '') AS user_reference,
              s.asset_id,
              s.asset_name,
              s.asset_public_id,
              l.assignment_role,
              COUNT(DISTINCT v.visit_id) AS event_visit_count,
              COUNT(DISTINCT i.issuance_id) AS certificate_count
            FROM re_account_asset_links l
            JOIN re_accounts a ON a.account_id = l.account_id
            JOIN re_assets s ON s.asset_id = l.asset_id
            LEFT JOIN re_enterprises e ON e.enterprise_id = a.enterprise_id
            LEFT JOIN re_visits v ON v.asset_id = s.asset_id
            LEFT JOIN re_issuances i ON i.asset_id = s.asset_id AND i.certificate_status = 'issued'
            GROUP BY l.link_id, e.enterprise_id, e.enterprise_name, a.account_id, a.user_email, user_reference, s.asset_id, s.asset_name, s.asset_public_id, l.assignment_role
            ORDER BY e.enterprise_name ASC, a.user_email ASC, s.asset_public_id ASC
            """
        ).fetchall()
        return [dict(row) for row in rows]


def list_re_v2_visits(db_path: Path | None = None) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
              v.visit_date_utc,
              v.visit_id,
              COALESCE(a.user_email, '') AS created_by_user,
              COALESCE(s.asset_name, '') AS asset_name,
              v.visit_status,
              v.review_status,
              v.issuance_status,
              v.direct_capture_session_status,
              v.direct_capture_window_minutes
            FROM re_visits v
            LEFT JOIN re_accounts a ON a.account_id = v.created_by_account_id
            LEFT JOIN re_assets s ON s.asset_id = v.asset_id
            ORDER BY v.created_at_utc DESC LIMIT 50
            """
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


def create_re_v2_account_asset_link(*, account_id: str, asset_id: str, assignment_role: str = 'assigned_operator', link_data: dict | None = None, db_path: Path | None = None) -> str:
    link_id = _new_id('RAL')
    now = _now_utc()
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO re_account_asset_links (
              link_id, account_id, asset_id, assignment_role, link_data_json, created_at_utc, updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                link_id,
                account_id.strip(),
                asset_id.strip(),
                assignment_role.strip() or 'assigned_operator',
                json.dumps(link_data or {}, ensure_ascii=True),
                now,
                now,
            ),
        )
        conn.commit()
    return link_id


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


def reset_and_seed_re_v2_demo(db_path: Path | None = None) -> None:
    target = Path(db_path) if db_path else DEFAULT_DB_PATH
    if target.exists():
        target.unlink()
    ensure_real_estate_v2_schema(target)

    demo = [
        {
            'enterprise_name': 'Andalucia Building Services',
            'enterprise_type': 'real_estate',
            'contact_email': 'ops@andaluciabuildingservices.demo',
            'contact_phone': '+34 600 100 100',
            'accounts': [
                {
                    'user_email': 'laura.adminfincas@andaluciabuildingservices.demo',
                    'user_phone': '+34 600 100 101',
                    'subgroup': 'building_admin',
                    'preferred_language': 'es',
                    'profile_data': {'scope_field': 'Residencial Aljarafe', 'reference_code': 'COM-ALJ-001'},
                },
                {
                    'user_email': 'diego.property@andaluciabuildingservices.demo',
                    'user_phone': '+34 600 100 102',
                    'subgroup': 'property_manager',
                    'preferred_language': 'es',
                    'profile_data': {'scope_field': 'Premium Rentals', 'reference_code': 'PM-ALJ-002'},
                },
            ],
            'assets': [
                {
                    'asset_public_id': 'RE2-PUB-0001',
                    'asset_type': 'apartment_block',
                    'asset_name': 'Residencial Aljarafe I',
                    'address_line': 'Calle Azahar 14',
                    'city': 'Tomares',
                    'province': 'Sevilla',
                    'postal_code': '41940',
                    'asset_data': {'community_name': 'Residencial Aljarafe', 'building_block': 'Bloque A'},
                },
                {
                    'asset_public_id': 'RE2-PUB-0002',
                    'asset_type': 'rental_apartment',
                    'asset_name': 'Apartamento Piloto 2B',
                    'address_line': 'Avenida del Olivo 21',
                    'city': 'Mairena del Aljarafe',
                    'province': 'Sevilla',
                    'postal_code': '41927',
                    'asset_data': {'occupancy_status': 'vacant', 'portfolio_segment': 'Premium Rentals'},
                },
            ],
        },
        {
            'enterprise_name': 'Iberia Portfolio Partners',
            'enterprise_type': 'real_estate',
            'contact_email': 'ops@iberiaportfolio.demo',
            'contact_phone': '+34 600 200 200',
            'accounts': [
                {
                    'user_email': 'marta.adminfincas@iberiaportfolio.demo',
                    'user_phone': '+34 600 200 201',
                    'subgroup': 'building_admin',
                    'preferred_language': 'en',
                    'profile_data': {'scope_field': 'Sevilla Centro', 'reference_code': 'COM-SVQ-010'},
                },
                {
                    'user_email': 'alex.property@iberiaportfolio.demo',
                    'user_phone': '+34 600 200 202',
                    'subgroup': 'property_manager',
                    'preferred_language': 'en',
                    'profile_data': {'scope_field': 'Corporate Lets', 'reference_code': 'PM-SVQ-011'},
                },
            ],
            'assets': [
                {
                    'asset_public_id': 'RE2-PUB-0003',
                    'asset_type': 'mixed_use_building',
                    'asset_name': 'Edificio Rioja 8',
                    'address_line': 'Calle Rioja 8',
                    'city': 'Sevilla',
                    'province': 'Sevilla',
                    'postal_code': '41001',
                    'asset_data': {'community_name': 'Rioja 8', 'building_block': 'Principal'},
                },
                {
                    'asset_public_id': 'RE2-PUB-0004',
                    'asset_type': 'managed_flat',
                    'asset_name': 'Corporate Flat Nervion',
                    'address_line': 'Calle Luis de Morales 12',
                    'city': 'Sevilla',
                    'province': 'Sevilla',
                    'postal_code': '41018',
                    'asset_data': {'occupancy_status': 'occupied', 'portfolio_segment': 'Corporate Lets'},
                },
            ],
        },
    ]

    enterprise_ids = []
    account_ids = []
    asset_ids = []
    for pack in demo:
        enterprise_id = create_re_v2_enterprise(
            enterprise_name=pack['enterprise_name'],
            enterprise_type=pack['enterprise_type'],
            contact_email=pack['contact_email'],
            contact_phone=pack['contact_phone'],
            enterprise_data={},
            db_path=target,
        )
        enterprise_ids.append(enterprise_id)

        local_account_ids = []
        for account in pack['accounts']:
            account_id = create_re_v2_account(
                user_email=account['user_email'],
                user_phone=account['user_phone'],
                user_role='operator',
                subgroup=account['subgroup'],
                enterprise_id=enterprise_id,
                preferred_language=account['preferred_language'],
                profile_data=account['profile_data'],
                db_path=target,
            )
            local_account_ids.append(account_id)
            account_ids.append(account_id)

        local_asset_ids = []
        for asset in pack['assets']:
            asset_id = create_re_v2_asset(
                enterprise_id=enterprise_id,
                asset_public_id=asset['asset_public_id'],
                asset_type=asset['asset_type'],
                asset_name=asset['asset_name'],
                address_line=asset['address_line'],
                city=asset['city'],
                province=asset['province'],
                postal_code=asset['postal_code'],
                country='ES',
                asset_data=asset['asset_data'],
                db_path=target,
            )
            local_asset_ids.append(asset_id)
            asset_ids.append(asset_id)

        for account_id, asset_id in zip(local_account_ids, local_asset_ids):
            create_re_v2_account_asset_link(
                account_id=account_id,
                asset_id=asset_id,
                assignment_role='primary_asset_owner_view',
                link_data={'seeded_demo': True},
                db_path=target,
            )
            create_re_v2_visit(
                asset_id=asset_id,
                created_by_account_id=account_id,
                visit_date_utc=_now_utc(),
                visit_data={'seeded_demo': True, 'workflow_note': 'Seeded V2 demo visit'},
                db_path=target,
            )

