from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .rwa_v1_schema import DEFAULT_DB_PATH, ensure_rwa_v1_schema


RWA_ASSET_CATEGORIES = (
    "residential",
    "tertiary",
    "industrial",
    "urban_land",
    "rural_land",
)


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _connect(db_path: Path | None = None) -> sqlite3.Connection:
    target = ensure_rwa_v1_schema(db_path)
    conn = sqlite3.connect(target)
    conn.row_factory = sqlite3.Row
    return conn


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def ensure_rwa_v1_demo_seed(db_path: Path | None = None) -> None:
    now = _now_utc()
    with _connect(db_path) as conn:
        count = int(conn.execute("SELECT COUNT(*) FROM rwa_assets").fetchone()[0] or 0)
        if count:
            return
        enterprise_id = _new_id("RWE")
        conn.execute(
            """
            INSERT INTO rwa_enterprises (
              enterprise_id, enterprise_name, enterprise_type, contact_email, contact_phone,
              enterprise_data_json, created_at_utc, updated_at_utc
            ) VALUES (?, ?, 'rwa', ?, ?, ?, ?, ?)
            """,
            (
                enterprise_id,
                "RWA Demo Portfolio",
                "ops@rwa.demo",
                "+34 600 000 000",
                json.dumps({"seeded_demo": True}),
                now,
                now,
            ),
        )
        assets = [
            {
                "asset_public_id": "RWA-PUB-0001",
                "asset_type": "residential",
                "asset_name": "Residencial Demo Nervion",
                "asset_data": {"category": "residential", "seeded_demo": True},
            },
            {
                "asset_public_id": "RWA-PUB-0002",
                "asset_type": "tertiary",
                "asset_name": "Oficinas Demo Cartuja",
                "asset_data": {"category": "tertiary", "seeded_demo": True},
            },
        ]
        for asset in assets:
            conn.execute(
                """
                INSERT INTO rwa_assets (
                  asset_id, enterprise_id, asset_public_id, asset_type, asset_name,
                  asset_status, asset_data_json, created_at_utc, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?)
                """,
                (
                    _new_id("RWAA"),
                    enterprise_id,
                    asset["asset_public_id"],
                    asset["asset_type"],
                    asset["asset_name"],
                    json.dumps(asset["asset_data"]),
                    now,
                    now,
                ),
            )
        conn.commit()


def list_rwa_v1_assets(db_path: Path | None = None) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT asset_id, asset_public_id, asset_type, asset_name, asset_status, asset_data_json, created_at_utc
            FROM rwa_assets
            ORDER BY created_at_utc DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]


def list_rwa_v1_visits_raw(db_path: Path | None = None) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT visit_id, asset_id, created_by_account_id, visit_date_utc, visit_status, review_status, issuance_status, created_at_utc
            FROM rwa_visits
            ORDER BY created_at_utc DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]


def list_rwa_v1_observations_raw(db_path: Path | None = None) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT observation_id, visit_id, asset_id, lpi_code, severity_0_5, observation_description, row_status, observation_data_json, created_at_utc
            FROM rwa_observations
            ORDER BY created_at_utc DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]


def list_rwa_v1_photos_raw(db_path: Path | None = None) -> list[dict]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT photo_id, visit_id, asset_id, observation_id, photo_filename, photo_hash_sha256, ingest_mode, photo_status, captured_at_utc, added_to_record_at_utc, photo_data_json
            FROM rwa_photos
            ORDER BY added_to_record_at_utc DESC, captured_at_utc DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]


def create_rwa_v1_visit(*, asset_id: str, visit_id: str, visit_date_utc: str | None = None, visit_data: dict | None = None, db_path: Path | None = None) -> str:
    now = _now_utc()
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO rwa_visits (
              visit_id, asset_id, created_by_account_id, visit_date_utc, visit_status, review_status,
              issuance_status, visit_data_json, created_at_utc, updated_at_utc
            ) VALUES (?, ?, NULL, ?, 'work', 'pending', 'not_issued', ?, ?, ?)
            """,
            (
                visit_id,
                asset_id,
                visit_date_utc or now,
                json.dumps(visit_data or {}),
                now,
                now,
            ),
        )
        conn.commit()
    return visit_id


def create_rwa_v1_observation(*, observation_id: str, visit_id: str, asset_id: str, lpi_code: str, severity_0_5: int, observation_description: str, coordinator_notes: str, uploaded_files: list, db_path: Path | None = None) -> str:
    now = _now_utc()
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO rwa_observations (
              observation_id, visit_id, asset_id, lpi_code, severity_0_5, observation_description,
              row_status, observation_data_json, created_at_utc, updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, 'work', ?, ?, ?)
            """,
            (
                observation_id,
                visit_id,
                asset_id,
                lpi_code,
                int(severity_0_5),
                observation_description.strip(),
                json.dumps({"coordinator_notes": coordinator_notes.strip()}),
                now,
                now,
            ),
        )
        for uploaded in uploaded_files:
            payload = uploaded.getvalue()
            conn.execute(
                """
                INSERT INTO rwa_photos (
                  photo_id, visit_id, asset_id, observation_id, photo_filename, photo_hash_sha256,
                  ingest_mode, photo_status, captured_at_utc, added_to_record_at_utc, photo_data_json
                ) VALUES (?, ?, ?, ?, ?, ?, 'manual_upload', 'active', ?, ?, ?)
                """,
                (
                    _new_id("RWAP"),
                    visit_id,
                    asset_id,
                    observation_id,
                    uploaded.name,
                    _sha256_bytes(payload),
                    now,
                    now,
                    json.dumps({"size_bytes": len(payload), "mime": getattr(uploaded, 'type', '')}),
                ),
            )
        conn.commit()
    return observation_id
