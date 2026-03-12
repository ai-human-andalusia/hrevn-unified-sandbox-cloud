"""Read-only Real Estate SQLite helpers for the cloud demo."""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RealEstateSnapshot:
    visits: list[dict[str, Any]]
    assets: list[dict[str, Any]]
    observations: list[dict[str, Any]]
    photos: list[dict[str, Any]]
    lpi_dictionary: list[dict[str, Any]]


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def load_real_estate_snapshot(db_path: Path) -> RealEstateSnapshot:
    conn = _connect(db_path)
    try:
        visits = _rows(
            conn,
            """
            select
              v.visit_id,
              v.asset_id,
              v.visit_date_utc,
              v.review_status,
              v.certification_status,
              v.inspector_name,
              v.root_hash_sha256,
              v.manifest_hash_sha256,
              a.asset_public_id,
              a.asset_template_type,
              a.asset_type,
              a.asset_city,
              a.client_name
            from visits v
            left join assets a on a.asset_id = v.asset_id
            order by v.visit_id
            """,
        )
        assets = _rows(
            conn,
            """
            select
              asset_id,
              asset_public_id,
              asset_template_type,
              asset_type,
              asset_name,
              asset_city,
              address_line,
              province,
              postal_code,
              client_name,
              entity_legal_name,
              gps_lat,
              gps_lon
            from assets
            order by asset_id
            """,
        )
        observations = _rows(
            conn,
            """
            select
              record_uuid,
              asset_id,
              visit_id,
              lpi_code,
              severity_0_5,
              observation_description,
              coordinator_notes,
              row_status,
              review_status,
              certification_status,
              min_photos_required,
              min_docs_required,
              out_of_scope_flag,
              out_of_scope_reason,
              captured_at_utc
            from observations
            order by visit_id, record_uuid
            """,
        )
        photos = _rows(
            conn,
            """
            select
              photo_uuid,
              record_uuid,
              asset_id,
              visit_id,
              lpi_code,
              photo_role,
              photo_hash_sha256,
              photo_filename,
              photo_relpath,
              photo_path,
              quality_flags,
              captured_at_utc
            from photos
            order by visit_id, photo_uuid
            """,
        )
        lpi_dictionary = _rows(
            conn,
            """
            select
              lpi_code,
              coalesce(lpi_display, lpi_label, lpi_name, label, lpi_code) as lpi_title,
              asset_template_type,
              category,
              lpi_group,
              version
            from lpi_dictionary
            where coalesce(lpi_code, '') <> ''
            order by lpi_code
            """,
        )
    finally:
        conn.close()
    for asset in assets:
        if not _clean_text(asset.get("asset_type")):
            asset["asset_type"] = _clean_text(asset.get("asset_template_type"))
        if not _clean_text(asset.get("asset_name")):
            asset["asset_name"] = _clean_text(asset.get("asset_public_id")) or _clean_text(asset.get("asset_id"))
    return RealEstateSnapshot(
        visits=visits,
        assets=assets,
        observations=observations,
        photos=photos,
        lpi_dictionary=lpi_dictionary,
    )


def build_real_estate_workspace(snapshot: RealEstateSnapshot, visit_id: str) -> dict[str, Any] | None:
    visit = next((item for item in snapshot.visits if item.get("visit_id") == visit_id), None)
    if not visit:
        return None

    asset = next((item for item in snapshot.assets if item.get("asset_id") == visit.get("asset_id")), None)
    observations = [item for item in snapshot.observations if item.get("visit_id") == visit_id]
    photos = [item for item in snapshot.photos if item.get("visit_id") == visit_id]
    lpi_by_code = {str(item.get("lpi_code") or ""): item for item in snapshot.lpi_dictionary}

    for obs in observations:
        lpi_code = _clean_text(obs.get("lpi_code"))
        obs["lpi_title"] = _clean_text(lpi_by_code.get(lpi_code, {}).get("lpi_title"))
        obs["severity_0_5"] = int(obs.get("severity_0_5") or 0)
        obs["min_photos_required"] = int(obs.get("min_photos_required") or (3 if obs["severity_0_5"] >= 3 else 1))
        obs["out_of_scope_flag"] = int(obs.get("out_of_scope_flag") or 0)

    photo_counts: dict[str, int] = {}
    for photo in photos:
        key = _clean_text(photo.get("record_uuid"))
        photo_counts[key] = photo_counts.get(key, 0) + 1

    total_required = sum(int(item.get("min_photos_required") or 0) for item in observations)
    total_present = len(photos)
    all_observations_have_lpi = all(bool(_clean_text(item.get("lpi_code"))) for item in observations) if observations else False
    root_ready = bool(_clean_text(visit.get("root_hash_sha256")))
    issuance_ready = (
        bool(observations)
        and bool(photos)
        and all_observations_have_lpi
        and total_present >= total_required
    )

    return {
        "visit": visit,
        "asset": asset or {},
        "observations": observations,
        "photos": photos,
        "photo_counts_by_record": photo_counts,
        "total_required_photos": total_required,
        "total_present_photos": total_present,
        "all_observations_have_lpi": all_observations_have_lpi,
        "issuance_ready": issuance_ready,
        "already_issued": root_ready,
        "lpi_dictionary_size": len(snapshot.lpi_dictionary),
    }


def build_real_estate_end_to_end_preview(snapshot: RealEstateSnapshot, visit_id: str) -> dict[str, Any] | None:
    visit = next((item for item in snapshot.visits if item.get("visit_id") == visit_id), None)
    if not visit:
        return None

    obs = [item for item in snapshot.observations if item.get("visit_id") == visit_id]
    photos = [item for item in snapshot.photos if item.get("visit_id") == visit_id]
    risk_score = sum(int(item.get("severity_0_5") or 0) for item in obs)
    if risk_score >= 12:
        risk_category = "high"
    elif risk_score >= 6:
        risk_category = "medium"
    else:
        risk_category = "low"

    validation_checks = {
        "has_asset_public_id": bool(visit.get("asset_public_id")),
        "has_visit_date": bool(visit.get("visit_date_utc")),
        "has_observations": len(obs) > 0,
        "has_photos": len(photos) > 0,
        "all_observations_have_lpi": all(bool(item.get("lpi_code")) for item in obs),
    }
    validation_ok = all(validation_checks.values())

    root_source = "|".join(
        [
            str(visit.get("visit_id") or ""),
            str(visit.get("asset_id") or ""),
            str(len(obs)),
            str(len(photos)),
            str(risk_score),
        ]
    )
    root_hash = hashlib.sha256(root_source.encode("utf-8")).hexdigest()
    manifest_hash = hashlib.sha256((root_hash + "|manifest").encode("utf-8")).hexdigest()
    pdf_hash = hashlib.sha256((root_hash + "|certificate").encode("utf-8")).hexdigest()
    issued_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "visit": visit,
        "validation_checks": validation_checks,
        "validation_ok": validation_ok,
        "observation_count": len(obs),
        "photo_count": len(photos),
        "risk_score": risk_score,
        "risk_category": risk_category,
        "certificate_preview": {
            "global_sequence_id": f"RE-SBX-{visit_id}",
            "asset_id": visit.get("asset_id"),
            "asset_public_id": visit.get("asset_public_id"),
            "visit_id": visit.get("visit_id"),
            "visit_date_utc": visit.get("visit_date_utc"),
            "issued_at_utc": issued_at,
            "issued_entity_id": "HREVN_SANDBOX",
            "root_hash_sha256": root_hash,
            "pdf_hash_sha256": pdf_hash,
            "pvm_version": "sandbox_preview_v1",
            "verification_url": "streamlit-cloud-preview",
        },
        "visit_report_preview": {
            "asset_public_id": visit.get("asset_public_id"),
            "visit_id": visit.get("visit_id"),
            "inspection_date_utc": visit.get("visit_date_utc"),
            "client_name": visit.get("client_name"),
            "inspector_name": visit.get("inspector_name") or "N/A",
            "location": visit.get("asset_city") or "N/A",
            "root_hash_sha256": root_hash,
            "manifest_hash_sha256": manifest_hash,
            "risk_score": risk_score,
            "risk_category": risk_category,
        },
        "observations_preview": obs,
        "photos_preview": photos,
    }
