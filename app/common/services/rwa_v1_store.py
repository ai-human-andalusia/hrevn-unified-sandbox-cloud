from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .rwa_v1_schema import DEFAULT_DB_PATH, ensure_rwa_v1_schema


RWA_ASSET_CATEGORIES = (
    "residential",
    "tertiary",
    "industrial",
    "urban_land",
    "rural_land",
)

_CAPTURE_TIMEOUT_MINUTES = 10


def _now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_utc() -> str:
    return _iso(_now_dt())


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).astimezone(timezone.utc)
    except ValueError:
        return None


def _connect(db_path: Path | None = None) -> sqlite3.Connection:
    target = ensure_rwa_v1_schema(db_path)
    conn = sqlite3.connect(target)
    conn.row_factory = sqlite3.Row
    return conn


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _ensure_visit_capture_columns(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(rwa_visits)").fetchall()}
    wanted = {
        "direct_capture_session_status": "TEXT NOT NULL DEFAULT 'open'",
        "direct_capture_started_at_utc": "TEXT",
        "direct_capture_last_activity_at_utc": "TEXT",
        "direct_capture_closed_at_utc": "TEXT",
        "direct_capture_closed_reason": "TEXT",
        "direct_capture_window_minutes": "INTEGER NOT NULL DEFAULT 0",
    }
    for name, ddl in wanted.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE rwa_visits ADD COLUMN {name} {ddl}")


def ensure_rwa_capture_schema(db_path: Path | None = None) -> Path:
    target = ensure_rwa_v1_schema(db_path)
    with sqlite3.connect(target) as conn:
        _ensure_visit_capture_columns(conn)
        conn.commit()
    return target


def ensure_rwa_v1_demo_seed(db_path: Path | None = None) -> None:
    target = ensure_rwa_capture_schema(db_path)
    now = _now_utc()
    with _connect(target) as conn:
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
    target = ensure_rwa_capture_schema(db_path)
    with _connect(target) as conn:
        rows = conn.execute(
            """
            SELECT asset_id, asset_public_id, asset_type, asset_name, asset_status, asset_data_json, created_at_utc
            FROM rwa_assets
            ORDER BY created_at_utc DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]


def list_rwa_v1_visits_raw(db_path: Path | None = None) -> list[dict]:
    target = ensure_rwa_capture_schema(db_path)
    with _connect(target) as conn:
        rows = conn.execute(
            """
            SELECT visit_id, asset_id, created_by_account_id, visit_date_utc, visit_status, review_status,
                   issuance_status, direct_capture_session_status, direct_capture_started_at_utc,
                   direct_capture_last_activity_at_utc, direct_capture_closed_at_utc,
                   direct_capture_closed_reason, direct_capture_window_minutes, visit_data_json,
                   created_at_utc, updated_at_utc
            FROM rwa_visits
            ORDER BY created_at_utc DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]


def list_rwa_v1_observations_raw(db_path: Path | None = None) -> list[dict]:
    target = ensure_rwa_capture_schema(db_path)
    with _connect(target) as conn:
        rows = conn.execute(
            """
            SELECT observation_id, visit_id, asset_id, lpi_code, severity_0_5, observation_description, row_status, observation_data_json, created_at_utc
            FROM rwa_observations
            ORDER BY created_at_utc DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]


def list_rwa_v1_photos_raw(db_path: Path | None = None) -> list[dict]:
    target = ensure_rwa_capture_schema(db_path)
    with _connect(target) as conn:
        rows = conn.execute(
            """
            SELECT photo_id, visit_id, asset_id, observation_id, photo_filename, photo_hash_sha256, ingest_mode, photo_status, captured_at_utc, added_to_record_at_utc, photo_data_json
            FROM rwa_photos
            ORDER BY added_to_record_at_utc DESC, captured_at_utc DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]


def list_rwa_v1_attachments_raw(db_path: Path | None = None) -> list[dict]:
    target = ensure_rwa_capture_schema(db_path)
    with _connect(target) as conn:
        rows = conn.execute(
            """
            SELECT attachment_id, visit_id, asset_id, observation_id, attachment_filename, attachment_hash_sha256,
                   attachment_kind, attachment_status, added_to_record_at_utc, attachment_data_json
            FROM rwa_attachments
            ORDER BY added_to_record_at_utc DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]


def create_rwa_v1_visit(*, asset_id: str, visit_id: str, visit_date_utc: str | None = None, visit_data: dict | None = None, db_path: Path | None = None) -> str:
    target = ensure_rwa_capture_schema(db_path)
    now = _now_utc()
    with _connect(target) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO rwa_visits (
              visit_id, asset_id, created_by_account_id, visit_date_utc, visit_status, review_status,
              issuance_status, direct_capture_session_status, direct_capture_started_at_utc,
              direct_capture_last_activity_at_utc, direct_capture_closed_at_utc,
              direct_capture_closed_reason, direct_capture_window_minutes,
              visit_data_json, created_at_utc, updated_at_utc
            ) VALUES (?, ?, NULL, ?, 'work', 'pending', 'not_issued', 'open', NULL, NULL, NULL, NULL, 0, ?, ?, ?)
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


def _visit_photo_metrics(conn: sqlite3.Connection, visit_id: str, ingest_mode: str | None = None) -> tuple[datetime | None, datetime | None, int]:
    if ingest_mode:
        rows = conn.execute(
            "SELECT captured_at_utc, added_to_record_at_utc FROM rwa_photos WHERE visit_id = ? AND ingest_mode = ? ORDER BY COALESCE(captured_at_utc, added_to_record_at_utc) ASC",
            (visit_id, ingest_mode),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT captured_at_utc, added_to_record_at_utc FROM rwa_photos WHERE visit_id = ? ORDER BY COALESCE(captured_at_utc, added_to_record_at_utc) ASC",
            (visit_id,),
        ).fetchall()
    timestamps = []
    for row in rows:
        dt = _parse_iso(row[0] or row[1])
        if dt:
            timestamps.append(dt)
    if not timestamps:
        return None, None, 0
    return timestamps[0], timestamps[-1], len(timestamps)


def refresh_rwa_v1_capture_session(visit_id: str, *, timeout_minutes: int = _CAPTURE_TIMEOUT_MINUTES, db_path: Path | None = None) -> dict | None:
    target = ensure_rwa_capture_schema(db_path)
    with _connect(target) as conn:
        row = conn.execute("SELECT * FROM rwa_visits WHERE visit_id = ?", (visit_id,)).fetchone()
        if not row:
            return None
        visit = dict(row)
        status = str(visit.get("direct_capture_session_status") or "open")
        if status != "open":
            return visit
        last_activity = _parse_iso(visit.get("direct_capture_last_activity_at_utc"))
        if not last_activity:
            return visit
        now = _now_dt()
        if now < last_activity + timedelta(minutes=timeout_minutes):
            return visit
        started, ended, _count = _visit_photo_metrics(conn, visit_id, "direct_capture")
        window_minutes = 0
        if started and ended:
            window_minutes = max(0, int((ended - started).total_seconds() // 60))
        now_iso = _iso(now)
        conn.execute(
            """
            UPDATE rwa_visits
            SET direct_capture_session_status = 'closed',
                direct_capture_closed_at_utc = ?,
                direct_capture_closed_reason = 'capture_timeout',
                direct_capture_window_minutes = ?,
                updated_at_utc = ?
            WHERE visit_id = ?
            """,
            (now_iso, window_minutes, now_iso, visit_id),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM rwa_visits WHERE visit_id = ?", (visit_id,)).fetchone()
        return dict(row) if row else None


def create_rwa_v1_observation(*, observation_id: str, visit_id: str, asset_id: str, lpi_code: str, severity_0_5: int, observation_description: str, coordinator_notes: str, file_entries: list[dict], db_path: Path | None = None) -> str:
    target = ensure_rwa_capture_schema(db_path)
    now_dt = _now_dt()
    now = _iso(now_dt)
    with _connect(target) as conn:
        visit_row = conn.execute("SELECT * FROM rwa_visits WHERE visit_id = ?", (visit_id,)).fetchone()
        if not visit_row:
            raise ValueError(f"Visit not found: {visit_id}")
        visit = dict(visit_row)
        normalized_entries = []
        for entry in file_entries:
            ingest_mode = str(entry.get('ingest_mode') or 'manual_upload')
            if ingest_mode == 'direct_capture' and str(visit.get('direct_capture_session_status') or 'open') != 'open':
                ingest_mode = 'manual_upload'
            normalized_entries.append({
                'filename': str(entry.get('filename') or ''),
                'payload': entry.get('payload') or b'',
                'mime': str(entry.get('mime') or ''),
                'ingest_mode': ingest_mode,
                'captured_at_utc': entry.get('captured_at_utc') or (now if ingest_mode == 'direct_capture' else None),
            })
        has_direct_capture = any(item['ingest_mode'] == 'direct_capture' for item in normalized_entries)
        has_manual_uploads = any(item['ingest_mode'] == 'manual_upload' for item in normalized_entries)
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
                json.dumps({"coordinator_notes": coordinator_notes.strip(), "has_direct_capture": has_direct_capture, "has_manual_uploads": has_manual_uploads}),
                now,
                now,
            ),
        )
        for item in normalized_entries:
            payload = item['payload']
            conn.execute(
                """
                INSERT INTO rwa_photos (
                  photo_id, visit_id, asset_id, observation_id, photo_filename, photo_hash_sha256,
                  ingest_mode, photo_status, captured_at_utc, added_to_record_at_utc, photo_data_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)
                """,
                (
                    _new_id("RWAP"),
                    visit_id,
                    asset_id,
                    observation_id,
                    item['filename'],
                    _sha256_bytes(payload),
                    item['ingest_mode'],
                    item['captured_at_utc'],
                    now,
                    json.dumps({"size_bytes": len(payload), "mime": item['mime'], "upload_mode": item['ingest_mode']}),
                ),
            )
        if has_direct_capture:
            started, ended, _count = _visit_photo_metrics(conn, visit_id, 'direct_capture')
            started_iso = _iso(started) if started else now
            last_iso = _iso(ended) if ended else now
            window_minutes = max(0, int((ended - started).total_seconds() // 60)) if started and ended else 0
            conn.execute(
                """
                UPDATE rwa_visits
                SET direct_capture_session_status = 'open',
                    direct_capture_started_at_utc = COALESCE(direct_capture_started_at_utc, ?),
                    direct_capture_last_activity_at_utc = ?,
                    direct_capture_window_minutes = ?,
                    updated_at_utc = ?
                WHERE visit_id = ?
                """,
                (started_iso, last_iso, window_minutes, now, visit_id),
            )
        if has_manual_uploads:
            conn.execute(
                """
                UPDATE rwa_visits
                SET updated_at_utc = ?,
                    visit_data_json = json_patch(COALESCE(visit_data_json, '{}'), json(?))
                WHERE visit_id = ?
                """,
                (now, json.dumps({"has_manual_uploads": True}), visit_id),
            )
        conn.commit()
    return observation_id


def finalize_rwa_v1_capture_session(visit_id: str, *, db_path: Path | None = None) -> dict | None:
    target = ensure_rwa_capture_schema(db_path)
    now = _now_dt()
    now_iso = _iso(now)
    with _connect(target) as conn:
        row = conn.execute("SELECT * FROM rwa_visits WHERE visit_id = ?", (visit_id,)).fetchone()
        if not row:
            return None
        visit = dict(row)
        started, ended, _count = _visit_photo_metrics(conn, visit_id, "direct_capture")
        window_minutes = 0
        if started and ended:
            window_minutes = max(0, int((ended - started).total_seconds() // 60))
        conn.execute(
            """
            UPDATE rwa_visits
            SET direct_capture_session_status = 'closed',
                direct_capture_closed_at_utc = COALESCE(direct_capture_closed_at_utc, ?),
                direct_capture_closed_reason = COALESCE(direct_capture_closed_reason, 'manual_finish'),
                direct_capture_window_minutes = ?,
                visit_status = CASE
                  WHEN visit_status IN ('issued', 'closed') THEN visit_status
                  ELSE 'pending_validation'
                END,
                updated_at_utc = ?
            WHERE visit_id = ?
            """,
            (now_iso, window_minutes, now_iso, visit_id),
        )
        conn.commit()
        updated = conn.execute("SELECT * FROM rwa_visits WHERE visit_id = ?", (visit_id,)).fetchone()
        return dict(updated) if updated else None


def attach_rwa_v1_files_to_visit(*, visit_id: str, uploaded_files: list, pre_issue_comments: str = "", db_path: Path | None = None) -> int:
    target = ensure_rwa_capture_schema(db_path)
    now = _now_utc()
    inserted = 0
    with _connect(target) as conn:
        visit_row = conn.execute("SELECT * FROM rwa_visits WHERE visit_id = ?", (visit_id,)).fetchone()
        if not visit_row:
            raise ValueError(f"Visit not found: {visit_id}")
        visit = dict(visit_row)
        asset_id = str(visit.get("asset_id") or "")
        existing_photo_names = {
            str(row[0] or "").strip().lower()
            for row in conn.execute(
                "SELECT photo_filename FROM rwa_photos WHERE visit_id = ?",
                (visit_id,),
            ).fetchall()
        }
        existing_attachment_names = {
            str(row[0] or "").strip().lower()
            for row in conn.execute(
                "SELECT attachment_filename FROM rwa_attachments WHERE visit_id = ?",
                (visit_id,),
            ).fetchall()
        }
        seen_names: set[str] = set()
        for uploaded in uploaded_files:
            filename = str(getattr(uploaded, "name", "") or "").strip()
            payload = uploaded.getvalue()
            mime = str(getattr(uploaded, "type", "") or "")
            lowered = filename.lower()
            if not filename:
                continue
            if lowered in seen_names or lowered in existing_photo_names or lowered in existing_attachment_names:
                continue
            seen_names.add(lowered)
            if filename.lower().endswith((".jpg", ".jpeg", ".png", ".heic", ".heif", ".webp", ".bmp", ".tif", ".tiff")):
                conn.execute(
                    """
                    INSERT INTO rwa_photos (
                      photo_id, visit_id, asset_id, observation_id, photo_filename, photo_hash_sha256,
                      ingest_mode, photo_status, captured_at_utc, added_to_record_at_utc, photo_data_json
                    ) VALUES (?, ?, ?, NULL, ?, ?, 'manual_upload', 'active', NULL, ?, ?)
                    """,
                    (
                        _new_id("RWAP"),
                        visit_id,
                        asset_id,
                        filename,
                        _sha256_bytes(payload),
                        now,
                        json.dumps({
                            "size_bytes": len(payload),
                            "mime": mime,
                            "upload_mode": "manual_upload",
                            "review_comment": pre_issue_comments.strip(),
                        }),
                    ),
                )
            else:
                attachment_kind = "pdf" if filename.lower().endswith(".pdf") else "document"
                conn.execute(
                    """
                    INSERT INTO rwa_attachments (
                      attachment_id, visit_id, asset_id, observation_id, attachment_filename, attachment_hash_sha256,
                      attachment_kind, attachment_status, added_to_record_at_utc, attachment_data_json
                    ) VALUES (?, ?, ?, NULL, ?, ?, ?, 'active', ?, ?)
                    """,
                    (
                        _new_id("RWAT"),
                        visit_id,
                        asset_id,
                        filename,
                        _sha256_bytes(payload),
                        attachment_kind,
                        now,
                        json.dumps({
                            "size_bytes": len(payload),
                            "mime": mime,
                            "added_mode": "manual_upload",
                            "review_comment": pre_issue_comments.strip(),
                        }),
                    ),
                )
            inserted += 1
        patch_payload = {
            "pre_issue_comments": pre_issue_comments.strip(),
            "has_manual_additions": bool(inserted),
            "review_last_saved_at_utc": now,
        }
        conn.execute(
            """
            UPDATE rwa_visits
            SET updated_at_utc = ?,
                visit_status = CASE
                  WHEN visit_status IN ('issued', 'closed') THEN visit_status
                  ELSE 'pending_validation'
                END,
                visit_data_json = json_patch(COALESCE(visit_data_json, '{}'), json(?))
            WHERE visit_id = ?
            """,
            (now, json.dumps(patch_payload), visit_id),
        )
        conn.commit()
    return inserted


def remove_rwa_v1_review_artifact(*, visit_id: str, artifact_kind: str, artifact_id: str, db_path: Path | None = None) -> bool:
    target = ensure_rwa_capture_schema(db_path)
    now = _now_utc()
    with _connect(target) as conn:
        visit_row = conn.execute("SELECT issuance_status FROM rwa_visits WHERE visit_id = ?", (visit_id,)).fetchone()
        if not visit_row:
            return False
        if str(visit_row[0] or "") == "issued":
            return False
        deleted = 0
        if artifact_kind == "photo":
            row = conn.execute(
                """
                SELECT ingest_mode FROM rwa_photos
                WHERE photo_id = ? AND visit_id = ?
                """,
                (artifact_id, visit_id),
            ).fetchone()
            if row and str(row[0] or "") == "manual_upload":
                deleted = conn.execute(
                    "DELETE FROM rwa_photos WHERE photo_id = ? AND visit_id = ?",
                    (artifact_id, visit_id),
                ).rowcount
        elif artifact_kind == "attachment":
            deleted = conn.execute(
                "DELETE FROM rwa_attachments WHERE attachment_id = ? AND visit_id = ?",
                (artifact_id, visit_id),
            ).rowcount
        if deleted:
            conn.execute(
                "UPDATE rwa_visits SET updated_at_utc = ? WHERE visit_id = ?",
                (now, visit_id),
            )
            conn.commit()
        return bool(deleted)


def replace_rwa_v1_review_artifact(
    *,
    visit_id: str,
    artifact_kind: str,
    artifact_id: str,
    replacement_file,
    db_path: Path | None = None,
) -> bool:
    target = ensure_rwa_capture_schema(db_path)
    now = _now_utc()
    with _connect(target) as conn:
        visit_row = conn.execute("SELECT issuance_status, asset_id FROM rwa_visits WHERE visit_id = ?", (visit_id,)).fetchone()
        if not visit_row:
            return False
        if str(visit_row[0] or "") == "issued":
            return False
        asset_id = str(visit_row[1] or "")
        filename = str(getattr(replacement_file, "name", "") or "").strip()
        payload = replacement_file.getvalue()
        mime = str(getattr(replacement_file, "type", "") or "")
        if not filename:
            return False
        lowered = filename.lower()
        existing_photo_names = {
            str(row[0] or "").strip().lower()
            for row in conn.execute(
                "SELECT photo_filename FROM rwa_photos WHERE visit_id = ? AND photo_id != ?",
                (visit_id, artifact_id if artifact_kind == "photo" else ""),
            ).fetchall()
        }
        existing_attachment_names = {
            str(row[0] or "").strip().lower()
            for row in conn.execute(
                "SELECT attachment_filename FROM rwa_attachments WHERE visit_id = ? AND attachment_id != ?",
                (visit_id, artifact_id if artifact_kind == "attachment" else ""),
            ).fetchall()
        }
        if lowered in existing_photo_names or lowered in existing_attachment_names:
            return False
        if artifact_kind == "photo":
            row = conn.execute(
                "SELECT ingest_mode FROM rwa_photos WHERE photo_id = ? AND visit_id = ?",
                (artifact_id, visit_id),
            ).fetchone()
            if not row or str(row[0] or "") != "manual_upload":
                return False
            conn.execute(
                """
                UPDATE rwa_photos
                SET photo_filename = ?,
                    photo_hash_sha256 = ?,
                    added_to_record_at_utc = ?,
                    photo_data_json = ?
                WHERE photo_id = ? AND visit_id = ?
                """,
                (
                    filename,
                    _sha256_bytes(payload),
                    now,
                    json.dumps({"size_bytes": len(payload), "mime": mime, "upload_mode": "manual_upload", "replaced_in_review": True}),
                    artifact_id,
                    visit_id,
                ),
            )
        elif artifact_kind == "attachment":
            attachment_kind = "pdf" if filename.lower().endswith(".pdf") else "document"
            conn.execute(
                """
                UPDATE rwa_attachments
                SET attachment_filename = ?,
                    attachment_hash_sha256 = ?,
                    attachment_kind = ?,
                    added_to_record_at_utc = ?,
                    attachment_data_json = ?
                WHERE attachment_id = ? AND visit_id = ?
                """,
                (
                    filename,
                    _sha256_bytes(payload),
                    attachment_kind,
                    now,
                    json.dumps({"size_bytes": len(payload), "mime": mime, "added_mode": "manual_upload", "replaced_in_review": True}),
                    artifact_id,
                    visit_id,
                ),
            )
        else:
            return False
        conn.execute("UPDATE rwa_visits SET updated_at_utc = ? WHERE visit_id = ?", (now, visit_id))
        conn.commit()
        return True


def validate_and_issue_rwa_v1_visit(*, visit_id: str, pre_issue_comments: str = "", db_path: Path | None = None) -> dict | None:
    target = ensure_rwa_capture_schema(db_path)
    now = _now_utc()
    with _connect(target) as conn:
        visit_row = conn.execute("SELECT * FROM rwa_visits WHERE visit_id = ?", (visit_id,)).fetchone()
        if not visit_row:
            raise ValueError(f"Visit not found: {visit_id}")
        visit = dict(visit_row)
        asset_id = str(visit.get("asset_id") or "")
        issuance_id = _new_id("RWAI")
        observations = conn.execute(
            "SELECT observation_id FROM rwa_observations WHERE visit_id = ?",
            (visit_id,),
        ).fetchall()
        photos = conn.execute(
            "SELECT photo_filename, photo_hash_sha256 FROM rwa_photos WHERE visit_id = ? ORDER BY added_to_record_at_utc, captured_at_utc",
            (visit_id,),
        ).fetchall()
        attachments = conn.execute(
            "SELECT attachment_filename, attachment_hash_sha256 FROM rwa_attachments WHERE visit_id = ? ORDER BY added_to_record_at_utc",
            (visit_id,),
        ).fetchall()
        manifest_payload = {
            "visit_id": visit_id,
            "asset_id": asset_id,
            "issued_at_utc": now,
            "observation_count": len(observations),
            "photo_count": len(photos),
            "attachment_count": len(attachments),
            "pre_issue_comments": pre_issue_comments.strip(),
        }
        manifest_hash = hashlib.sha256(json.dumps(manifest_payload, sort_keys=True).encode("utf-8")).hexdigest()
        root_material = "|".join(
            [visit_id, asset_id, manifest_hash]
            + [f"{row[0]}:{row[1]}" for row in photos]
            + [f"{row[0]}:{row[1]}" for row in attachments]
        )
        root_hash = hashlib.sha256(root_material.encode("utf-8")).hexdigest()
        conn.execute(
            """
            INSERT INTO rwa_issuances (
              issuance_id, visit_id, asset_id, certificate_status, zip_status, issued_at_utc,
              root_hash_sha256, manifest_hash_sha256, issuance_data_json
            ) VALUES (?, ?, ?, 'issued', 'issued', ?, ?, ?, ?)
            """,
            (
                issuance_id,
                visit_id,
                asset_id,
                now,
                root_hash,
                manifest_hash,
                json.dumps({"pre_issue_comments": pre_issue_comments.strip(), "issued_from": "rwa_review_panel"}),
            ),
        )
        conn.execute(
            """
            UPDATE rwa_visits
            SET visit_status = 'issued',
                review_status = 'validated',
                issuance_status = 'issued',
                updated_at_utc = ?,
                visit_data_json = json_patch(COALESCE(visit_data_json, '{}'), json(?))
            WHERE visit_id = ?
            """,
            (
                now,
                json.dumps({"pre_issue_comments": pre_issue_comments.strip(), "issued_at_utc": now}),
                visit_id,
            ),
        )
        conn.commit()
        issued = conn.execute("SELECT * FROM rwa_issuances WHERE issuance_id = ?", (issuance_id,)).fetchone()
        return dict(issued) if issued else None
