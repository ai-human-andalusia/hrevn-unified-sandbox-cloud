"""AER package generation for Agent Operations demo."""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from datetime import datetime, timezone
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False).encode("utf-8")


def _normalize_state(record: dict[str, Any]) -> dict[str, str]:
    status = str(record.get("status") or "pending_review")
    human_action = str(record.get("human_action") or "pending")

    if status == "executed_sealed" or human_action == "approved":
        return {
            "human_approval_status": "approved",
            "status": "executed_sealed",
            "execution_result": "executed_after_human_authorization",
            "seal_status": "sealed",
        }
    if status == "rejected" or human_action == "rejected":
        return {
            "human_approval_status": "rejected",
            "status": "rejected",
            "execution_result": "blocked_by_human_rejection",
            "seal_status": "sealed_rejection",
        }
    return {
        "human_approval_status": "pending",
        "status": "pending_review",
        "execution_result": "awaiting_human_decision",
        "seal_status": "not_sealed",
    }


def _full_seal_reference(record: dict[str, Any], normalized: dict[str, str], packaged_at_utc: str) -> str:
    raw = "|".join(
        [
            str(record.get("record_id") or ""),
            str(record.get("agent_id") or ""),
            str(record.get("intent") or ""),
            str(record.get("tool_name") or ""),
            str(record.get("submitted_at_utc") or ""),
            normalized["human_approval_status"],
            str(record.get("reviewer_name") or ""),
            str(record.get("reviewed_at_utc") or ""),
            str(record.get("decision_rationale") or ""),
            packaged_at_utc,
        ]
    )
    return f"sha256:{hashlib.sha256(raw.encode('utf-8')).hexdigest()}"


def _build_report_pdf(record: dict[str, Any], operation_record: dict[str, Any], approval_record: dict[str, Any], execution_record: dict[str, Any]) -> bytes:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    _, height = A4
    y = height - 48

    def line(text: str, gap: int = 16, font: str = "Helvetica", size: int = 10) -> None:
        nonlocal y
        pdf.setFont(font, size)
        pdf.drawString(44, y, text[:120])
        y -= gap

    line("H-REVN AER — Verifiable Execution Record", 20, "Helvetica-Bold", 14)
    line("Package Family: H-REVN AER", 14)
    line("Bundle Profile: agent_operation_aer_v1", 14)
    line("Verification Model: ROOT_AER_V1", 14)
    line(f"AER ID: {operation_record['aer_id']}")
    line(f"Record ID: {record['record_id']}")
    line(f"Workflow ID: {record['workflow_id']}")
    line(f"Agent: {record['agent_name']} ({record['agent_role']})")
    line(f"Operation: {record['intent']}")
    line(f"Tool: {record['tool_name']}")
    line(f"Proposed At: {operation_record['proposed_at_utc']}")
    line(f"Packaged At: {operation_record['packaged_at_utc']}")
    line(f"Risk Level: {record['risk_level']}")
    line(f"Approval Policy: {record['approval_policy']}")
    line(f"Approval Status: {approval_record['human_approval_status']}")
    line(f"Reviewer: {approval_record['reviewer_name']} / {approval_record['reviewer_role']}")
    line(f"Execution Result: {execution_record['execution_result']}")
    line(f"Seal Reference: {execution_record['seal_reference'][:48]}...")
    line("Decision Rationale:", 18, "Helvetica-Bold", 10)
    rationale = approval_record["decision_rationale"] or "No rationale recorded."
    chunks = [rationale[i:i + 105] for i in range(0, len(rationale), 105)] or ["No rationale recorded."]
    for chunk in chunks:
        line(chunk, 14)
    y -= 8
    line("Operation Parameters:", 18, "Helvetica-Bold", 10)
    for item in record.get("parameters", []):
        line(f"- {item['field']}: {item['value']} ({item['type']})", 14)
    pdf.showPage()
    pdf.save()
    return buffer.getvalue()


def build_agent_operation_aer_package(record: dict[str, Any]) -> dict[str, Any]:
    packaged_at_utc = _utc_now()
    aer_id = f"AER-{record['record_id']}"
    normalized = _normalize_state(record)
    full_seal_reference = _full_seal_reference(record, normalized, packaged_at_utc)
    package_delivery_id = f"{record['record_id']}_{packaged_at_utc.replace(':', '').replace('-', '').replace('T', '_').replace('Z', '')}"

    operation_record = {
        "aer_id": aer_id,
        "event_id": record["record_id"],
        "proposed_at_utc": record.get("submitted_at_utc") or "",
        "packaged_at_utc": packaged_at_utc,
        "workflow_id": record.get("workflow_id") or f"WF-{record['record_id']}",
        "agent_id": record.get("agent_id") or "",
        "agent_name": record.get("agent_name") or "",
        "agent_role": record.get("agent_role") or "",
        "action_type": record.get("operation_type") or "",
        "tool_name": record.get("tool_name") or "",
        "proposed_action": record.get("intent") or "",
        "parameters": record.get("parameters", []),
        "human_approval_required": bool(record.get("human_approval_required", True)),
        "version": record.get("aer_version") or "aer_demo_v1",
    }
    approval_record = {
        "aer_id": aer_id,
        "record_id": record["record_id"],
        "human_approval_status": normalized["human_approval_status"],
        "reviewer_name": record.get("reviewer_name") or "Pending reviewer",
        "reviewer_role": record.get("reviewer_role") or "Pending reviewer role",
        "reviewed_at_utc": record.get("reviewed_at_utc") or "",
        "decision_rationale": record.get("decision_rationale") or "No rationale recorded.",
        "approval_policy": record.get("approval_policy") or "",
    }
    execution_record = {
        "aer_id": aer_id,
        "record_id": record["record_id"],
        "execution_result": normalized["execution_result"],
        "seal_status": normalized["seal_status"],
        "seal_reference": full_seal_reference,
        "seal_reference_display": full_seal_reference[:31] + "...",
        "status": normalized["status"],
        "generated_at_utc": packaged_at_utc,
    }

    artifacts: dict[str, bytes] = {
        "operation_record.json": _json_bytes(operation_record),
        "approval_record.json": _json_bytes(approval_record),
        "execution_record.json": _json_bytes(execution_record),
    }
    artifacts["agent_operation_review_report.pdf"] = _build_report_pdf(record, operation_record, approval_record, execution_record)

    artifact_catalog = [
        {"artifact": "operation_record.json", "category": "core", "role": "structured_operation_record"},
        {"artifact": "approval_record.json", "category": "core", "role": "human_approval_record"},
        {"artifact": "execution_record.json", "category": "core", "role": "execution_outcome_record"},
        {"artifact": "agent_operation_review_report.pdf", "category": "core", "role": "human_readable_review_report"},
        {"artifact": "manifest.json", "category": "control", "role": "package_manifest"},
        {"artifact": "PROTOCOL_PROFILE.txt", "category": "support", "role": "package_profile_summary"},
        {"artifact": "VERIFICATION.txt", "category": "support", "role": "verification_guidance_summary"},
        {"artifact": "HOW_TO_VERIFY_THIS_AER_PACKAGE.txt", "category": "support", "role": "step_by_step_verification_guide"},
        {"artifact": "CHECKSUMS.sha256", "category": "verification", "role": "artifact_hash_list"},
        {"artifact": "ROOT_HASH_SHA256.txt", "category": "verification", "role": "package_root_hash"},
        {"artifact": "ROOT_SPEC_AER_V1.txt", "category": "verification", "role": "root_hash_rule"},
    ]
    manifest = {
        "aer_id": aer_id,
        "record_id": record["record_id"],
        "generated_at_utc": packaged_at_utc,
        "workflow_id": operation_record["workflow_id"],
        "package_type": "agent_operation_aer_demo_v1",
        "package_family": "hrevn_aer",
        "bundle_profile": "agent_operation_aer_v1",
        "verification_model": "ROOT_AER_V1",
        "external_anchor_status": "not_anchored",
        "artifact_count": len(artifact_catalog),
        "artifacts": artifact_catalog,
        "authoritative_files": [
            "operation_record.json",
            "approval_record.json",
            "execution_record.json",
            "agent_operation_review_report.pdf",
            "manifest.json",
            "CHECKSUMS.sha256",
            "ROOT_HASH_SHA256.txt",
            "ROOT_SPEC_AER_V1.txt",
        ],
        "supporting_files": [
            "PROTOCOL_PROFILE.txt",
            "VERIFICATION.txt",
            "HOW_TO_VERIFY_THIS_AER_PACKAGE.txt",
        ],
        "checksum_scope": [
            "operation_record.json",
            "approval_record.json",
            "execution_record.json",
            "agent_operation_review_report.pdf",
            "manifest.json",
            "PROTOCOL_PROFILE.txt",
            "VERIFICATION.txt",
            "HOW_TO_VERIFY_THIS_AER_PACKAGE.txt",
            "ROOT_HASH_SHA256.txt",
            "ROOT_SPEC_AER_V1.txt",
        ],
        "root_scope": [
            "operation_record.json",
            "approval_record.json",
            "execution_record.json",
            "agent_operation_review_report.pdf",
            "manifest.json",
        ],
        "root_serialization": {
            "encoding": "utf-8",
            "format": "filename:sha256",
            "sort_order": "ascending filename",
            "line_separator": "\\n",
            "trailing_newline": False,
        },
        "version": "aer_demo_v1",
    }
    manifest_bytes = _json_bytes(manifest)
    artifacts["manifest.json"] = manifest_bytes

    root_input_names = manifest["root_scope"]
    root_hash_pairs = [(name, _sha256_bytes(artifacts[name])) for name in root_input_names]
    root_basis = "\n".join(f"{name}:{sha}" for name, sha in sorted(root_hash_pairs, key=lambda item: item[0])).encode("utf-8")
    root_hash = _sha256_bytes(root_basis)
    artifacts["ROOT_HASH_SHA256.txt"] = (root_hash + "\n").encode("utf-8")
    artifacts["ROOT_SPEC_AER_V1.txt"] = (
        "ROOT = sha256(sorted filename:sha256 pairs for root_scope files listed in manifest)\n"
        "SERIALIZATION = UTF-8 text, '\\n' as line separator, no trailing newline after last line\n"
    ).encode("utf-8")
    artifacts["PROTOCOL_PROFILE.txt"] = (
        "PACKAGE FAMILY = H-REVN AER\n"
        "BUNDLE PROFILE = agent_operation_aer_v1\n"
        "VERIFICATION MODEL = ROOT_AER_V1\n"
        "CLASSIC H-REVN VISIT BUNDLE = not applicable\n"
        "EXTERNAL ANCHOR STATUS = not_anchored\n"
    ).encode("utf-8")
    artifacts["VERIFICATION.txt"] = (
        "This package is an H-REVN AER package, not a classic visit bundle.\n"
        "Authoritative files are declared in manifest.json.\n"
        "Verification follows CHECKSUMS.sha256 and ROOT_SPEC_AER_V1.txt.\n"
        "No external blockchain anchor is included in this demo package.\n"
    ).encode("utf-8")
    artifacts["HOW_TO_VERIFY_THIS_AER_PACKAGE.txt"] = (
        "1. Verify SHA-256 values listed in CHECKSUMS.sha256.\n"
        "2. Open manifest.json and read root_scope.\n"
        "3. Build UTF-8 lines as filename:sha256 sorted by filename.\n"
        "4. Join lines with '\\n' and do not append a trailing newline.\n"
        "5. SHA-256 that exact text and compare it with ROOT_HASH_SHA256.txt.\n"
    ).encode("utf-8")

    checksum_names = manifest["checksum_scope"]
    checksum_rows = [(name, _sha256_bytes(artifacts[name])) for name in checksum_names]
    checksums_text = "\n".join(f"{sha}  {name}" for name, sha in sorted(checksum_rows, key=lambda item: item[0])) + "\n"
    artifacts["CHECKSUMS.sha256"] = checksums_text.encode("utf-8")

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in artifacts.items():
            zf.writestr(name, content)
    zip_bytes = zip_buffer.getvalue()
    zip_sha256 = _sha256_bytes(zip_bytes)
    zip_filename = f"{package_delivery_id}_{root_hash}.zip"
    delivery_seal_filename = f"{package_delivery_id}_{root_hash}.zip.sha256.txt"
    delivery_seal_text = (
        f"PACKAGE FAMILY = H-REVN AER\n"
        f"BUNDLE PROFILE = agent_operation_aer_v1\n"
        f"PACKAGE DELIVERY ID = {package_delivery_id}\n"
        f"AER ID = {aer_id}\n"
        f"RECORD ID = {record['record_id']}\n"
        f"DELIVERY ARTIFACT = {zip_filename}\n"
        f"DELIVERY SHA256 = {zip_sha256}\n"
        f"DELIVERY SEAL STATUS = detached_container_hash\n"
        f"MATCH RULE = this sidecar is valid only for the exact DELIVERY ARTIFACT named above\n"
    ).encode("utf-8")

    return {
        "aer_id": aer_id,
        "package_delivery_id": package_delivery_id,
        "manifest_hash": _sha256_bytes(manifest_bytes),
        "root_hash": root_hash,
        "zip_sha256": zip_sha256,
        "artifacts": [{"artifact": name, "sha256": sha, "size_bytes": len(artifacts[name])} for name, sha in sorted(checksum_rows, key=lambda item: item[0])],
        "zip_filename": zip_filename,
        "zip_bytes": zip_bytes,
        "delivery_seal_filename": delivery_seal_filename,
        "delivery_seal_bytes": delivery_seal_text,
        "report_filename": "agent_operation_review_report.pdf",
    }
