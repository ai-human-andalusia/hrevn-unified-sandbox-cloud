"""AER package generation for Agent Operations demo."""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
import base64
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False).encode("utf-8")


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


@dataclass(frozen=True)
class AERSigningConfig:
    enabled: bool
    issuer: str
    key_id: str
    private_key: str
    verification_url: str
    signature_profile: str = "hrevn_signing_v1"
    algorithm: str = "ed25519"


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


def _load_ed25519_private_key(private_key_value: str):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    raw = private_key_value.strip()
    if not raw:
        raise ValueError("empty signing key")
    if "BEGIN PRIVATE KEY" in raw:
        return serialization.load_pem_private_key(raw.encode("utf-8"), password=None)
    try:
        return Ed25519PrivateKey.from_private_bytes(bytes.fromhex(raw))
    except ValueError:
        pass
    try:
        return Ed25519PrivateKey.from_private_bytes(base64.b64decode(raw))
    except Exception as exc:  # pragma: no cover - defensive parse path
        raise ValueError("unsupported Ed25519 private key format") from exc


def _ed25519_public_key_bytes(public_key) -> bytes:
    from cryptography.hazmat.primitives import serialization

    return public_key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )


def _build_signature_payload(
    aer_id: str,
    record: dict[str, Any],
    manifest_hash: str,
    root_hash: str,
    signing_config: AERSigningConfig,
) -> dict[str, Any]:
    return {
        "package_family": "hrevn_aer",
        "bundle_profile": "agent_operation_aer_v1",
        "aer_id": aer_id,
        "record_id": record["record_id"],
        "workflow_id": record.get("workflow_id") or f"WF-{record['record_id']}",
        "signature_profile": signing_config.signature_profile,
        "signed_by": signing_config.issuer,
        "key_id": signing_config.key_id,
        "signature_algorithm": signing_config.algorithm,
        "signature_scope": ["manifest.json", "ROOT_HASH_SHA256.txt"],
        "manifest_hash": manifest_hash,
        "root_hash": root_hash,
        "verification_url": signing_config.verification_url,
    }


def _build_signature_spec_bytes() -> bytes:
    return (
        "SIGNATURE PROFILE = hrevn_signing_v1\n"
        "SIGNATURE ARTIFACT = SIGNATURE.json\n"
        "SIGNED PAYLOAD ARTIFACT = SIGNATURE_PAYLOAD.json\n"
        "SIGNATURE SPEC = SIGNATURE_SPEC_AER_V1.txt\n"
        "ALGORITHM = ed25519\n"
        "PAYLOAD SERIALIZATION = exact UTF-8 bytes of SIGNATURE_PAYLOAD.json as stored in the package\n"
        "PAYLOAD NEWLINE RULE = no trailing newline required or appended by the verifier\n"
        "VERIFICATION METHOD = decode signature_value_base64, decode public_key_base64, verify signature over exact payload bytes\n"
        "PAYLOAD CONTENT = package_family, bundle_profile, aer_id, record_id, workflow_id, signature_profile, signed_by, key_id, signature_algorithm, signature_scope, manifest_hash, root_hash, verification_url\n"
    ).encode("utf-8")


def _maybe_build_signature_artifacts(
    aer_id: str,
    record: dict[str, Any],
    manifest_hash: str,
    root_hash: str,
    signing_config: AERSigningConfig | None,
) -> tuple[dict[str, Any] | None, bytes | None, bytes | None]:
    if not signing_config or not signing_config.enabled or not signing_config.private_key.strip():
        return None, None, None

    private_key = _load_ed25519_private_key(signing_config.private_key)
    public_key = private_key.public_key()
    payload = _build_signature_payload(aer_id, record, manifest_hash, root_hash, signing_config)
    payload_bytes = _json_bytes(payload)
    signature_bytes = private_key.sign(payload_bytes)
    public_key_bytes = _ed25519_public_key_bytes(public_key)
    signature_artifact = {
        **payload,
        "signature_status": "signed",
        "signature_spec_artifact": "SIGNATURE_SPEC_AER_V1.txt",
        "signed_payload_artifact": "SIGNATURE_PAYLOAD.json",
        "payload_hash_sha256": hashlib.sha256(payload_bytes).hexdigest(),
        "public_key_fingerprint_sha256": hashlib.sha256(public_key_bytes).hexdigest(),
        "public_key_base64": base64.b64encode(public_key_bytes).decode("ascii"),
        "signature_value_base64": base64.b64encode(signature_bytes).decode("ascii"),
    }
    return signature_artifact, _json_bytes(signature_artifact), payload_bytes


def build_agent_operation_aer_package(record: dict[str, Any], signing_config: AERSigningConfig | None = None) -> dict[str, Any]:
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

    signing_requested = bool(signing_config and signing_config.enabled and signing_config.private_key.strip())

    artifact_catalog = [
        {"artifact": "operation_record.json", "category": "core", "role": "structured_operation_record"},
        {"artifact": "approval_record.json", "category": "core", "role": "human_approval_record"},
        {"artifact": "execution_record.json", "category": "core", "role": "execution_outcome_record"},
        {"artifact": "agent_operation_review_report.pdf", "category": "core", "role": "human_readable_review_report"},
        {"artifact": "manifest.json", "category": "control", "role": "package_manifest"},
        {"artifact": "PROTOCOL_PROFILE.txt", "category": "support", "role": "package_profile_summary"},
        {"artifact": "RIGHTS_NOTICE.txt", "category": "support", "role": "rights_and_ip_notice"},
        {"artifact": "VERIFICATION.txt", "category": "support", "role": "verification_guidance_summary"},
        {"artifact": "HOW_TO_VERIFY_THIS_AER_PACKAGE.txt", "category": "support", "role": "step_by_step_verification_guide"},
        {"artifact": "CHECKSUMS.sha256", "category": "verification", "role": "artifact_hash_list"},
        {"artifact": "ROOT_HASH_SHA256.txt", "category": "verification", "role": "package_root_hash"},
        {"artifact": "ROOT_SPEC_AER_V1.txt", "category": "verification", "role": "root_hash_rule"},
    ]
    if signing_requested:
        artifact_catalog.extend(
            [
                {"artifact": "SIGNATURE_PAYLOAD.json", "category": "verification", "role": "signed_payload_record"},
                {"artifact": "SIGNATURE.json", "category": "verification", "role": "institutional_signature_record"},
                {"artifact": "SIGNATURE_SPEC_AER_V1.txt", "category": "verification", "role": "signature_verification_rule"},
            ]
        )
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
        "signature_status": "signed" if signing_requested else "unsigned_demo",
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
            "RIGHTS_NOTICE.txt",
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
            "RIGHTS_NOTICE.txt",
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
    if signing_requested and signing_config:
        manifest["signature_profile"] = signing_config.signature_profile
        manifest["signed_by"] = signing_config.issuer
        manifest["key_id"] = signing_config.key_id
        manifest["signature_algorithm"] = signing_config.algorithm
        manifest["signature_scope"] = ["manifest.json", "ROOT_HASH_SHA256.txt"]
        manifest["verification_url"] = signing_config.verification_url
        manifest["signature_payload_artifact"] = "SIGNATURE_PAYLOAD.json"
        manifest["signature_spec_artifact"] = "SIGNATURE_SPEC_AER_V1.txt"
        manifest["authoritative_files"].extend(["SIGNATURE_PAYLOAD.json", "SIGNATURE.json", "SIGNATURE_SPEC_AER_V1.txt"])
        manifest["checksum_scope"].extend(["SIGNATURE_PAYLOAD.json", "SIGNATURE.json", "SIGNATURE_SPEC_AER_V1.txt"])
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
    artifacts["RIGHTS_NOTICE.txt"] = (
        "H-REVN AER package\n"
        "\n"
        "Copyright © 2026 H-REVN. All rights reserved.\n"
        "\n"
        "This package and its included documentation, schemas, verification materials,\n"
        "and software-generated records are proprietary to H-REVN except where otherwise stated.\n"
        "\n"
        "This package follows the H-REVN AER profile. The H-REVN protocol, documentation,\n"
        "and related materials are protected under applicable intellectual property laws.\n"
        "\n"
        "Any registration references, if applicable, should be verified against the corresponding\n"
        "official records and should be cited using the exact registration identifier.\n"
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

    signature_artifact, signature_bytes, signature_payload_bytes = _maybe_build_signature_artifacts(
        aer_id,
        record,
        _sha256_bytes(manifest_bytes),
        root_hash,
        signing_config if signing_requested else None,
    )
    if signature_artifact and signature_bytes and signature_payload_bytes:
        artifacts["SIGNATURE_PAYLOAD.json"] = signature_payload_bytes
        artifacts["SIGNATURE.json"] = signature_bytes
        artifacts["SIGNATURE_SPEC_AER_V1.txt"] = _build_signature_spec_bytes()

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
    delivery_bundle_buffer = io.BytesIO()
    delivery_bundle_filename = f"{package_delivery_id}_{root_hash}_delivery_bundle.zip"
    with zipfile.ZipFile(delivery_bundle_buffer, "w", compression=zipfile.ZIP_DEFLATED) as delivery_zf:
        delivery_zf.writestr(zip_filename, zip_bytes)
        delivery_zf.writestr(delivery_seal_filename, delivery_seal_text)
    delivery_bundle_bytes = delivery_bundle_buffer.getvalue()
    delivery_bundle_sha256 = _sha256_bytes(delivery_bundle_bytes)

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
        "delivery_bundle_filename": delivery_bundle_filename,
        "delivery_bundle_bytes": delivery_bundle_bytes,
        "delivery_bundle_sha256": delivery_bundle_sha256,
        "report_filename": "agent_operation_review_report.pdf",
    }
