"""SQLite-backed demo storage for Agent Operations."""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class AgentOperationsSnapshot:
    records: list[dict[str, Any]]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {row[1] for row in conn.execute(f"pragma table_info({table_name})").fetchall()}


def _ensure_record_columns(conn: sqlite3.Connection) -> None:
    columns = _table_columns(conn, "agent_operation_records")
    required = {
        "workflow_id": "alter table agent_operation_records add column workflow_id text default ''",
        "agent_role": "alter table agent_operation_records add column agent_role text default ''",
        "human_approval_required": "alter table agent_operation_records add column human_approval_required integer not null default 1",
        "reviewer_name": "alter table agent_operation_records add column reviewer_name text default ''",
        "reviewer_role": "alter table agent_operation_records add column reviewer_role text default ''",
        "reviewed_at_utc": "alter table agent_operation_records add column reviewed_at_utc text default ''",
        "decision_rationale": "alter table agent_operation_records add column decision_rationale text default ''",
        "execution_result": "alter table agent_operation_records add column execution_result text default 'awaiting_human_decision'",
        "aer_version": "alter table agent_operation_records add column aer_version text default 'aer_demo_v1'",
    }
    for column, ddl in required.items():
        if column not in columns:
            conn.execute(ddl)


def _seed_record_defaults(conn: sqlite3.Connection) -> None:
    defaults = {
        "CAR-2026-001": {
            "workflow_id": "WF-TREASURY-2026-001",
            "agent_role": "Treasury execution agent",
            "reviewer_name": "",
            "reviewer_role": "",
            "reviewed_at_utc": "",
            "decision_rationale": "",
            "execution_result": "awaiting_human_decision",
        },
        "CAR-2026-002": {
            "workflow_id": "WF-IAM-2026-002",
            "agent_role": "Privileged access control agent",
            "reviewer_name": "Miguel Herrero",
            "reviewer_role": "Security reviewer",
            "reviewed_at_utc": "2026-03-11T18:09:00Z",
            "decision_rationale": "Administrative access recovery validated under critical-access policy.",
            "execution_result": "executed_after_human_authorization",
        },
        "CAR-2026-003": {
            "workflow_id": "WF-LEGAL-2026-003",
            "agent_role": "Legal enforcement agent",
            "reviewer_name": "Miguel Herrero",
            "reviewer_role": "Legal reviewer",
            "reviewed_at_utc": "2026-03-10T13:48:00Z",
            "decision_rationale": "Freeze rejected pending external documentary confirmation.",
            "execution_result": "blocked_by_human_rejection",
        },
    }
    for record_id, values in defaults.items():
        conn.execute(
            """
            update agent_operation_records
            set workflow_id = coalesce(nullif(workflow_id, ''), ?),
                agent_role = coalesce(nullif(agent_role, ''), ?),
                reviewer_name = coalesce(nullif(reviewer_name, ''), ?),
                reviewer_role = coalesce(nullif(reviewer_role, ''), ?),
                reviewed_at_utc = coalesce(nullif(reviewed_at_utc, ''), ?),
                decision_rationale = coalesce(nullif(decision_rationale, ''), ?),
                execution_result = coalesce(nullif(execution_result, ''), ?)
            where record_id = ?
            """,
            (
                values["workflow_id"],
                values["agent_role"],
                values["reviewer_name"],
                values["reviewer_role"],
                values["reviewed_at_utc"],
                values["decision_rationale"],
                values["execution_result"],
                record_id,
            ),
        )


def ensure_agent_operations_demo_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = _connect(db_path)
    try:
        conn.executescript(
            """
            create table if not exists agent_operation_records (
              record_id text primary key,
              submitted_at_utc text not null,
              agent_id text,
              agent_name text not null,
              operation_type text not null,
              intent text not null,
              tool_name text,
              risk_level text not null,
              approval_policy text not null,
              review_reason text not null,
              status text not null,
              human_action text not null default 'pending',
              seal_status text not null default 'not_sealed',
              seal_reference text not null default '',
              recommended_for_execution integer not null default 0
            );

            create table if not exists agent_operation_parameters (
              parameter_id text primary key,
              record_id text not null,
              param_key text not null,
              param_value text,
              param_type text,
              display_order integer not null default 0
            );
            """
        )
        _ensure_record_columns(conn)
        count = conn.execute("select count(*) from agent_operation_records").fetchone()[0]
        if not count:
            records = [
                {
                    "record_id": "CAR-2026-001",
                    "submitted_at_utc": "2026-03-12T09:10:00Z",
                    "agent_id": "agt_treasury_01",
                    "agent_name": "Treasury_Bot_v2",
                    "agent_role": "Treasury execution agent",
                    "workflow_id": "WF-TREASURY-2026-001",
                    "operation_type": "outbound_payment",
                    "intent": "Outgoing payment release",
                    "tool_name": "Swift_Transfer_API",
                    "risk_level": "HIGH",
                    "approval_policy": "Treasury dual approval",
                    "review_reason": "Regulated outbound payment exceeds policy threshold and requires human authorization.",
                    "status": "pending_review",
                    "human_action": "pending",
                    "seal_status": "not_sealed",
                    "seal_reference": "",
                    "recommended_for_execution": 0,
                    "human_approval_required": 1,
                    "reviewer_name": "",
                    "reviewer_role": "",
                    "reviewed_at_utc": "",
                    "decision_rationale": "",
                    "execution_result": "awaiting_human_decision",
                    "aer_version": "aer_demo_v1",
                    "parameters": [
                        ("amount", "5000", "number", 1),
                        ("currency", "USD", "string", 2),
                        ("destination", "Vendor_Acct_8832", "string", 3),
                        ("reference", "INV-505", "string", 4),
                    ],
                },
                {
                    "record_id": "CAR-2026-002",
                    "submitted_at_utc": "2026-03-11T18:05:00Z",
                    "agent_id": "agt_access_01",
                    "agent_name": "Access_Bot_v1",
                    "agent_role": "Privileged access control agent",
                    "workflow_id": "WF-IAM-2026-002",
                    "operation_type": "privileged_access_change",
                    "intent": "Admin password reset",
                    "tool_name": "IAM_Admin_API",
                    "risk_level": "CRITICAL",
                    "approval_policy": "CISO approval required",
                    "review_reason": "Privileged access action over an administrative identity requires formal authorization.",
                    "status": "executed_sealed",
                    "human_action": "approved",
                    "seal_status": "sealed",
                    "seal_reference": "sha256:8f3c4a1b9d9a0f7d2b11",
                    "recommended_for_execution": 1,
                    "human_approval_required": 1,
                    "reviewer_name": "Miguel Herrero",
                    "reviewer_role": "Security reviewer",
                    "reviewed_at_utc": "2026-03-11T18:09:00Z",
                    "decision_rationale": "Administrative access recovery validated under critical-access policy.",
                    "execution_result": "executed_after_human_authorization",
                    "aer_version": "aer_demo_v1",
                    "parameters": [
                        ("target_user", "j.doe@company.com", "string", 1),
                        ("action", "force_reset", "string", 2),
                    ],
                },
                {
                    "record_id": "CAR-2026-003",
                    "submitted_at_utc": "2026-03-10T13:42:00Z",
                    "agent_id": "agt_legal_03",
                    "agent_name": "Legal_Bot_v3",
                    "agent_role": "Legal enforcement agent",
                    "workflow_id": "WF-LEGAL-2026-003",
                    "operation_type": "wallet_freeze",
                    "intent": "Wallet precautionary freeze",
                    "tool_name": "Chain_Freeze_API",
                    "risk_level": "HIGH",
                    "approval_policy": "Legal counsel review",
                    "review_reason": "Potentially irreversible regulated action over client funds requires legal review.",
                    "status": "rejected",
                    "human_action": "rejected",
                    "seal_status": "sealed_rejection",
                    "seal_reference": "sha256:rejected_no_action_7721",
                    "recommended_for_execution": 0,
                    "human_approval_required": 1,
                    "reviewer_name": "Miguel Herrero",
                    "reviewer_role": "Legal reviewer",
                    "reviewed_at_utc": "2026-03-10T13:48:00Z",
                    "decision_rationale": "Freeze rejected pending external documentary confirmation.",
                    "execution_result": "blocked_by_human_rejection",
                    "aer_version": "aer_demo_v1",
                    "parameters": [
                        ("wallet_address", "0x7a59...8f3c", "string", 1),
                        ("reason_code", "SEC_INQUIRY", "string", 2),
                    ],
                },
            ]
            for record in records:
                conn.execute(
                    """
                    insert into agent_operation_records (
                      record_id, submitted_at_utc, agent_id, agent_name, operation_type, intent, tool_name,
                      risk_level, approval_policy, review_reason, status, human_action, seal_status,
                      seal_reference, recommended_for_execution, workflow_id, agent_role,
                      human_approval_required, reviewer_name, reviewer_role, reviewed_at_utc,
                      decision_rationale, execution_result, aer_version
                    ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record["record_id"],
                        record["submitted_at_utc"],
                        record["agent_id"],
                        record["agent_name"],
                        record["operation_type"],
                        record["intent"],
                        record["tool_name"],
                        record["risk_level"],
                        record["approval_policy"],
                        record["review_reason"],
                        record["status"],
                        record["human_action"],
                        record["seal_status"],
                        record["seal_reference"],
                        record["recommended_for_execution"],
                        record["workflow_id"],
                        record["agent_role"],
                        record["human_approval_required"],
                        record["reviewer_name"],
                        record["reviewer_role"],
                        record["reviewed_at_utc"],
                        record["decision_rationale"],
                        record["execution_result"],
                        record["aer_version"],
                    ),
                )
                for idx, (key, value, ptype, order_idx) in enumerate(record["parameters"], start=1):
                    conn.execute(
                        """
                        insert into agent_operation_parameters (
                          parameter_id, record_id, param_key, param_value, param_type, display_order
                        ) values (?, ?, ?, ?, ?, ?)
                        """,
                        (f"{record['record_id']}-P{idx:03d}", record["record_id"], key, value, ptype, order_idx),
                    )
        _seed_record_defaults(conn)
        conn.commit()
    finally:
        conn.close()


def _risk_rank(value: str) -> int:
    order = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
    return order.get(str(value or "").upper(), 0)


def load_agent_operations_snapshot(db_path: Path) -> AgentOperationsSnapshot:
    ensure_agent_operations_demo_db(db_path)
    conn = _connect(db_path)
    try:
        records = [
            dict(row)
            for row in conn.execute(
                """
                select *
                from agent_operation_records
                order by submitted_at_utc desc, record_id desc
                """
            ).fetchall()
        ]
        for record in records:
            params = [
                dict(row)
                for row in conn.execute(
                    """
                    select param_key, param_value, param_type, display_order
                    from agent_operation_parameters
                    where record_id = ?
                    order by display_order, parameter_id
                    """,
                    (record["record_id"],),
                ).fetchall()
            ]
            record["parameters"] = [
                {"field": item["param_key"], "value": item["param_value"], "type": item["param_type"]}
                for item in params
            ]
            record["recommended_for_execution"] = bool(record.get("recommended_for_execution"))
            record["human_approval_required"] = bool(record.get("human_approval_required", 1))
            record["risk_rank"] = _risk_rank(str(record.get("risk_level") or ""))
    finally:
        conn.close()
    return AgentOperationsSnapshot(records=records)


def set_agent_operation_decision(
    db_path: Path,
    record_id: str,
    decision: str,
    reviewer_name: str = "Human Reviewer",
    reviewer_role: str = "Operations Reviewer",
    rationale: str = "",
) -> None:
    ensure_agent_operations_demo_db(db_path)
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "select record_id, agent_name, intent, submitted_at_utc from agent_operation_records where record_id = ?",
            (record_id,),
        ).fetchone()
        if not row:
            return
        timestamp = _utc_now()
        normalized_rationale = (rationale or "").strip()
        raw = f"{row['record_id']}|{decision}|{row['agent_name']}|{row['intent']}|{row['submitted_at_utc']}|{timestamp}|{normalized_rationale}"
        seal = f"sha256:{hashlib.sha256(raw.encode()).hexdigest()[:20]}"
        if decision == "approved":
            conn.execute(
                """
                update agent_operation_records
                set status = 'executed_sealed',
                    human_action = 'approved',
                    seal_status = 'sealed',
                    seal_reference = ?,
                    recommended_for_execution = 1,
                    reviewer_name = ?,
                    reviewer_role = ?,
                    reviewed_at_utc = ?,
                    decision_rationale = ?,
                    execution_result = 'executed_after_human_authorization'
                where record_id = ?
                """,
                (
                    seal,
                    reviewer_name,
                    reviewer_role,
                    timestamp,
                    normalized_rationale or "Approved under current regulated-operation policy.",
                    record_id,
                ),
            )
        elif decision == "rejected":
            conn.execute(
                """
                update agent_operation_records
                set status = 'rejected',
                    human_action = 'rejected',
                    seal_status = 'sealed_rejection',
                    seal_reference = ?,
                    recommended_for_execution = 0,
                    reviewer_name = ?,
                    reviewer_role = ?,
                    reviewed_at_utc = ?,
                    decision_rationale = ?,
                    execution_result = 'blocked_by_human_rejection'
                where record_id = ?
                """,
                (
                    seal,
                    reviewer_name,
                    reviewer_role,
                    timestamp,
                    normalized_rationale or "Rejected by human reviewer.",
                    record_id,
                ),
            )
        conn.commit()
    finally:
        conn.close()
