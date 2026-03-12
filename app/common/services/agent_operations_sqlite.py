"""SQLite-backed demo storage for Agent Operations."""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class AgentOperationsSnapshot:
    records: list[dict[str, Any]]


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


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
        count = conn.execute("select count(*) from agent_operation_records").fetchone()[0]
        if count:
            return

        records = [
            {
                "record_id": "CAR-2026-001",
                "submitted_at_utc": "2026-03-12T09:10:00Z",
                "agent_id": "agt_treasury_01",
                "agent_name": "Treasury_Bot_v2",
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
                  seal_reference, recommended_for_execution
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        records = [dict(row) for row in conn.execute(
            """
            select *
            from agent_operation_records
            order by submitted_at_utc desc, record_id desc
            """
        ).fetchall()]
        for record in records:
            params = [dict(row) for row in conn.execute(
                """
                select param_key, param_value, param_type, display_order
                from agent_operation_parameters
                where record_id = ?
                order by display_order, parameter_id
                """,
                (record["record_id"],),
            ).fetchall()]
            record["parameters"] = [
                {"field": item["param_key"], "value": item["param_value"], "type": item["param_type"]}
                for item in params
            ]
            record["recommended_for_execution"] = bool(record.get("recommended_for_execution"))
            record["risk_rank"] = _risk_rank(str(record.get("risk_level") or ""))
    finally:
        conn.close()
    return AgentOperationsSnapshot(records=records)


def set_agent_operation_decision(db_path: Path, record_id: str, decision: str) -> None:
    ensure_agent_operations_demo_db(db_path)
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "select record_id, agent_name, intent, submitted_at_utc from agent_operation_records where record_id = ?",
            (record_id,),
        ).fetchone()
        if not row:
            return
        raw = f"{row['record_id']}|{decision}|{row['agent_name']}|{row['intent']}|{row['submitted_at_utc']}"
        seal = f"sha256:{hashlib.sha256(raw.encode()).hexdigest()[:20]}"
        if decision == "approved":
            conn.execute(
                """
                update agent_operation_records
                set status = 'executed_sealed',
                    human_action = 'approved',
                    seal_status = 'sealed',
                    seal_reference = ?,
                    recommended_for_execution = 1
                where record_id = ?
                """,
                (seal, record_id),
            )
        elif decision == "rejected":
            conn.execute(
                """
                update agent_operation_records
                set status = 'rejected',
                    human_action = 'rejected',
                    seal_status = 'sealed_rejection',
                    seal_reference = ?,
                    recommended_for_execution = 0
                where record_id = ?
                """,
                (seal, record_id),
            )
        conn.commit()
    finally:
        conn.close()
