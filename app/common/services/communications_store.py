from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CommunicationsSnapshot:
    inbound_emails: list[dict[str, Any]]
    support_tickets: list[dict[str, Any]]
    sales_leads: list[dict[str, Any]]
    outbound_emails: list[dict[str, Any]]
    total_received: int
    total_support: int
    total_business: int
    total_general: int
    total_sent: int


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS comm_inbound_emails (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  enterprise_id TEXT,
  source_provider TEXT NOT NULL DEFAULT 'gmail',
  source_message_id TEXT UNIQUE,
  source_thread_id TEXT,
  from_email TEXT,
  from_name TEXT,
  subject TEXT,
  body_text TEXT,
  received_at_utc TEXT,
  status TEXT NOT NULL DEFAULT 'open',
  classification TEXT NOT NULL DEFAULT 'general',
  classification_reason TEXT,
  processed_at_utc TEXT,
  created_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_comm_inbound_emails_enterprise_id ON comm_inbound_emails(enterprise_id);
CREATE INDEX IF NOT EXISTS idx_comm_inbound_emails_classification ON comm_inbound_emails(classification);
CREATE INDEX IF NOT EXISTS idx_comm_inbound_emails_status ON comm_inbound_emails(status);

CREATE TABLE IF NOT EXISTS comm_support_tickets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  enterprise_id TEXT,
  source_email_id INTEGER REFERENCES comm_inbound_emails(id) ON DELETE SET NULL,
  ticket_code TEXT UNIQUE,
  title TEXT NOT NULL,
  description TEXT,
  customer_email TEXT,
  topic TEXT NOT NULL DEFAULT 'technical_support',
  status TEXT NOT NULL DEFAULT 'open',
  priority TEXT NOT NULL DEFAULT 'normal',
  opened_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  resolved_at_utc TEXT,
  closed_at_utc TEXT,
  created_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_comm_support_tickets_enterprise_id ON comm_support_tickets(enterprise_id);
CREATE INDEX IF NOT EXISTS idx_comm_support_tickets_status ON comm_support_tickets(status);

CREATE TABLE IF NOT EXISTS comm_support_ticket_messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ticket_id INTEGER NOT NULL REFERENCES comm_support_tickets(id) ON DELETE CASCADE,
  sender_type TEXT NOT NULL DEFAULT 'customer',
  message_text TEXT NOT NULL,
  created_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS comm_sales_leads (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  enterprise_id TEXT,
  source_email_id INTEGER REFERENCES comm_inbound_emails(id) ON DELETE SET NULL,
  lead_code TEXT UNIQUE,
  company_name TEXT,
  contact_email TEXT,
  subject TEXT,
  message_excerpt TEXT,
  interest_type TEXT NOT NULL DEFAULT 'general',
  priority TEXT NOT NULL DEFAULT 'normal',
  status TEXT NOT NULL DEFAULT 'new',
  created_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_comm_sales_leads_enterprise_id ON comm_sales_leads(enterprise_id);
CREATE INDEX IF NOT EXISTS idx_comm_sales_leads_status ON comm_sales_leads(status);

CREATE TABLE IF NOT EXISTS comm_outbound_emails (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  related_entity_type TEXT,
  related_entity_id TEXT,
  to_email TEXT,
  subject TEXT,
  body_text TEXT,
  delivery_channel TEXT NOT NULL DEFAULT 'smtp',
  delivery_status TEXT NOT NULL DEFAULT 'queued',
  provider_message_id TEXT,
  sent_at_utc TEXT,
  created_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_comm_outbound_emails_delivery_status ON comm_outbound_emails(delivery_status);
"""


def ensure_communications_schema(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(SCHEMA_SQL)
        conn.commit()


def _fetch_all(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def load_communications_snapshot(db_path: Path) -> CommunicationsSnapshot:
    ensure_communications_schema(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        inbound = _fetch_all(
            conn,
            """
            SELECT id, from_email, from_name, subject, received_at_utc, status, classification, classification_reason
            FROM comm_inbound_emails
            ORDER BY COALESCE(received_at_utc, created_at_utc) DESC, id DESC
            LIMIT 250
            """,
        )
        tickets = _fetch_all(
            conn,
            """
            SELECT ticket_code, title, customer_email, topic, status, priority, opened_at_utc
            FROM comm_support_tickets
            ORDER BY COALESCE(opened_at_utc, created_at_utc) DESC, id DESC
            LIMIT 250
            """,
        )
        leads = _fetch_all(
            conn,
            """
            SELECT lead_code, company_name, contact_email, subject, interest_type, status, created_at_utc
            FROM comm_sales_leads
            ORDER BY created_at_utc DESC, id DESC
            LIMIT 250
            """,
        )
        outbound = _fetch_all(
            conn,
            """
            SELECT to_email, subject, delivery_channel, delivery_status, sent_at_utc, created_at_utc
            FROM comm_outbound_emails
            ORDER BY COALESCE(sent_at_utc, created_at_utc) DESC, id DESC
            LIMIT 250
            """,
        )

        total_received = int(conn.execute("SELECT COUNT(*) FROM comm_inbound_emails").fetchone()[0] or 0)
        total_support = int(conn.execute("SELECT COUNT(*) FROM comm_support_tickets").fetchone()[0] or 0)
        total_business = int(conn.execute("SELECT COUNT(*) FROM comm_sales_leads").fetchone()[0] or 0)
        total_general = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM comm_inbound_emails m
                LEFT JOIN comm_support_tickets st ON st.source_email_id = m.id
                LEFT JOIN comm_sales_leads sl ON sl.source_email_id = m.id
                WHERE COALESCE(m.classification, 'general') = 'general'
                  AND st.id IS NULL
                  AND sl.id IS NULL
                """
            ).fetchone()[0]
            or 0
        )
        total_sent = int(conn.execute("SELECT COUNT(*) FROM comm_outbound_emails").fetchone()[0] or 0)

    return CommunicationsSnapshot(
        inbound_emails=inbound,
        support_tickets=tickets,
        sales_leads=leads,
        outbound_emails=outbound,
        total_received=total_received,
        total_support=total_support,
        total_business=total_business,
        total_general=total_general,
        total_sent=total_sent,
    )
