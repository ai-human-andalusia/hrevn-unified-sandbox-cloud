from __future__ import annotations

import base64
import json
import sqlite3
import urllib.parse
import urllib.request
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


@dataclass(frozen=True)
class CommunicationsSyncResult:
    fetched: int
    inserted: int
    support_tickets: int
    sales_leads: int
    general_emails: int
    sent_fetched: int
    sent_inserted: int


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
  source_thread_id TEXT,
  from_email TEXT,
  from_name TEXT,
  sent_at_utc TEXT,
  created_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_comm_outbound_emails_delivery_status ON comm_outbound_emails(delivery_status);
CREATE INDEX IF NOT EXISTS idx_comm_outbound_emails_provider_message_id ON comm_outbound_emails(provider_message_id);

CREATE TABLE IF NOT EXISTS comm_sync_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider TEXT NOT NULL DEFAULT 'gmail',
  status TEXT NOT NULL DEFAULT 'ok',
  fetched_count INTEGER NOT NULL DEFAULT 0,
  inserted_count INTEGER NOT NULL DEFAULT 0,
  support_count INTEGER NOT NULL DEFAULT 0,
  business_count INTEGER NOT NULL DEFAULT 0,
  general_count INTEGER NOT NULL DEFAULT 0,
  detail_text TEXT,
  created_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


def ensure_communications_schema(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(SCHEMA_SQL)
        existing = {row[1] for row in conn.execute("PRAGMA table_info(comm_outbound_emails)").fetchall()}
        if "source_thread_id" not in existing:
            conn.execute("ALTER TABLE comm_outbound_emails ADD COLUMN source_thread_id TEXT")
        if "from_email" not in existing:
            conn.execute("ALTER TABLE comm_outbound_emails ADD COLUMN from_email TEXT")
        if "from_name" not in existing:
            conn.execute("ALTER TABLE comm_outbound_emails ADD COLUMN from_name TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_comm_outbound_emails_provider_message_id ON comm_outbound_emails(provider_message_id)")
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


def _base64url_decode(input_value: str = "") -> str:
    s = input_value.replace("-", "+").replace("_", "/")
    pad = "=" * ((4 - len(s) % 4) % 4)
    return base64.b64decode((s + pad).encode("utf-8")).decode("utf-8", errors="replace")


def _first_header(headers: list[dict[str, Any]] | None, name: str) -> str:
    target = name.lower()
    for item in headers or []:
        if str(item.get("name") or "").lower() == target:
            return str(item.get("value") or "")
    return ""


def _extract_text_from_payload(payload: dict[str, Any] | None) -> str:
    if not payload:
        return ""
    body = payload.get("body") or {}
    if body.get("data"):
        return _base64url_decode(str(body.get("data") or ""))
    parts = payload.get("parts") or []
    for part in parts:
        if str(part.get("mimeType") or "") == "text/plain" and ((part.get("body") or {}).get("data")):
            return _base64url_decode(str((part.get("body") or {}).get("data") or ""))
    for part in parts:
        nested = _extract_text_from_payload(part)
        if nested:
            return nested
    return ""


def classify_inbound_email(subject: str = "", body: str = "") -> tuple[str, str]:
    s = f"{subject}\n{body}".lower()
    support_hints = ["incidencia", "error", "fallo", "soporte", "no funciona", "problema", "urgent", "bug", "ticket"]
    business_hints = ["contratar", "presupuesto", "demo", "servicio", "pricing", "quote", "partner", "empresa", "plan", "meeting", "proposal", "invest"]
    if any(k in s for k in support_hints):
        return ("support", "Matched support keywords in subject/body")
    if any(k in s for k in business_hints):
        return ("business", "Matched commercial keywords in subject/body")
    return ("general", "No support/commercial keyword match")


def _http_json(url: str, *, method: str = "GET", headers: dict[str, str] | None = None, data: bytes | None = None) -> dict[str, Any]:
    req = urllib.request.Request(url, method=method, headers=headers or {}, data=data)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _gmail_access_token(*, client_id: str, client_secret: str, refresh_token: str) -> str:
    form = urllib.parse.urlencode({
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }).encode("utf-8")
    payload = _http_json(
        "https://oauth2.googleapis.com/token",
        method="POST",
        headers={"content-type": "application/x-www-form-urlencoded"},
        data=form,
    )
    token = str(payload.get("access_token") or "")
    if not token:
        raise RuntimeError("Gmail OAuth token refresh returned no access token")
    return token


def _gmail_api(path: str, *, access_token: str) -> dict[str, Any]:
    return _http_json(
        f"https://gmail.googleapis.com/gmail/v1/{path}",
        headers={"Authorization": f"Bearer {access_token}"},
    )


def _insert_support_ticket(conn: sqlite3.Connection, *, source_email_id: int, from_email: str, subject: str, body_text: str) -> None:
    cur = conn.execute(
        "INSERT INTO comm_support_tickets(source_email_id,title,description,customer_email,topic,status,priority) VALUES(?,?,?,?, 'technical_support','open','normal')",
        (source_email_id, subject or "Support request", body_text[:3000], from_email or None),
    )
    ticket_id = int(cur.lastrowid)
    ticket_code = f"SUP-{ticket_id:06d}"
    conn.execute("UPDATE comm_support_tickets SET ticket_code=?, updated_at_utc=CURRENT_TIMESTAMP WHERE id=?", (ticket_code, ticket_id))
    conn.execute("INSERT INTO comm_support_ticket_messages(ticket_id,sender_type,message_text) VALUES(?, ?, ?)", (ticket_id, "customer", (body_text or "(empty message)")[:3000]))


def _insert_sales_lead(conn: sqlite3.Connection, *, source_email_id: int, from_email: str, subject: str, body_text: str) -> None:
    cur = conn.execute(
        "INSERT INTO comm_sales_leads(source_email_id,contact_email,subject,message_excerpt,interest_type,priority,status) VALUES(?,?,?,?, 'general','normal','new')",
        (source_email_id, from_email or None, subject or None, body_text[:1200]),
    )
    lead_id = int(cur.lastrowid)
    lead_code = f"LEAD-{lead_id:06d}"
    conn.execute("UPDATE comm_sales_leads SET lead_code=?, updated_at_utc=CURRENT_TIMESTAMP WHERE id=?", (lead_code, lead_id))


def _sync_gmail_sent(conn: sqlite3.Connection, *, access_token: str, user: str, max_results: int) -> tuple[int, int]:
    listing = _gmail_api(
        f"users/{user}/messages?q={urllib.parse.quote('in:sent', safe='')}&maxResults={max(1, min(int(max_results), 100))}",
        access_token=access_token,
    )
    messages = listing.get("messages") or []
    fetched = len(messages)
    inserted = 0
    for message in messages:
        msg_id = str(message.get("id") or "")
        if not msg_id:
            continue
        exists = conn.execute(
            "SELECT 1 FROM comm_outbound_emails WHERE provider_message_id=? LIMIT 1",
            (msg_id,),
        ).fetchone()
        if exists:
            continue
        full = _gmail_api(
            f"users/{user}/messages/{urllib.parse.quote(msg_id, safe='')}?format=full",
            access_token=access_token,
        )
        payload = full.get("payload") or {}
        headers = payload.get("headers") or []
        subject = _first_header(headers, "Subject")
        to_raw = _first_header(headers, "To")
        from_raw = _first_header(headers, "From")
        date_raw = _first_header(headers, "Date")
        body_text = _extract_text_from_payload(payload) or str(full.get("snippet") or "")
        to_email = to_raw
        if "<" in to_raw and ">" in to_raw:
            _, _, right = to_raw.partition("<")
            to_email = right.split(">", 1)[0].strip()
        from_email = from_raw
        from_name = ""
        if "<" in from_raw and ">" in from_raw:
            left, _, right = from_raw.partition("<")
            from_name = left.strip().strip('"')
            from_email = right.split(">", 1)[0].strip()
        conn.execute(
            "INSERT INTO comm_outbound_emails(related_entity_type,related_entity_id,to_email,subject,body_text,delivery_channel,delivery_status,provider_message_id,source_thread_id,from_email,from_name,sent_at_utc) VALUES(NULL,NULL,?,?,?,'gmail','sent',?,?,?,?,?)",
            (to_email[:240] if to_email else None, subject[:500] if subject else None, body_text[:12000] if body_text else None, msg_id, str(full.get('threadId') or '') or None, from_email[:240] if from_email else None, from_name[:180] if from_name else None, date_raw[:120] if date_raw else None),
        )
        inserted += 1
    return fetched, inserted


def sync_gmail_inbox(db_path: Path, *, gmail_client_id: str, gmail_client_secret: str, gmail_refresh_token: str, gmail_mailbox_user: str = "me", gmail_sync_query: str = "is:unread", max_results: int = 20) -> CommunicationsSyncResult:
    ensure_communications_schema(db_path)
    if not gmail_client_id or not gmail_client_secret or not gmail_refresh_token:
        raise RuntimeError("Gmail OAuth2 credentials missing")
    access_token = _gmail_access_token(client_id=gmail_client_id, client_secret=gmail_client_secret, refresh_token=gmail_refresh_token)
    user = urllib.parse.quote(gmail_mailbox_user or "me", safe="")
    query = urllib.parse.quote(gmail_sync_query or "is:unread", safe="")
    listing = _gmail_api(f"users/{user}/messages?q={query}&maxResults={max(1, min(int(max_results), 100))}", access_token=access_token)
    messages = listing.get("messages") or []
    result = {"fetched": len(messages), "inserted": 0, "support_tickets": 0, "sales_leads": 0, "general_emails": 0, "sent_fetched": 0, "sent_inserted": 0}
    with sqlite3.connect(str(db_path)) as conn:
        for message in messages:
            msg_id = str(message.get("id") or "")
            if not msg_id:
                continue
            full = _gmail_api(f"users/{user}/messages/{urllib.parse.quote(msg_id, safe='')}?format=full", access_token=access_token)
            payload = full.get("payload") or {}
            headers = payload.get("headers") or []
            subject = _first_header(headers, "Subject")
            from_raw = _first_header(headers, "From")
            date_raw = _first_header(headers, "Date")
            body_text = _extract_text_from_payload(payload) or str(full.get("snippet") or "")
            classification, classification_reason = classify_inbound_email(subject, body_text)
            from_email = from_raw
            from_name = ""
            if "<" in from_raw and ">" in from_raw:
                left, _, right = from_raw.partition("<")
                from_name = left.strip().strip('"')
                from_email = right.split(">", 1)[0].strip()
            cur = conn.execute(
                "INSERT OR IGNORE INTO comm_inbound_emails(source_provider,source_message_id,source_thread_id,from_email,from_name,subject,body_text,received_at_utc,status,classification,classification_reason,processed_at_utc) VALUES(?,?,?,?,?,?,?,?,'open',?,?,CURRENT_TIMESTAMP)",
                ('gmail', msg_id, str(full.get('threadId') or '') or None, from_email[:240] if from_email else None, from_name[:180] if from_name else None, subject[:500] if subject else None, body_text[:12000] if body_text else None, date_raw[:120] if date_raw else None, classification, classification_reason),
            )
            if cur.rowcount < 1:
                continue
            email_id = int(cur.lastrowid)
            result['inserted'] += 1
            if classification == 'support':
                _insert_support_ticket(conn, source_email_id=email_id, from_email=from_email, subject=subject, body_text=body_text)
                result['support_tickets'] += 1
            elif classification == 'business':
                _insert_sales_lead(conn, source_email_id=email_id, from_email=from_email, subject=subject, body_text=body_text)
                result['sales_leads'] += 1
            else:
                result['general_emails'] += 1
        sent_fetched, sent_inserted = _sync_gmail_sent(conn, access_token=access_token, user=user, max_results=max_results)
        result['sent_fetched'] = sent_fetched
        result['sent_inserted'] = sent_inserted
        conn.execute("INSERT INTO comm_sync_runs(provider,status,fetched_count,inserted_count,support_count,business_count,general_count,detail_text) VALUES(?,?,?,?,?,?,?,?)", ('gmail','ok',result['fetched'] + result['sent_fetched'],result['inserted'] + result['sent_inserted'],result['support_tickets'],result['sales_leads'],result['general_emails'],f"manual_streamlit_sync inbox={result['fetched']} sent={result['sent_fetched']}"))
        conn.commit()
    return CommunicationsSyncResult(**result)
