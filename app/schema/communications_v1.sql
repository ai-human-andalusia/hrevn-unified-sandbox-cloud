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
