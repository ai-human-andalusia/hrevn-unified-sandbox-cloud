PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS rwa_accounts (
  account_id TEXT PRIMARY KEY,
  user_email TEXT NOT NULL UNIQUE,
  first_name TEXT,
  last_name TEXT,
  display_name TEXT,
  user_phone TEXT,
  user_role TEXT NOT NULL DEFAULT 'operator',
  preferred_language TEXT NOT NULL DEFAULT 'en',
  profile_data_json TEXT NOT NULL DEFAULT '{}',
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rwa_enterprises (
  enterprise_id TEXT PRIMARY KEY,
  enterprise_name TEXT NOT NULL,
  enterprise_type TEXT NOT NULL DEFAULT 'rwa',
  contact_email TEXT,
  contact_phone TEXT,
  enterprise_data_json TEXT NOT NULL DEFAULT '{}',
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rwa_assets (
  asset_id TEXT PRIMARY KEY,
  enterprise_id TEXT,
  asset_public_id TEXT NOT NULL UNIQUE,
  asset_type TEXT NOT NULL DEFAULT 'rwa_asset',
  asset_name TEXT NOT NULL,
  asset_status TEXT NOT NULL DEFAULT 'active',
  asset_data_json TEXT NOT NULL DEFAULT '{}',
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  FOREIGN KEY (enterprise_id) REFERENCES rwa_enterprises(enterprise_id)
);

CREATE TABLE IF NOT EXISTS rwa_visits (
  visit_id TEXT PRIMARY KEY,
  asset_id TEXT NOT NULL,
  created_by_account_id TEXT,
  visit_date_utc TEXT,
  visit_status TEXT NOT NULL DEFAULT 'work',
  review_status TEXT NOT NULL DEFAULT 'pending',
  issuance_status TEXT NOT NULL DEFAULT 'not_issued',
  visit_data_json TEXT NOT NULL DEFAULT '{}',
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  FOREIGN KEY (asset_id) REFERENCES rwa_assets(asset_id),
  FOREIGN KEY (created_by_account_id) REFERENCES rwa_accounts(account_id)
);

CREATE TABLE IF NOT EXISTS rwa_observations (
  observation_id TEXT PRIMARY KEY,
  visit_id TEXT NOT NULL,
  asset_id TEXT NOT NULL,
  lpi_code TEXT,
  severity_0_5 INTEGER NOT NULL DEFAULT 0,
  observation_description TEXT,
  row_status TEXT NOT NULL DEFAULT 'work',
  observation_data_json TEXT NOT NULL DEFAULT '{}',
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  FOREIGN KEY (visit_id) REFERENCES rwa_visits(visit_id),
  FOREIGN KEY (asset_id) REFERENCES rwa_assets(asset_id)
);

CREATE TABLE IF NOT EXISTS rwa_photos (
  photo_id TEXT PRIMARY KEY,
  visit_id TEXT NOT NULL,
  asset_id TEXT NOT NULL,
  observation_id TEXT,
  photo_filename TEXT NOT NULL,
  photo_hash_sha256 TEXT,
  ingest_mode TEXT NOT NULL DEFAULT 'direct_capture',
  photo_status TEXT NOT NULL DEFAULT 'active',
  captured_at_utc TEXT,
  added_to_record_at_utc TEXT,
  photo_data_json TEXT NOT NULL DEFAULT '{}',
  FOREIGN KEY (visit_id) REFERENCES rwa_visits(visit_id),
  FOREIGN KEY (asset_id) REFERENCES rwa_assets(asset_id),
  FOREIGN KEY (observation_id) REFERENCES rwa_observations(observation_id)
);

CREATE TABLE IF NOT EXISTS rwa_attachments (
  attachment_id TEXT PRIMARY KEY,
  visit_id TEXT NOT NULL,
  asset_id TEXT NOT NULL,
  observation_id TEXT,
  attachment_filename TEXT NOT NULL,
  attachment_hash_sha256 TEXT,
  attachment_kind TEXT NOT NULL DEFAULT 'other',
  attachment_status TEXT NOT NULL DEFAULT 'active',
  added_to_record_at_utc TEXT NOT NULL,
  attachment_data_json TEXT NOT NULL DEFAULT '{}',
  FOREIGN KEY (visit_id) REFERENCES rwa_visits(visit_id),
  FOREIGN KEY (asset_id) REFERENCES rwa_assets(asset_id),
  FOREIGN KEY (observation_id) REFERENCES rwa_observations(observation_id)
);

CREATE TABLE IF NOT EXISTS rwa_issuances (
  issuance_id TEXT PRIMARY KEY,
  visit_id TEXT NOT NULL,
  asset_id TEXT NOT NULL,
  certificate_status TEXT NOT NULL DEFAULT 'not_issued',
  zip_status TEXT NOT NULL DEFAULT 'not_issued',
  issued_at_utc TEXT,
  root_hash_sha256 TEXT,
  manifest_hash_sha256 TEXT,
  issuance_data_json TEXT NOT NULL DEFAULT '{}',
  FOREIGN KEY (visit_id) REFERENCES rwa_visits(visit_id),
  FOREIGN KEY (asset_id) REFERENCES rwa_assets(asset_id)
);

CREATE TABLE IF NOT EXISTS rwa_deliveries (
  delivery_id TEXT PRIMARY KEY,
  issuance_id TEXT NOT NULL,
  target_email TEXT,
  email_status TEXT NOT NULL DEFAULT 'not_sent',
  verify_count INTEGER NOT NULL DEFAULT 0,
  zip_download_count INTEGER NOT NULL DEFAULT 0,
  delivery_data_json TEXT NOT NULL DEFAULT '{}',
  created_at_utc TEXT NOT NULL,
  FOREIGN KEY (issuance_id) REFERENCES rwa_issuances(issuance_id)
);
