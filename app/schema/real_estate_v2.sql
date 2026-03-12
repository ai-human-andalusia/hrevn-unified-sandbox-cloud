PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS re_accounts (
  account_id TEXT PRIMARY KEY,
  user_email TEXT NOT NULL UNIQUE,
  user_phone TEXT,
  user_role TEXT NOT NULL,
  subgroup TEXT NOT NULL CHECK (subgroup IN ('building_admin', 'property_manager')),
  enterprise_id TEXT,
  account_status TEXT NOT NULL DEFAULT 'active',
  preferred_language TEXT NOT NULL DEFAULT 'en',
  profile_data_json TEXT NOT NULL DEFAULT '{}',
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS re_enterprises (
  enterprise_id TEXT PRIMARY KEY,
  enterprise_name TEXT NOT NULL,
  enterprise_type TEXT NOT NULL,
  contact_email TEXT,
  contact_phone TEXT,
  enterprise_data_json TEXT NOT NULL DEFAULT '{}',
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS re_assets (
  asset_id TEXT PRIMARY KEY,
  enterprise_id TEXT,
  asset_public_id TEXT NOT NULL UNIQUE,
  asset_type TEXT NOT NULL,
  asset_name TEXT NOT NULL,
  address_line TEXT,
  city TEXT,
  province TEXT,
  postal_code TEXT,
  country TEXT NOT NULL DEFAULT 'ES',
  asset_status TEXT NOT NULL DEFAULT 'active',
  asset_data_json TEXT NOT NULL DEFAULT '{}',
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  FOREIGN KEY (enterprise_id) REFERENCES re_enterprises(enterprise_id)
);

CREATE TABLE IF NOT EXISTS re_visits (
  visit_id TEXT PRIMARY KEY,
  asset_id TEXT NOT NULL,
  created_by_account_id TEXT,
  visit_date_utc TEXT,
  visit_status TEXT NOT NULL DEFAULT 'work',
  review_status TEXT NOT NULL DEFAULT 'pending',
  issuance_status TEXT NOT NULL DEFAULT 'not_issued',
  delivery_status TEXT NOT NULL DEFAULT 'not_delivered',
  direct_capture_session_status TEXT NOT NULL DEFAULT 'open',
  direct_capture_started_at_utc TEXT,
  direct_capture_last_activity_at_utc TEXT,
  direct_capture_closed_at_utc TEXT,
  direct_capture_closed_reason TEXT,
  direct_capture_window_minutes INTEGER NOT NULL DEFAULT 0,
  visit_data_json TEXT NOT NULL DEFAULT '{}',
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  FOREIGN KEY (asset_id) REFERENCES re_assets(asset_id),
  FOREIGN KEY (created_by_account_id) REFERENCES re_accounts(account_id)
);

CREATE TABLE IF NOT EXISTS re_observations (
  observation_id TEXT PRIMARY KEY,
  visit_id TEXT NOT NULL,
  asset_id TEXT NOT NULL,
  lpi_code TEXT,
  severity_0_5 INTEGER NOT NULL DEFAULT 0,
  observation_description TEXT,
  row_status TEXT NOT NULL DEFAULT 'work',
  in_review_flag INTEGER NOT NULL DEFAULT 0,
  observation_data_json TEXT NOT NULL DEFAULT '{}',
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  FOREIGN KEY (visit_id) REFERENCES re_visits(visit_id),
  FOREIGN KEY (asset_id) REFERENCES re_assets(asset_id)
);

CREATE TABLE IF NOT EXISTS re_photos (
  photo_id TEXT PRIMARY KEY,
  visit_id TEXT NOT NULL,
  asset_id TEXT NOT NULL,
  observation_id TEXT,
  photo_filename TEXT NOT NULL,
  photo_hash_sha256 TEXT,
  file_type TEXT NOT NULL DEFAULT 'image',
  ingest_mode TEXT NOT NULL DEFAULT 'direct_capture' CHECK (ingest_mode IN ('direct_capture', 'manual_upload')),
  photo_role TEXT NOT NULL DEFAULT 'support',
  photo_status TEXT NOT NULL DEFAULT 'active',
  captured_at_utc TEXT,
  added_to_record_at_utc TEXT,
  added_by_account_id TEXT,
  photo_data_json TEXT NOT NULL DEFAULT '{}',
  FOREIGN KEY (visit_id) REFERENCES re_visits(visit_id),
  FOREIGN KEY (asset_id) REFERENCES re_assets(asset_id),
  FOREIGN KEY (observation_id) REFERENCES re_observations(observation_id),
  FOREIGN KEY (added_by_account_id) REFERENCES re_accounts(account_id)
);

CREATE TABLE IF NOT EXISTS re_attachments (
  attachment_id TEXT PRIMARY KEY,
  visit_id TEXT NOT NULL,
  asset_id TEXT NOT NULL,
  observation_id TEXT,
  attachment_filename TEXT NOT NULL,
  attachment_hash_sha256 TEXT,
  attachment_kind TEXT NOT NULL DEFAULT 'other' CHECK (attachment_kind IN ('image', 'pdf', 'doc', 'other')),
  ingest_mode TEXT NOT NULL DEFAULT 'manual_upload' CHECK (ingest_mode IN ('manual_upload')),
  attachment_status TEXT NOT NULL DEFAULT 'active',
  added_to_record_at_utc TEXT NOT NULL,
  added_by_account_id TEXT,
  attachment_data_json TEXT NOT NULL DEFAULT '{}',
  FOREIGN KEY (visit_id) REFERENCES re_visits(visit_id),
  FOREIGN KEY (asset_id) REFERENCES re_assets(asset_id),
  FOREIGN KEY (observation_id) REFERENCES re_observations(observation_id),
  FOREIGN KEY (added_by_account_id) REFERENCES re_accounts(account_id)
);

CREATE TABLE IF NOT EXISTS re_issuances (
  issuance_id TEXT PRIMARY KEY,
  visit_id TEXT NOT NULL,
  asset_id TEXT NOT NULL,
  certificate_status TEXT NOT NULL DEFAULT 'not_issued',
  zip_status TEXT NOT NULL DEFAULT 'not_issued',
  issued_at_utc TEXT,
  root_hash_sha256 TEXT,
  manifest_hash_sha256 TEXT,
  issuance_data_json TEXT NOT NULL DEFAULT '{}',
  FOREIGN KEY (visit_id) REFERENCES re_visits(visit_id),
  FOREIGN KEY (asset_id) REFERENCES re_assets(asset_id)
);

CREATE TABLE IF NOT EXISTS re_deliveries (
  delivery_id TEXT PRIMARY KEY,
  issuance_id TEXT NOT NULL,
  target_email TEXT,
  email_status TEXT NOT NULL DEFAULT 'not_sent',
  verify_count INTEGER NOT NULL DEFAULT 0,
  zip_download_count INTEGER NOT NULL DEFAULT 0,
  delivery_data_json TEXT NOT NULL DEFAULT '{}',
  created_at_utc TEXT NOT NULL,
  FOREIGN KEY (issuance_id) REFERENCES re_issuances(issuance_id)
);

CREATE INDEX IF NOT EXISTS idx_re_accounts_subgroup ON re_accounts(subgroup);
CREATE INDEX IF NOT EXISTS idx_re_assets_enterprise ON re_assets(enterprise_id);
CREATE INDEX IF NOT EXISTS idx_re_visits_asset ON re_visits(asset_id);
CREATE INDEX IF NOT EXISTS idx_re_observations_visit ON re_observations(visit_id);
CREATE INDEX IF NOT EXISTS idx_re_photos_visit ON re_photos(visit_id);
CREATE INDEX IF NOT EXISTS idx_re_attachments_visit ON re_attachments(visit_id);
CREATE INDEX IF NOT EXISTS idx_re_issuances_visit ON re_issuances(visit_id);
CREATE INDEX IF NOT EXISTS idx_re_deliveries_issuance ON re_deliveries(issuance_id);
