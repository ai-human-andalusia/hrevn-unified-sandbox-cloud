# Real Estate V2 Field Classification

This file classifies the Real Estate V2 model into:

- common fields
- building_admin fields
- property_manager fields
- extensible JSON fields

## 1. Common account fields

Table: `re_accounts`

Fixed columns:
- `account_id`
- `user_email`
- `user_role`
- `subgroup`
- `enterprise_id`
- `account_status`
- `preferred_language`
- `created_at_utc`
- `updated_at_utc`

JSON extension:
- `profile_data_json`

Common use:
- identity
- role
- subgroup routing
- enterprise linkage
- language and status

## 2. Common enterprise fields

Table: `re_enterprises`

Fixed columns:
- `enterprise_id`
- `enterprise_name`
- `enterprise_type`
- `contact_email`
- `contact_phone`
- `created_at_utc`
- `updated_at_utc`

JSON extension:
- `enterprise_data_json`

Common use:
- legal/account ownership
- contact and routing

## 3. Common asset fields

Table: `re_assets`

Fixed columns:
- `asset_id`
- `enterprise_id`
- `asset_public_id`
- `asset_type`
- `asset_name`
- `address_line`
- `city`
- `province`
- `postal_code`
- `country`
- `asset_status`
- `created_at_utc`
- `updated_at_utc`

JSON extension:
- `asset_data_json`

Common use:
- identify the property or managed asset
- provide location and status

## 4. Common visit fields

Table: `re_visits`

Fixed columns:
- `visit_id`
- `asset_id`
- `created_by_account_id`
- `visit_date_utc`
- `visit_status`
- `review_status`
- `issuance_status`
- `delivery_status`
- `created_at_utc`
- `updated_at_utc`

JSON extension:
- `visit_data_json`

Common use:
- event/visit lifecycle
- review/issuance/delivery tracking

## 5. Common observation fields

Table: `re_observations`

Fixed columns:
- `observation_id`
- `visit_id`
- `asset_id`
- `lpi_code`
- `severity_0_5`
- `observation_description`
- `row_status`
- `in_review_flag`
- `created_at_utc`
- `updated_at_utc`

JSON extension:
- `observation_data_json`

Common use:
- visible findings
- structured issue severity
- review blocking or approval

## 6. Common photo fields

Table: `re_photos`

Fixed columns:
- `photo_id`
- `visit_id`
- `asset_id`
- `observation_id`
- `photo_filename`
- `photo_hash_sha256`
- `photo_role`
- `photo_status`
- `captured_at_utc`

JSON extension:
- `photo_data_json`

Common use:
- evidence integrity
- linking each photo to visit/asset/observation

## 7. Common issuance fields

Table: `re_issuances`

Fixed columns:
- `issuance_id`
- `visit_id`
- `asset_id`
- `certificate_status`
- `zip_status`
- `issued_at_utc`
- `root_hash_sha256`
- `manifest_hash_sha256`

JSON extension:
- `issuance_data_json`

Common use:
- certificate and ZIP generation
- integrity hashes

## 8. Common delivery fields

Table: `re_deliveries`

Fixed columns:
- `delivery_id`
- `issuance_id`
- `target_email`
- `email_status`
- `verify_count`
- `zip_download_count`
- `created_at_utc`

JSON extension:
- `delivery_data_json`

Common use:
- email delivery
- verification count
- ZIP download count

## 9. `building_admin` specific fields

Recommended storage:
- mostly in `asset_data_json`
- partly in `visit_data_json`
- partly in `observation_data_json`

Typical fields:
- `community_name`
- `building_block`
- `staircase`
- `floor_reference`
- `common_area_scope`
- `incident_scope`
- `neighbor_impact_level`
- `building_urgency_level`
- `maintenance_context`
- `community_reference_code`

When to promote to fixed columns:
- only if queried constantly across most records

## 10. `property_manager` specific fields

Recommended storage:
- mostly in `asset_data_json`
- partly in `visit_data_json`
- partly in `observation_data_json`

Typical fields:
- `tenant_status`
- `occupancy_status`
- `rental_context`
- `handover_stage`
- `maintenance_vendor`
- `tenant_incident_flag`
- `commercial_priority`
- `portfolio_segment`
- `service_level`
- `property_reference_code`

When to promote to fixed columns:
- only if they become common reporting fields

## 11. JSON strategy

### `profile_data_json`
Use for:
- avatar-specific preferences
- UI defaults
- subgroup-specific metadata

### `enterprise_data_json`
Use for:
- billing profile
- legal references
- enterprise-specific preferences

### `asset_data_json`
Use for:
- subgroup-specific property descriptors
- business context not common to all Real Estate actors

### `visit_data_json`
Use for:
- workflow-specific capture fields
- scheduling context
- service context

### `observation_data_json`
Use for:
- subgroup-specific classification
- business severity overlays
- review annotations

### `photo_data_json`
Use for:
- AI review details
- photo title proposals
- quality flags
- capture notes

### `issuance_data_json`
Use for:
- issuance wording
- template profile
- blockchain/anchor metadata

### `delivery_data_json`
Use for:
- delivery channel metadata
- resend history
- download events detail

## 12. Practical rule

- fixed columns for common logic, filtering and counting
- JSON for subgroup-specific richness
- promote JSON fields to fixed columns only when they become stable and heavily used
