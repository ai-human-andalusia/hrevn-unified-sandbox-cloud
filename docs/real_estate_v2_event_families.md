# Real Estate V2 Event Families

This document defines how `real_estate_v2` should support two business families inside the same vertical without duplicating the whole database:

- rental inspection / entry-exit evidence
- community incident / ad hoc evidence

The goal is:
- one Real Estate vertical
- one common core schema
- two event families
- subgroup-driven UI and validation

## 1. Business decision

The database should not be split into two separate verticals for:
- `building_admin`
- `property_manager`
- `family_office`
- `investment_fund`

Instead:
- `property_manager`, `family_office`, and `investment_fund` should share the same **rental inspection family**
- `building_admin` should use a **community incident family**

This keeps:
- one Real Estate vertical
- one asset model
- one issuance model
- one delivery model

while allowing different event logic.

## 2. Existing common core that should remain shared

The current `real_estate_v2` schema already gives us a useful common core:

- `re_accounts`
- `re_enterprises`
- `re_assets`
- `re_units`
- `re_account_asset_links`
- `re_visits`
- `re_observations`
- `re_photos`
- `re_attachments`
- `re_issuances`
- `re_deliveries`

This core should remain common.

## 3. Proposed event families

### Family A: rental inspection

Target subgroups:
- `property_manager`
- `family_office`
- `investment_fund`

Typical use cases:
- move-in check
- move-out check
- occupancy handover
- pre-rental condition record
- post-rental damage record

This family is centered on:
- property condition
- unit-level inspection
- tenant turnover
- visual evidence of condition

### Family B: community incident

Target subgroup:
- `building_admin`

Typical use cases:
- common-area incident
- maintenance issue
- damage in staircase/lift/roof/facade
- community follow-up record
- evidence for service providers or owners

This family is centered on:
- incidents
- common-area location
- urgency
- maintenance follow-up
- evidence collected when needed, not necessarily tied to tenant turnover

## 4. Proposed common event header

The current table `re_visits` should act as the shared event header.

Recommended evolution:
- keep `re_visits` as the event header table
- add the following columns over time:

Common header additions:
- `event_family`
- `event_type`
- `event_reference_code`
- `closure_mode`

Recommended meanings:
- `event_family`
  - `rental_inspection`
  - `community_incident`
- `event_type`
  - examples: `move_in`, `move_out`, `incident`, `follow_up`
- `event_reference_code`
  - human-readable code if needed for UI/certificate
- `closure_mode`
  - `manual_close`
  - `auto_close_capture_timeout`
  - `closed_after_review`

This keeps one event table but gives it enough semantic routing.

## 5. Family-specific detail tables

Do not put every family-specific field directly in `re_visits`.

Instead, add two detail tables:

### `re_rental_event_details`

Suggested fields:
- `event_id` (FK to `re_visits.visit_id`)
- `inspection_context`
- `occupancy_state`
- `handover_party`
- `tenant_reference`
- `contract_reference`
- `meter_reading_note`
- `keys_handover_note`
- `event_detail_json`

Use:
- captures the details specific to rental entry/exit flows

### `re_incident_event_details`

Suggested fields:
- `event_id` (FK to `re_visits.visit_id`)
- `incident_area`
- `incident_type`
- `urgency_level`
- `reported_by`
- `maintenance_context`
- `resolution_target`
- `event_detail_json`

Use:
- captures the details specific to community incidents and follow-up

## 6. Asset and unit model

The current model already supports:
- one enterprise with many assets
- one asset with many units

This is good and should remain.

Recommended interpretation:
- rental inspections often happen at **unit** level
- community incidents often happen at **asset** level or common-area level

So:
- rental family should usually use `unit_id`
- building admin family may often leave `unit_id` null and work at asset scope

## 7. Subgroup model

### Recommended Real Estate subgroups going forward

The current schema only supports:
- `building_admin`
- `property_manager`

Recommended target list:
- `building_admin`
- `property_manager`
- `family_office`
- `investment_fund`

Interpretation:
- `property_manager`, `family_office`, `investment_fund` route to the same family: `rental_inspection`
- `building_admin` routes to `community_incident`

## 8. UI routing rule

### Admin habitat
Should see:
- enterprise/account setup
- assignments
- counts
- lifecycle
- support/security view

### User habitat
If subgroup is:
- `property_manager`
- `family_office`
- `investment_fund`

then the user should land in:
- rental inspection workflow

### Enterprise habitat
Should see:
- portfolio view
- assets/units
- event/certificate summaries
- support/tickets

### Building admin user
Should land in:
- community incident workflow

## 9. Observation and evidence model

`re_observations`, `re_photos`, and `re_attachments` should remain common.

Why:
- both families need findings
- both families need severity
- both families need evidence
- both families need output and delivery

The difference should come from:
- family-specific rules
- family-specific labels
- family-specific required fields

Not from duplicating the entire evidence model.

## 10. Output model

Output can still stay under one Real Estate issuance family if needed.

But the templates should eventually branch by `event_family`:
- rental inspection output
- community incident output

Both can still reuse:
- issuance
- delivery
- manifest
- zip
- verification

## 11. Migration strategy

### Phase 1
Keep current `real_estate_v2` as core and document the split.

### Phase 2
Add subgroup values:
- `family_office`
- `investment_fund`

### Phase 3
Add event-family semantics to `re_visits`.

### Phase 4
Add:
- `re_rental_event_details`
- `re_incident_event_details`

### Phase 5
Route UI by subgroup/family:
- rental inspection UI
- community incident UI

## 12. Recommendation

Do not duplicate the full Real Estate database.

Do this instead:
- keep one Real Estate V2 database
- keep one common asset/evidence/issuance model
- split only the event-family details and UI flow

This is the cleanest balance between:
- reuse
- flexibility
- maintainability
- and product clarity.
