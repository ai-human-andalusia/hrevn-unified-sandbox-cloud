# Real Estate V2 Foundation

This schema resets the Real Estate vertical around a single database with two active subgroups:

- building_admin
- property_manager

Design rules:

- one database for the Real Estate vertical
- one shared core schema for both subgroups
- subgroup-specific attributes live in JSON fields
- certificates, zip packages and deliveries remain explicit first-class tables
- direct capture and manual upload are distinguished formally
- future subgroups can be added without multiplying physical tables

Tables:

- re_accounts
- re_enterprises
- re_assets
- re_visits
- re_observations
- re_photos
- re_attachments
- re_issuances
- re_deliveries

Core operational fields stay as columns.
Subtype-specific fields stay in JSON.

## Operational capture rules

- direct capture remains open only while evidence is being captured from the camera flow
- if no new direct capture arrives for 10 minutes, the direct capture session closes automatically
- once the direct capture session closes, new evidence can still be added manually before final issuance
- manual additions do not block validation or certificate issuance by themselves
- the protocol must declare in output how many items entered as direct capture and how many entered as manual upload
- the protocol must declare the direct capture window duration in the final output
- login inactivity timeout remains separate from capture timeout and is set to 15 minutes
