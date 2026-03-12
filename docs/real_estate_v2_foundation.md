# Real Estate V2 Foundation

This schema resets the Real Estate vertical around a single database with two active subgroups:

- building_admin
- property_manager

Design rules:

- one database for the Real Estate vertical
- one shared core schema for both subgroups
- subgroup-specific attributes live in JSON fields
- certificates, zip packages and deliveries remain explicit first-class tables
- future subgroups can be added without multiplying physical tables

Tables:

- re_accounts
- re_enterprises
- re_assets
- re_visits
- re_observations
- re_photos
- re_issuances
- re_deliveries

Core operational fields stay as columns.
Subtype-specific fields stay in JSON.
