"""Fivetran to Lakeflow Connect migration toolkit.

Modules:
    fivetran   -- Fivetran REST API client
    inventory  -- inventory document model and summarisation
    catalog    -- connector capability catalog (Fivetran service -> Lakeflow Connect)
    mapping    -- migration plan builder
    mar        -- Fivetran Platform Connector MAR and billing loading
    consumption -- Fivetran consumption and spend export for the account team
    bundle     -- Databricks Asset Bundle emitter
"""

__version__ = "0.1.0"

INVENTORY_SCHEMA_VERSION = "1"
PLAN_SCHEMA_VERSION = "1"
