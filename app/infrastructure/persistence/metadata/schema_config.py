# This file defines the schemas for the application.
# The schemas are defined as a list of dictionaries, which are then used to create Schema objects.

SCHEMAS_METADATA = [
    {
        "name": "fields_aliases",
        "description": "Schema for fields prices data.",
        "table_name": "fields_prices",
        "primary_key": ["field_code"],  # Single natural key
        "properties": [
            {"name": "field_code", "type": "integer", "db_type": "BIGINT", "required": True, "primary_key": True},
            {"name": "field_name", "type": "string", "db_type": "VARCHAR", "required": True}
        ],
    },
    {
        "name": "well_production",
        "description": "Schema for well production data.",
        "table_name": "well_production",
        "primary_key": ["field_code", "well_code", "production_period"],  # Composite natural key
        "properties": [
            {"name": "field_code", "type": "integer", "db_type": "BIGINT", "required": True, "primary_key": True},
            {"name": "field_name", "type": "string", "db_type": "VARCHAR"},
            {"name": "well_code", "type": "integer", "db_type": "BIGINT", "required": True, "primary_key": True},
            {"name": "well_reference", "type": "string", "db_type": "VARCHAR"},
            {"name": "well_name", "type": "string", "db_type": "VARCHAR"},
            {"name": "production_period", "type": "string", "db_type": "TIMESTAMP", "required": True, "primary_key": True},
            {"name": "days_on_production", "type": "integer", "db_type": "BIGINT"},
            {"name": "oil_production_kbd", "type": "number", "db_type": "DOUBLE"},
            {"name": "gas_production_mmcfd", "type": "number", "db_type": "DOUBLE"},
            {"name": "liquids_production_kbd", "type": "number", "db_type": "DOUBLE"},
            {"name": "water_production_kbd", "type": "number", "db_type": "DOUBLE"},
            {"name": "data_source", "type": "string", "db_type": "VARCHAR"},
            {"name": "source_data", "type": "string", "db_type": "VARCHAR"},
            {"name": "partition_0", "type": "string", "db_type": "VARCHAR"},
        ],
    }
]