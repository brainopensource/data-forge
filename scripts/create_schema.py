import requests
import json

# Configuration
API_BASE_URL = "http://127.0.0.1:8080"
SCHEMA_NAME = "well_production"

# The definition for the new version of the schema.
# The 'name' is taken from the URL, and the 'version' is assigned by the service.
SCHEMA_DEFINITION = {
    "description": "Schema for well production data.",
    "table_name": "well_production",
    "primary_key": ["field_code", "well_code", "production_period"],
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

def register_new_schema_version():
    """
    Sends a POST request to register a new version of the well_production schema.
    """
    endpoint = f"{API_BASE_URL}/schemas/{SCHEMA_NAME}"
    headers = {"Content-Type": "application/json"}
    
    print(f"Sending POST request to: {endpoint}")
    print("Request body:")
    print(json.dumps(SCHEMA_DEFINITION, indent=2))
    
    try:
        response = requests.post(endpoint, data=json.dumps(SCHEMA_DEFINITION), headers=headers)
        
        # Check for successful response
        if response.status_code == 201:
            print("\nSuccessfully registered new schema version!")
            print("Response:")
            print(json.dumps(response.json(), indent=2))
        else:
            print(f"\nError: Received status code {response.status_code}")
            print("Response content:")
            print(response.text)
            
    except requests.exceptions.RequestException as e:
        print(f"\nAn error occurred while making the request: {e}")
        print("Please ensure the FastAPI application is running at the specified address.")

if __name__ == "__main__":
    register_new_schema_version() 