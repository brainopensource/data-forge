# Production-Ready Schema Manager: A Version-Centric Implementation Plan

This report details the architecture and implementation steps for a production-grade, versioned schema management system. This approach is fundamental for real-world applications where schemas evolve over time, ensuring data integrity and preventing breaking changes for consumers.

---

## 1. Core Concepts: Schema Immutability and Versioning

The core principle is that **schemas, once registered, are immutable**. They cannot be changed. Instead, to evolve a schema, you register a new, incremental version.

- **Backward-Compatible Change:** A change that does not break existing consumers. Example: adding a new *optional* field.
- **Breaking Change:** A change that will break existing consumers. Example: changing a field's data type, renaming a field, or adding a *required* field.

When a breaking change is needed, a new schema version is registered, and a separate **data migration** process is required to convert existing data to the new version's format.

---

## 2. Versioned Directory Structure

To support versioning cleanly, the storage directory will be structured by schema name, with versioned files inside.

- **New Structure:** `data/schemas/{schema_name}/{version_number}.json`
- **Example:**
    - `data/schemas/well_production/1.json`
    - `data/schemas/well_production/2.json`

This structure is intuitive and scales well.

---

## 3. Versioned API Design

The API must be redesigned to be version-aware.

| Method | Endpoint                             | Description                                                                 |
|--------|--------------------------------------|-----------------------------------------------------------------------------|
| `POST` | `/schemas/{schema_name}`             | Registers a new version of a schema. The service assigns the next version number. |
| `GET`  | `/schemas`                           | Lists all schema *families* available (e.g., "well_production").            |
| `GET`  | `/schemas/{schema_name}`             | Lists all available versions for the specified schema (e.g., `[1, 2, 3]`).   |
| `GET`  | `/schemas/{schema_name}/latest`      | Gets the full schema definition for the most recent version.                |
| `GET`  | `/schemas/{schema_name}/{version}` | Gets the full schema definition for a specific version.                     |
| `DELETE`| `/schemas/{schema_name}`             | (Soft) Deletes an entire schema family, archiving all its versions.         |

---

## 4. Repository Layer (`FileSchemaRepository`)

The repository is updated to manage the versioned directory structure. Its responsibility remains simple: interact with the file system.

```python
# app/infrastructure/persistence/repositories/schema_repository.py

import json
import os
import shutil
from pathlib import Path
from typing import Dict, Any, List, Optional

from app.domain.exceptions.exceptions import SchemaNotFoundException

class FileSchemaRepository:
    def __init__(self, schema_dir: str = "data/schemas", archive_dir: str = "data/schemas_archive"):
        self.schema_path = Path(schema_dir)
        self.archive_path = Path(archive_dir)
        self.schema_path.mkdir(parents=True, exist_ok=True)
        self.archive_path.mkdir(parents=True, exist_ok=True)

    def get_versions_for_schema(self, schema_name: str) -> List[int]:
        schema_dir = self.schema_path / schema_name
        if not schema_dir.is_dir():
            return []
        versions = [int(p.stem) for p in schema_dir.glob("*.json")]
        versions.sort()
        return versions

    def load_definition(self, schema_name: str, version: int) -> Dict[str, Any]:
        file_path = self.schema_path / schema_name / f"{version}.json"
        if not file_path.exists():
            raise SchemaNotFoundException(f"Schema '{schema_name}' version {version} not found.")
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def save_definition(self, schema_name: str, version: int, schema_definition: Dict[str, Any]):
        schema_dir = self.schema_path / schema_name
        schema_dir.mkdir(parents=True, exist_ok=True)
        file_path = schema_dir / f"{version}.json"
        
        # Add version to the definition itself for self-documentation
        schema_definition['version'] = version

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(schema_definition, f, indent=4)

    def archive_schema_family(self, schema_name: str):
        """Moves the entire schema directory to the archive."""
        source_dir = self.schema_path / schema_name
        if not source_dir.is_dir():
            raise SchemaNotFoundException(f"Schema family '{schema_name}' not found.")
        
        destination_dir = self.archive_path / schema_name
        # To avoid errors, remove old archive if it exists
        if destination_dir.exists():
            shutil.rmtree(destination_dir)
            
        shutil.move(str(source_dir), str(self.archive_path))
```

---

## 5. Service Layer (`SchemaService`)

The service layer contains the core logic for versioning, caching, and schema lifecycle management.

```diff
# High-level diff for app/application/services/schema_service.py

- self._schema_cache: Dict[str, Schema]
+ self._schema_cache: Dict[str, Dict[int, Schema]]

- def _load_schemas(self): ...
+ def _load_schemas(self):
+     # Scan all schema directories
+     # For each schema, load all its versions into the new cache structure

- def get_schema(self, schema_name: str) -> Optional[Schema]: ...
+ def get_schema(self, schema_name: str, version: Optional[int] = None) -> Schema:
+     # If version is None, get latest, otherwise get specific version.
+     # Raise SchemaNotFoundException if not found.

+ def get_latest_schema_version(self, schema_name: str) -> int: ...

+ def list_schema_versions(self, schema_name: str) -> List[int]: ...

- def create_or_update_schema(...)
+ def register_new_schema_version(self, schema_name: str, schema_definition: Dict[str, Any]) -> Schema:
+     # 1. Get latest version number for the schema_name. If none, next_version is 1.
+     # 2. (Optional but Recommended) Perform compatibility check against latest version.
+     # 3. Call repository to save the definition with the new version.
+     # 4. Create Schema object and update the in-memory cache.
+     # 5. Return the newly created Schema object.

- def delete_schema(...)
+ def delete_schema_family(self, schema_name: str):
+     # 1. Call repository's archive_schema_family method.
+     # 2. Clear the corresponding entries from the in-memory cache.

```

---

## 6. API Layer (`schemas.py`)

The API router implementation needs to be updated to match the new versioned design.

```diff
# High-level diff for app/api/routes/schemas.py

# Remove old POST and DELETE endpoints. Add new versioned endpoints.

+ @router.post("/{schema_name}", status_code=201)
+ async def register_schema_version(...):
+     # Calls schema_service.register_new_schema_version
+     # Returns the full definition of the newly created schema version.

+ @router.get("/")
+ async def list_schema_families(...): ...

+ @router.get("/{schema_name}")
+ async def list_versions_for_schema(...): ...

+ @router.get("/{schema_name}/latest")
+ async def get_latest_schema(...): ...

+ @router.get("/{schema_name}/{version}")
+ async def get_specific_schema_version(...): ...

+ @router.delete("/{schema_name}", status_code=204)
+ async def delete_schema_family(...):
+     # Calls schema_service.delete_schema_family

```

---

## 7. Data Migration Strategy

This is a **critical operational process**, not automated application code. It's performed deliberately when a breaking schema change is introduced.

**Workflow:**
1.  **Register New Schema:** A developer registers `schema:v2` which contains a breaking change (e.g., `field_a` data type changed from `string` to `int`).
2.  **Create Migration Script:** A separate Python script is created in the `scripts/` directory (e.g., `migrate_well_production_v1_to_v2.py`).
3.  **Implement Script Logic:** This script uses the high-performance capabilities of Polars.

**Example Migration Script Skeleton:**

```python
# scripts/migrate_well_production_v1_to_v2.py
import polars as pl
import pyarrow.parquet as pq

# Assumes data for each schema version is stored in a separate location.
SOURCE_DATA_PATH = "data/raw/well_production/v1/"
DESTINATION_DATA_PATH = "data/raw/well_production/v2/"

def migrate_file(file_path: str):
    # 1. Read data written with v1 of the schema
    df = pl.read_parquet(file_path)

    # 2. Apply transformations to conform to v2
    #    This is the core logic for the breaking change.
    df = df.with_columns(
        pl.col("field_code").cast(pl.Int64, strict=False),  # Example: change type
        pl.col("new_required_field").fill_null("default_value") # Example: add required field
    ).rename({"old_name": "new_name"}) # Example: rename field

    # 3. Write transformed data to the new location
    #    It's crucial to get the new PyArrow schema from the Schema Service
    #    to ensure the output file metadata is correct.
    # new_arrow_schema = schema_service.get_arrow_schema("well_production", 2)
    # df.write_parquet(
    #     f"{DESTINATION_DATA_PATH}/{file_path.name}",
    #     pyarrow_options={"schema": new_arrow_schema}
    # )

# Loop through all v1 files and apply the migration
# for file in Path(SOURCE_DATA_PATH).glob("*.parquet"):
#     migrate_file(file)
```

This version-centric plan provides a complete roadmap for a robust, production-ready schema management system capable of evolving safely over time.
