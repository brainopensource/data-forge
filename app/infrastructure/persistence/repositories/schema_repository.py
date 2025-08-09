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

    def get_all_schema_names(self) -> List[str]:
        if not self.schema_path.is_dir():
            return []
        return [d.name for d in self.schema_path.iterdir() if d.is_dir()]

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
