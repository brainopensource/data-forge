"""
SQLite Data Source Plugin for DataForge

This plugin provides connectivity to SQLite database files,
allowing users to query data and import it into DataForge.

Features:
- Connect to SQLite database files
- Execute SQL queries
- Browse available tables
- Data type inference
- Query validation
- Read-only mode support
"""

import sqlite3
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

from frontend.core.interfaces.plugin_interfaces import (
    IDataSourcePlugin, ConnectionConfig, DataSourceType, PluginConfigBuilder
)


class SQLiteDataSourcePlugin(IDataSourcePlugin):
    """SQLite data source plugin implementation."""
    
    def __init__(self):
        self._connections: Dict[str, sqlite3.Connection] = {}
    
    def get_name(self) -> str:
        return "SQLite Data Source"
    
    def get_version(self) -> str:
        return "1.0.0"
    
    def get_description(self) -> str:
        return "Connect to SQLite database files and execute queries"
    
    def get_data_source_type(self) -> DataSourceType:
        return DataSourceType.DATABASE
    
    def get_connection_config(self) -> ConnectionConfig:
        """Return connection configuration for SQLite."""
        fields = [
            PluginConfigBuilder.file_field(
                label="Database File",
                required=True,
                file_types=["*.db", "*.sqlite", "*.sqlite3"]
            ),
            PluginConfigBuilder.number_field(
                label="Connection Timeout (seconds)",
                default=30,
                min_value=1,
                max_value=300
            ),
            PluginConfigBuilder.checkbox_field(
                label="Read Only Mode",
                default=False
            ),
            PluginConfigBuilder.text_field(
                label="Custom SQL Query (optional)",
                placeholder="SELECT * FROM table_name LIMIT 100"
            )
        ]
        
        validation_rules = [
            {
                "field": "database_path",
                "rule": "file_exists",
                "message": "Database file must exist"
            },
            {
                "field": "database_path", 
                "rule": "file_extension",
                "extensions": [".db", ".sqlite", ".sqlite3"],
                "message": "File must be a SQLite database"
            }
        ]
        
        return ConnectionConfig(
            fields=fields,
            validation_rules=validation_rules,
            connection_test_query="SELECT 1",
            supports_streaming=False,
            max_batch_size=10000
        )
    
    def test_connection(self, config: Dict[str, Any]) -> Tuple[bool, str]:
        """Test connection to SQLite database."""
        try:
            db_path = config.get("database_path", "")
            if not db_path:
                return False, "Database path is required"
            
            db_file = Path(db_path)
            if not db_file.exists():
                return False, f"Database file not found: {db_path}"
            
            # Try to connect
            read_only = config.get("read_only", False)
            timeout = config.get("timeout", 30)
            
            if read_only:
                conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=timeout)
            else:
                conn = sqlite3.connect(db_path, timeout=timeout)
            
            # Test with a simple query
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            
            conn.close()
            return True, "Connection successful"
            
        except sqlite3.Error as e:
            return False, f"SQLite error: {str(e)}"
        except Exception as e:
            return False, f"Connection error: {str(e)}"
    
    def fetch_data(self, config: Dict[str, Any], query: Optional[str] = None, 
                   limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch data from SQLite database."""
        try:
            # Get connection
            conn = self._get_connection(config)
            cursor = conn.cursor()
            
            # Use provided query or default
            if query:
                sql_query = query
            else:
                # Default: get data from first table
                tables = self.get_available_tables(config)
                if not tables:
                    return []
                sql_query = f"SELECT * FROM {tables[0]}"
            
            # Add limit if specified
            if limit and "LIMIT" not in sql_query.upper():
                sql_query += f" LIMIT {limit}"
            
            # Execute query
            cursor.execute(sql_query)
            
            # Get column names
            column_names = [description[0] for description in cursor.description]
            
            # Fetch data
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries
            result = []
            for row in rows:
                record = {}
                for i, value in enumerate(row):
                    # Handle SQLite data types
                    if value is None:
                        record[column_names[i]] = None
                    elif isinstance(value, (int, float, str)):
                        record[column_names[i]] = value
                    else:
                        # Convert other types to string
                        record[column_names[i]] = str(value)
                result.append(record)
            
            return result
            
        except sqlite3.Error as e:
            raise Exception(f"SQLite error: {str(e)}")
        except Exception as e:
            raise Exception(f"Data fetch error: {str(e)}")
    
    def get_schema(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Get database schema information."""
        try:
            conn = self._get_connection(config)
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            schema = {
                "database_type": "sqlite",
                "database_path": config.get("database_path"),
                "tables": {}
            }
            
            # Get schema for each table
            for table_name in tables:
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
                table_schema = {
                    "columns": [],
                    "primary_keys": [],
                    "row_count": 0
                }
                
                for column in columns:
                    col_info = {
                        "name": column[1],
                        "type": column[2],
                        "nullable": not bool(column[3]),
                        "primary_key": bool(column[5])
                    }
                    table_schema["columns"].append(col_info)
                    
                    if col_info["primary_key"]:
                        table_schema["primary_keys"].append(col_info["name"])
                
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                table_schema["row_count"] = cursor.fetchone()[0]
                
                schema["tables"][table_name] = table_schema
            
            return schema
            
        except sqlite3.Error as e:
            raise Exception(f"SQLite error: {str(e)}")
        except Exception as e:
            raise Exception(f"Schema error: {str(e)}")
    
    def get_available_tables(self, config: Dict[str, Any]) -> List[str]:
        """Get list of available tables."""
        try:
            conn = self._get_connection(config)
            cursor = conn.cursor()
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            return [row[0] for row in cursor.fetchall()]
            
        except sqlite3.Error as e:
            raise Exception(f"SQLite error: {str(e)}")
        except Exception as e:
            raise Exception(f"Tables query error: {str(e)}")
    
    def validate_query(self, config: Dict[str, Any], query: str) -> Tuple[bool, str]:
        """Validate SQL query syntax."""
        try:
            conn = self._get_connection(config)
            cursor = conn.cursor()
            
            # Try to explain the query (this validates syntax without executing)
            cursor.execute(f"EXPLAIN QUERY PLAN {query}")
            return True, "Query syntax is valid"
            
        except sqlite3.Error as e:
            return False, f"Query validation error: {str(e)}"
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    def supports_streaming(self) -> bool:
        """SQLite doesn't support streaming."""
        return False
    
    def _get_connection(self, config: Dict[str, Any]) -> sqlite3.Connection:
        """Get or create connection to database."""
        db_path = config.get("database_path", "")
        connection_key = f"{db_path}_{config.get('read_only', False)}"
        
        if connection_key not in self._connections:
            read_only = config.get("read_only", False)
            timeout = config.get("timeout", 30)
            
            if read_only:
                conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=timeout)
            else:
                conn = sqlite3.connect(db_path, timeout=timeout)
            
            # Configure connection
            conn.row_factory = sqlite3.Row  # Enable column access by name
            self._connections[connection_key] = conn
        
        return self._connections[connection_key]
    
    def cleanup(self) -> None:
        """Clean up database connections."""
        for conn in self._connections.values():
            try:
                conn.close()
            except:
                pass
        self._connections.clear()
    
    def on_configuration_updated(self, config: Dict[str, Any]) -> None:
        """Handle configuration updates."""
        # Close existing connections when configuration changes
        self.cleanup()
