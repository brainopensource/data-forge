"""
Data Controller

Handles data loading, processing, and state management following SOLID principles.
Extracts data management logic from the main application for better separation of concerns.
"""

import threading
from typing import Any, Optional, Dict, List, Callable, Union, Tuple
from datetime import datetim            elif isinstance(data, list) and data:
                metadata.record_count = len(data)
                if hasattr(data, 'shape'):
                    metadata.record_count = data.shape[0]  # type: ignore
                    if hasattr(data, 'columns'):
                        metadata.columns = list(data.columns)  # type: ignore
                        metadata.column_count = len(metadata.columns)
            elif hasattr(data, 'shape'):
                metadata.record_count = data.shape[0]  # type: ignore
                if hasattr(data, 'columns'):
                    metadata.columns = list(data.columns)  # type: ignore
                    metadata.column_count = len(metadata.columns)um import Enum
from dataclasses import dataclass, field
import json


class DataSource(Enum):
    """Enum for different data sources"""
    API = "api"
    GENERATED = "generated"
    FILE = "file"
    EXTERNAL = "external"
    UNKNOWN = "unknown"


class DataStatus(Enum):
    """Enum for data loading status"""
    IDLE = "idle"
    LOADING = "loading"
    LOADED = "loaded"
    ERROR = "error"
    PROCESSING = "processing"


@dataclass
class DataMetadata:
    """Metadata about loaded data"""
    source: DataSource = DataSource.UNKNOWN
    source_details: str = ""
    record_count: int = 0
    column_count: int = 0
    columns: List[str] = field(default_factory=list)
    loaded_at: Optional[datetime] = None
    data_type: str = "unknown"
    size_bytes: Optional[int] = None
    schema_name: Optional[str] = None
    compression: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary."""
        return {
            'source': self.source.value,
            'source_details': self.source_details,
            'record_count': self.record_count,
            'column_count': self.column_count,
            'columns': self.columns,
            'loaded_at': self.loaded_at.isoformat() if self.loaded_at else None,
            'data_type': self.data_type,
            'size_bytes': self.size_bytes,
            'schema_name': self.schema_name,
            'compression': self.compression
        }


class DataController:
    """
    Handles data loading, processing, and state management.
    
    Responsibilities:
    - Data loading from various sources (API, files, generators)
    - Data state management and thread safety
    - Data validation and error handling
    - Callback management for UI updates
    - Data metadata tracking
    - Data transformation and processing coordination
    
    Follows SOLID principles:
    - Single Responsibility: Only handles data operations
    - Open/Closed: Extensible for new data sources
    - Liskov Substitution: Consistent interface for all operations
    - Interface Segregation: Clean interfaces for different operations
    - Dependency Inversion: Accepts service instances rather than creating them
    """
    
    def __init__(self, api_client=None, data_generator=None):
        """
        Initialize the data controller.
        
        Args:
            api_client: Client for API operations
            data_generator: Service for generating sample data
        """
        self.api_client = api_client
        self.data_generator = data_generator
        
        # Thread-safe data storage
        self.current_data: Optional[Any] = None
        self.data_metadata = DataMetadata()
        self.data_lock = threading.RLock()
        self.status = DataStatus.IDLE
        
        # Callback management
        self.loading_callbacks: List[Callable[[bool], None]] = []
        self.error_callbacks: List[Callable[[str], None]] = []
        self.data_change_callbacks: List[Callable[[Any, DataMetadata], None]] = []
        self.status_callbacks: List[Callable[[DataStatus], None]] = []
        
        # Processing history
        self.operation_history: List[Dict[str, Any]] = []
        self.max_history = 50
        
    def set_api_client(self, api_client):
        """Set or update the API client."""
        self.api_client = api_client
        
    def set_data_generator(self, data_generator):
        """Set or update the data generator."""
        self.data_generator = data_generator
        
    # Callback management methods
    def add_loading_callback(self, callback: Callable[[bool], None]):
        """Add callback for loading state changes (True=started, False=ended)."""
        if callable(callback) and callback not in self.loading_callbacks:
            self.loading_callbacks.append(callback)
            
    def add_error_callback(self, callback: Callable[[str], None]):
        """Add callback for error notifications."""
        if callable(callback) and callback not in self.error_callbacks:
            self.error_callbacks.append(callback)
            
    def add_data_change_callback(self, callback: Callable[[Any, DataMetadata], None]):
        """Add callback for data changes."""
        if callable(callback) and callback not in self.data_change_callbacks:
            self.data_change_callbacks.append(callback)
            
    def add_status_callback(self, callback: Callable[[DataStatus], None]):
        """Add callback for status changes."""
        if callable(callback) and callback not in self.status_callbacks:
            self.status_callbacks.append(callback)
            
    def remove_loading_callback(self, callback: Callable[[bool], None]):
        """Remove loading callback."""
        if callback in self.loading_callbacks:
            self.loading_callbacks.remove(callback)
            
    def remove_error_callback(self, callback: Callable[[str], None]):
        """Remove error callback."""
        if callback in self.error_callbacks:
            self.error_callbacks.remove(callback)
            
    def remove_data_change_callback(self, callback: Callable[[Any, DataMetadata], None]):
        """Remove data change callback."""
        if callback in self.data_change_callbacks:
            self.data_change_callbacks.remove(callback)
            
    def remove_status_callback(self, callback: Callable[[DataStatus], None]):
        """Remove status callback."""
        if callback in self.status_callbacks:
            self.status_callbacks.remove(callback)
            
    # Data loading methods
    def load_data_from_api(self, schema_name: str, **kwargs) -> bool:
        """
        Load data from API endpoint.
        
        Args:
            schema_name: Name of the schema to load
            **kwargs: Additional parameters for API call
            
        Returns:
            True if successful, False otherwise
        """
        if not self.api_client:
            self._notify_error("No API client configured")
            return False
            
        try:
            self._set_status(DataStatus.LOADING)
            self._notify_loading_start()
            
            # Call API client
            table, count = self.api_client.read_polars(schema_name)
            
            if table is None:
                self._notify_error(f"No data returned from API for schema '{schema_name}'")
                return False
                
            # Create metadata
            metadata = DataMetadata(
                source=DataSource.API,
                source_details=f"API schema: {schema_name}",
                record_count=count,
                loaded_at=datetime.now(),
                schema_name=schema_name,
                data_type="polars_dataframe"
            )
            
            # Extract column information
            if hasattr(table, 'columns'):
                metadata.columns = list(table.columns)  # type: ignore
                metadata.column_count = len(metadata.columns)
            elif hasattr(table, 'schema'):
                metadata.columns = list(table.schema.names) if table.schema else []  # type: ignore
                metadata.column_count = len(metadata.columns)
                
            # Store data safely
            with self.data_lock:
                self.current_data = table
                self.data_metadata = metadata
                
            self._set_status(DataStatus.LOADED)
            self._notify_loading_end()
            self._notify_data_change()
            self._add_to_history("load_from_api", {"schema_name": schema_name})
            
            return True
            
        except Exception as e:
            self._set_status(DataStatus.ERROR)
            self._notify_error(f"Failed to load data from API: {str(e)}")
            self._notify_loading_end()
            return False
            
    def generate_sample_data(self, num_records: int, data_type: str = "default", **kwargs) -> bool:
        """
        Generate sample data using data generator.
        
        Args:
            num_records: Number of records to generate
            data_type: Type of data to generate
            **kwargs: Additional parameters for generation
            
        Returns:
            True if successful, False otherwise
        """
        if not self.data_generator:
            self._notify_error("No data generator configured")
            return False
            
        try:
            self._set_status(DataStatus.LOADING)
            self._notify_loading_start()
            
            # Generate data
            sample_data = self.data_generator.generate_sample_data(num_records, data_type, **kwargs)
            
            if not sample_data:
                self._notify_error("No sample data generated")
                return False
                
            # Create metadata
            metadata = DataMetadata(
                source=DataSource.GENERATED,
                source_details=f"Generated {data_type} data",
                record_count=num_records,
                loaded_at=datetime.now(),
                data_type=data_type
            )
            
            # Extract column information
            if isinstance(sample_data, list) and sample_data:
                if isinstance(sample_data[0], dict):
                    metadata.columns = list(sample_data[0].keys())
                    metadata.column_count = len(metadata.columns)
            
            # Store data safely
            with self.data_lock:
                self.current_data = sample_data
                self.data_metadata = metadata
                
            self._set_status(DataStatus.LOADED)
            self._notify_loading_end()
            self._notify_data_change()
            self._add_to_history("generate_sample", {"num_records": num_records, "data_type": data_type})
            
            return True
            
        except Exception as e:
            self._set_status(DataStatus.ERROR)
            self._notify_error(f"Failed to generate sample data: {str(e)}")
            self._notify_loading_end()
            return False
            
    def load_data_from_file(self, file_path: str, file_type: str = "auto", **kwargs) -> bool:
        """
        Load data from a file.
        
        Args:
            file_path: Path to the file
            file_type: Type of file (csv, json, parquet, etc.)
            **kwargs: Additional parameters for file loading
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self._set_status(DataStatus.LOADING)
            self._notify_loading_start()
            
            # Auto-detect file type if needed
            if file_type == "auto":
                file_type = self._detect_file_type(file_path)
                
            # Load based on file type
            data = self._load_file_by_type(file_path, file_type, **kwargs)
            
            if data is None:
                self._notify_error(f"Could not load data from file: {file_path}")
                return False
                
            # Create metadata
            metadata = DataMetadata(
                source=DataSource.FILE,
                source_details=f"File: {file_path}",
                loaded_at=datetime.now(),
                data_type=file_type
            )
            
            # Extract size and column information
            try:
                import os
                metadata.size_bytes = os.path.getsize(file_path)
            except Exception:
                pass
                
            if isinstance(data, list) and data:
                metadata.record_count = len(data)
                if isinstance(data[0], dict):
                    metadata.columns = list(data[0].keys())
                    metadata.column_count = len(metadata.columns)
            elif hasattr(data, 'shape'):
                metadata.record_count = data.shape[0]
                if hasattr(data, 'columns'):
                    metadata.columns = list(data.columns)
                    metadata.column_count = len(metadata.columns)
                    
            # Store data safely
            with self.data_lock:
                self.current_data = data
                self.data_metadata = metadata
                
            self._set_status(DataStatus.LOADED)
            self._notify_loading_end()
            self._notify_data_change()
            self._add_to_history("load_from_file", {"file_path": file_path, "file_type": file_type})
            
            return True
            
        except Exception as e:
            self._set_status(DataStatus.ERROR)
            self._notify_error(f"Failed to load data from file: {str(e)}")
            self._notify_loading_end()
            return False
            
    def set_data_directly(self, data: Any, metadata: Optional[DataMetadata] = None) -> bool:
        """
        Set data directly with optional metadata.
        
        Args:
            data: The data to set
            metadata: Optional metadata, will be generated if not provided
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self._set_status(DataStatus.PROCESSING)
            
            # Generate metadata if not provided
            if metadata is None:
                metadata = self._generate_metadata_for_data(data)
                
            # Store data safely
            with self.data_lock:
                self.current_data = data
                self.data_metadata = metadata
                
            self._set_status(DataStatus.LOADED)
            self._notify_data_change()
            self._add_to_history("set_direct", {"data_type": type(data).__name__})
            
            return True
            
        except Exception as e:
            self._set_status(DataStatus.ERROR)
            self._notify_error(f"Failed to set data directly: {str(e)}")
            return False
            
    # Data access methods
    def get_current_data(self) -> Optional[Any]:
        """Get current loaded data (thread-safe)."""
        with self.data_lock:
            return self.current_data
            
    def get_data_metadata(self) -> DataMetadata:
        """Get metadata about current data."""
        with self.data_lock:
            return self.data_metadata
            
    def get_data_copy(self) -> Optional[Any]:
        """Get a copy of the current data (when possible)."""
        with self.data_lock:
            if self.current_data is None:
                return None
                
            try:
                # Try to create a copy
                if hasattr(self.current_data, 'copy'):
                    return self.current_data.copy()
                elif hasattr(self.current_data, 'clone'):
                    return self.current_data.clone()
                elif isinstance(self.current_data, (list, dict)):
                    import copy
                    return copy.deepcopy(self.current_data)
                else:
                    # Return original if copy not possible
                    return self.current_data
            except Exception:
                return self.current_data
                
    def get_status(self) -> DataStatus:
        """Get current data status."""
        return self.status
        
    def has_data(self) -> bool:
        """Check if data is currently loaded."""
        with self.data_lock:
            return self.current_data is not None
            
    def clear_data(self):
        """Clear current data and metadata."""
        with self.data_lock:
            self.current_data = None
            self.data_metadata = DataMetadata()
            
        self._set_status(DataStatus.IDLE)
        self._notify_data_change()
        self._add_to_history("clear_data", {})
        
    def get_data_summary(self) -> Dict[str, Any]:
        """Get summary information about current data."""
        with self.data_lock:
            if not self.current_data:
                return {"status": "no_data"}
                
            try:
                summary = {
                    "status": "has_data",
                    "metadata": self.data_metadata.to_dict()
                }
                
                # Add data-specific information
                if hasattr(self.current_data, 'shape'):
                    # DataFrame-like object
                    summary.update({
                        'type': 'dataframe',
                        'shape': self.current_data.shape,
                        'dtypes': str(getattr(self.current_data, 'dtypes', 'unknown'))
                    })
                elif isinstance(self.current_data, list):
                    # List of records
                    summary.update({
                        'type': 'list',
                        'length': len(self.current_data),
                        'sample_keys': list(self.current_data[0].keys()) if self.current_data and isinstance(self.current_data[0], dict) else []
                    })
                else:
                    summary.update({
                        'type': type(self.current_data).__name__,
                        'string_repr': str(type(self.current_data))
                    })
                    
                return summary
                
            except Exception as e:
                return {
                    "status": "error",
                    "error": str(e),
                    "type": type(self.current_data).__name__
                }
                
    def get_operation_history(self) -> List[Dict[str, Any]]:
        """Get history of operations performed."""
        return self.operation_history.copy()
        
    def clear_operation_history(self):
        """Clear operation history."""
        self.operation_history.clear()
        
    # Private helper methods
    def _set_status(self, status: DataStatus):
        """Set status and notify callbacks."""
        if self.status != status:
            self.status = status
            self._notify_status_change()
            
    def _notify_loading_start(self):
        """Notify callbacks that loading has started."""
        for callback in self.loading_callbacks:
            try:
                callback(True)
            except Exception as e:
                print(f"Error in loading callback: {e}")
                
    def _notify_loading_end(self):
        """Notify callbacks that loading has ended."""
        for callback in self.loading_callbacks:
            try:
                callback(False)
            except Exception as e:
                print(f"Error in loading callback: {e}")
                
    def _notify_error(self, error_message: str):
        """Notify callbacks about errors."""
        for callback in self.error_callbacks:
            try:
                callback(error_message)
            except Exception as e:
                print(f"Error in error callback: {e}")
                
    def _notify_data_change(self):
        """Notify callbacks about data changes."""
        for callback in self.data_change_callbacks:
            try:
                callback(self.current_data, self.data_metadata)
            except Exception as e:
                print(f"Error in data change callback: {e}")
                
    def _notify_status_change(self):
        """Notify callbacks about status changes."""
        for callback in self.status_callbacks:
            try:
                callback(self.status)
            except Exception as e:
                print(f"Error in status callback: {e}")
                
    def _add_to_history(self, operation: str, params: Dict[str, Any]):
        """Add operation to history."""
        entry = {
            'operation': operation,
            'params': params,
            'timestamp': datetime.now(),
            'status': self.status.value
        }
        
        self.operation_history.append(entry)
        
        # Limit history size
        if len(self.operation_history) > self.max_history:
            self.operation_history.pop(0)
            
    def _detect_file_type(self, file_path: str) -> str:
        """Detect file type from extension."""
        import os
        _, ext = os.path.splitext(file_path.lower())
        
        type_map = {
            '.csv': 'csv',
            '.json': 'json',
            '.parquet': 'parquet',
            '.feather': 'feather',
            '.xlsx': 'excel',
            '.xls': 'excel',
            '.txt': 'text'
        }
        
        return type_map.get(ext, 'unknown')
        
    def _load_file_by_type(self, file_path: str, file_type: str, **kwargs) -> Optional[Any]:
        """Load file based on its type."""
        try:
            if file_type == 'csv':
                return self._load_csv(file_path, **kwargs)
            elif file_type == 'json':
                return self._load_json(file_path, **kwargs)
            elif file_type == 'parquet':
                return self._load_parquet(file_path, **kwargs)
            elif file_type == 'feather':
                return self._load_feather(file_path, **kwargs)
            elif file_type == 'excel':
                return self._load_excel(file_path, **kwargs)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
        except Exception as e:
            raise Exception(f"Error loading {file_type} file: {str(e)}")
            
    def _load_csv(self, file_path: str, **kwargs) -> List[Dict[str, Any]]:
        """Load CSV file."""
        import csv
        
        data = []
        encoding = kwargs.get('encoding', 'utf-8')
        delimiter = kwargs.get('delimiter', ',')
        
        with open(file_path, 'r', encoding=encoding) as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            for row in reader:
                data.append(dict(row))
                
        return data
        
    def _load_json(self, file_path: str, **kwargs) -> Any:
        """Load JSON file."""
        encoding = kwargs.get('encoding', 'utf-8')
        
        with open(file_path, 'r', encoding=encoding) as f:
            return json.load(f)
            
    def _load_parquet(self, file_path: str, **kwargs) -> Any:
        """Load Parquet file."""
        try:
            import polars as pl
            return pl.read_parquet(file_path)
        except ImportError:
            try:
                import pandas as pd
                return pd.read_parquet(file_path)
            except ImportError:
                raise ImportError("Neither polars nor pandas available for parquet loading")
                
    def _load_feather(self, file_path: str, **kwargs) -> Any:
        """Load Feather file."""
        try:
            import polars as pl
            return pl.read_ipc(file_path)
        except ImportError:
            try:
                import pandas as pd
                return pd.read_feather(file_path)
            except ImportError:
                raise ImportError("Neither polars nor pandas available for feather loading")
                
    def _load_excel(self, file_path: str, **kwargs) -> Any:
        """Load Excel file."""
        try:
            import pandas as pd
            return pd.read_excel(file_path, **kwargs)
        except ImportError:
            raise ImportError("pandas not available for Excel loading")
            
    def _generate_metadata_for_data(self, data: Any) -> DataMetadata:
        """Generate metadata for given data."""
        metadata = DataMetadata(
            source=DataSource.UNKNOWN,
            source_details="Direct data assignment",
            loaded_at=datetime.now()
        )
        
        try:
            if hasattr(data, 'shape'):
                # DataFrame-like
                metadata.record_count = data.shape[0]
                if hasattr(data, 'columns'):
                    metadata.columns = list(data.columns)
                    metadata.column_count = len(metadata.columns)
                metadata.data_type = type(data).__name__
            elif isinstance(data, list):
                metadata.record_count = len(data)
                if data and isinstance(data[0], dict):
                    metadata.columns = list(data[0].keys())
                    metadata.column_count = len(metadata.columns)
                metadata.data_type = "list"
            elif isinstance(data, dict):
                metadata.record_count = 1
                metadata.columns = list(data.keys())
                metadata.column_count = len(metadata.columns)
                metadata.data_type = "dict"
            else:
                metadata.data_type = type(data).__name__
                
        except Exception:
            pass
            
        return metadata
        
    def __str__(self) -> str:
        """String representation of the data controller."""
        return f"DataController(status={self.status.value}, has_data={self.has_data()})"
        
    def __repr__(self) -> str:
        """Detailed string representation."""
        return (f"DataController(status={self.status.value}, "
                f"has_data={self.has_data()}, "
                f"record_count={self.data_metadata.record_count})")
