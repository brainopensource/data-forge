"""
Plugin Interface Definitions for DataForge Frontend

This module defines standardized interfaces that plugins must implement
to integrate with the DataForge plugin system.

Interfaces:
- IDataSourcePlugin: For custom data source connections
- IVisualizationPlugin: For custom visualization components
- IWorkflowPlugin: For workflow automation
- IUIComponentPlugin: For custom UI components
- IExportPlugin: For custom export formats
- IValidationPlugin: For data validation extensions
- ITransformationPlugin: For data transformation operations
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union, Tuple, Callable
from dataclasses import dataclass
from enum import Enum
import tkinter as tk


# Common data structures for plugin interfaces

@dataclass
class ConnectionConfig:
    """Configuration definition for data source connections."""
    fields: List[Dict[str, Any]]
    validation_rules: List[Dict[str, Any]]
    connection_test_query: Optional[str] = None
    supports_streaming: bool = False
    max_batch_size: int = 1000


@dataclass
class WorkflowStep:
    """Definition of a workflow step."""
    name: str
    description: str
    parameters: Dict[str, Any]
    input_type: str
    output_type: str
    is_async: bool = False
    timeout: int = 300  # seconds


@dataclass
class UIComponentConfig:
    """Configuration for UI component creation."""
    component_type: str
    properties: Dict[str, Any]
    event_handlers: Dict[str, str]
    layout_hints: Dict[str, Any]


@dataclass
class ValidationResult:
    """Result of data validation operation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    suggestions: List[str]
    metadata: Dict[str, Any]


@dataclass
class TransformationResult:
    """Result of data transformation operation."""
    transformed_data: Any
    success: bool
    errors: List[str]
    metadata: Dict[str, Any]


class DataSourceType(Enum):
    """Types of data sources."""
    DATABASE = "database"
    FILE = "file"
    API = "api"
    STREAM = "stream"
    CLOUD = "cloud"


class ExportFormat(Enum):
    """Supported export formats."""
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    EXCEL = "excel"
    PDF = "pdf"
    XML = "xml"


# Plugin Interface Definitions

class IPlugin(ABC):
    """Base interface for all plugins."""
    
    @abstractmethod
    def get_name(self) -> str:
        """Return plugin name."""
        pass
    
    @abstractmethod
    def get_version(self) -> str:
        """Return plugin version."""
        pass
    
    @abstractmethod
    def get_description(self) -> str:
        """Return plugin description."""
        pass
    
    def initialize(self) -> None:
        """Initialize plugin (optional)."""
        pass
    
    def cleanup(self) -> None:
        """Cleanup plugin resources (optional)."""
        pass
    
    def on_configuration_updated(self, config: Dict[str, Any]) -> None:
        """Handle configuration updates (optional)."""
        pass


class IDataSourcePlugin(IPlugin):
    """Interface for data source plugins."""
    
    @abstractmethod
    def get_data_source_type(self) -> DataSourceType:
        """Return the type of data source."""
        pass
    
    @abstractmethod
    def get_connection_config(self) -> ConnectionConfig:
        """Return connection configuration UI definition."""
        pass
    
    @abstractmethod
    def test_connection(self, config: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Test connection with given configuration.
        
        Returns:
            Tuple[bool, str]: (success, message)
        """
        pass
    
    @abstractmethod
    def fetch_data(self, config: Dict[str, Any], query: Optional[str] = None, 
                   limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch data from the source."""
        pass
    
    @abstractmethod
    def get_schema(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Get data schema from the source."""
        pass
    
    def supports_streaming(self) -> bool:
        """Return whether the data source supports streaming."""
        return False
    
    def get_data_preview(self, config: Dict[str, Any], limit: int = 10) -> List[Dict[str, Any]]:
        """Get preview of data (default implementation)."""
        return self.fetch_data(config, limit=limit)
    
    def get_available_tables(self, config: Dict[str, Any]) -> List[str]:
        """Get list of available tables/collections (optional)."""
        return []
    
    def validate_query(self, config: Dict[str, Any], query: str) -> Tuple[bool, str]:
        """Validate query syntax (optional)."""
        return True, "Query validation not implemented"


class IVisualizationPlugin(IPlugin):
    """Interface for visualization plugins."""
    
    @abstractmethod
    def get_supported_plot_types(self) -> List[str]:
        """Return list of supported plot types."""
        pass
    
    @abstractmethod
    def create_plot(self, data: List[Dict], plot_type: str, config: Dict[str, Any]) -> Any:
        """Create visualization with given data and configuration."""
        pass
    
    @abstractmethod
    def get_plot_config_ui(self, plot_type: str) -> Dict[str, Any]:
        """Return configuration UI definition for plot type."""
        pass
    
    def validate_data_for_plot(self, data: List[Dict], plot_type: str) -> Tuple[bool, str]:
        """Validate if data is suitable for plot type."""
        return True, "Data validation not implemented"
    
    def export_plot(self, plot_object: Any, format: str, path: str) -> bool:
        """Export plot to file (optional implementation)."""
        return False
    
    def get_plot_requirements(self, plot_type: str) -> Dict[str, Any]:
        """Get data requirements for plot type (optional)."""
        return {}
    
    def update_plot(self, plot_object: Any, new_data: List[Dict], config: Dict[str, Any]) -> bool:
        """Update existing plot with new data (optional)."""
        return False


class IWorkflowPlugin(IPlugin):
    """Interface for workflow automation plugins."""
    
    @abstractmethod
    def get_workflow_steps(self) -> List[WorkflowStep]:
        """Return available workflow steps."""
        pass
    
    @abstractmethod
    def execute_step(self, step_name: str, input_data: Any, parameters: Dict[str, Any]) -> Any:
        """Execute a workflow step."""
        pass
    
    @abstractmethod
    def validate_workflow(self, steps: List[Dict[str, Any]]) -> List[str]:
        """Validate workflow configuration, return list of errors."""
        pass
    
    def get_step_dependencies(self, step_name: str) -> List[str]:
        """Get dependencies for a specific step (optional)."""
        return []
    
    def estimate_execution_time(self, step_name: str, parameters: Dict[str, Any]) -> int:
        """Estimate execution time in seconds (optional)."""
        return 0
    
    def supports_parallel_execution(self, step_name: str) -> bool:
        """Return whether step supports parallel execution (optional)."""
        return False
    
    def get_step_configuration_schema(self, step_name: str) -> Dict[str, Any]:
        """Get configuration schema for step (optional)."""
        return {}


class IUIComponentPlugin(IPlugin):
    """Interface for custom UI component plugins."""
    
    @abstractmethod
    def get_component_types(self) -> List[str]:
        """Return available component types."""
        pass
    
    @abstractmethod
    def create_component(self, parent: Any, component_type: str, config: Dict[str, Any]) -> Any:
        """Create UI component."""
        pass
    
    @abstractmethod
    def get_component_config_ui(self, component_type: str) -> Dict[str, Any]:
        """Return configuration UI for component type."""
        pass
    
    def destroy_component(self, component: Any) -> None:
        """Destroy component and clean up resources (optional)."""
        pass
    
    def update_component(self, component: Any, config: Dict[str, Any]) -> bool:
        """Update component configuration (optional)."""
        return False
    
    def get_component_events(self, component_type: str) -> List[str]:
        """Get available events for component type (optional)."""
        return []
    
    def bind_event_handler(self, component: Any, event: str, handler: Callable) -> bool:
        """Bind event handler to component (optional)."""
        return False


class IExportPlugin(IPlugin):
    """Interface for export plugins."""
    
    @abstractmethod
    def get_supported_formats(self) -> List[str]:
        """Return supported export formats."""
        pass
    
    @abstractmethod
    def export_data(self, data: List[Dict], format: str, config: Dict[str, Any]) -> bytes:
        """Export data to specified format."""
        pass
    
    @abstractmethod
    def get_export_config_ui(self, format: str) -> Dict[str, Any]:
        """Return configuration UI for export format."""
        pass
    
    def validate_data_for_export(self, data: List[Dict], format: str) -> Tuple[bool, str]:
        """Validate if data can be exported in format."""
        return True, "Validation not implemented"
    
    def get_file_extension(self, format: str) -> str:
        """Get file extension for format (optional)."""
        return ""
    
    def supports_streaming_export(self, format: str) -> bool:
        """Return whether format supports streaming export (optional)."""
        return False
    
    def estimate_file_size(self, data: List[Dict], format: str) -> int:
        """Estimate exported file size in bytes (optional)."""
        return 0


class IValidationPlugin(IPlugin):
    """Interface for data validation plugins."""
    
    @abstractmethod
    def get_validation_types(self) -> List[str]:
        """Return available validation types."""
        pass
    
    @abstractmethod
    def validate_data(self, data: List[Dict], validation_type: str, 
                     config: Dict[str, Any]) -> ValidationResult:
        """Validate data with specified validation type."""
        pass
    
    @abstractmethod
    def get_validation_config_ui(self, validation_type: str) -> Dict[str, Any]:
        """Return configuration UI for validation type."""
        pass
    
    def get_validation_schema(self, validation_type: str) -> Dict[str, Any]:
        """Get validation configuration schema (optional)."""
        return {}
    
    def auto_suggest_validation(self, data: List[Dict]) -> List[str]:
        """Auto-suggest validation types for data (optional)."""
        return []
    
    def fix_validation_errors(self, data: List[Dict], validation_result: ValidationResult) -> List[Dict]:
        """Attempt to fix validation errors (optional)."""
        return data


class ITransformationPlugin(IPlugin):
    """Interface for data transformation plugins."""
    
    @abstractmethod
    def get_transformation_types(self) -> List[str]:
        """Return available transformation types."""
        pass
    
    @abstractmethod
    def transform_data(self, data: List[Dict], transformation_type: str, 
                      config: Dict[str, Any]) -> TransformationResult:
        """Transform data with specified transformation type."""
        pass
    
    @abstractmethod
    def get_transformation_config_ui(self, transformation_type: str) -> Dict[str, Any]:
        """Return configuration UI for transformation type."""
        pass
    
    def validate_transformation_config(self, transformation_type: str, 
                                     config: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate transformation configuration (optional)."""
        return True, "Configuration validation not implemented"
    
    def preview_transformation(self, data: List[Dict], transformation_type: str, 
                             config: Dict[str, Any], limit: int = 10) -> List[Dict]:
        """Preview transformation result (optional)."""
        result = self.transform_data(data[:limit], transformation_type, config)
        return result.transformed_data if result.success else data[:limit]
    
    def get_transformation_schema(self, transformation_type: str) -> Dict[str, Any]:
        """Get transformation configuration schema (optional)."""
        return {}
    
    def supports_batch_processing(self, transformation_type: str) -> bool:
        """Return whether transformation supports batch processing (optional)."""
        return False


# Helper classes for plugin development

class PluginConfigBuilder:
    """Helper class for building plugin configuration UIs."""
    
    @staticmethod
    def text_field(label: str, required: bool = False, default: str = "", 
                   placeholder: str = "") -> Dict[str, Any]:
        """Create text field configuration."""
        return {
            "type": "text",
            "label": label,
            "required": required,
            "default": default,
            "placeholder": placeholder
        }
    
    @staticmethod
    def number_field(label: str, required: bool = False, default: Union[int, float] = 0,
                    min_value: Optional[Union[int, float]] = None,
                    max_value: Optional[Union[int, float]] = None) -> Dict[str, Any]:
        """Create number field configuration."""
        config = {
            "type": "number",
            "label": label,
            "required": required,
            "default": default
        }
        if min_value is not None:
            config["min"] = min_value
        if max_value is not None:
            config["max"] = max_value
        return config
    
    @staticmethod
    def select_field(label: str, options: List[str], required: bool = False, 
                    default: str = "") -> Dict[str, Any]:
        """Create select field configuration."""
        return {
            "type": "select",
            "label": label,
            "options": options,
            "required": required,
            "default": default
        }
    
    @staticmethod
    def checkbox_field(label: str, default: bool = False) -> Dict[str, Any]:
        """Create checkbox field configuration."""
        return {
            "type": "checkbox",
            "label": label,
            "default": default
        }
    
    @staticmethod
    def file_field(label: str, required: bool = False, 
                  file_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """Create file field configuration."""
        config = {
            "type": "file",
            "label": label,
            "required": required
        }
        if file_types:
            config["file_types"] = file_types
        return config


class PluginValidator:
    """Helper class for validating plugin implementations."""
    
    @staticmethod
    def validate_interface_implementation(plugin_class: type, interface_class: type) -> List[str]:
        """Validate that plugin class implements interface correctly."""
        errors = []
        
        # Check if plugin inherits from interface
        if not issubclass(plugin_class, interface_class):
            errors.append(f"Plugin does not inherit from {interface_class.__name__}")
            return errors
        
        # Check abstract methods are implemented
        abstract_methods = [
            method for method in interface_class.__abstractmethods__
        ]
        
        for method_name in abstract_methods:
            if not hasattr(plugin_class, method_name):
                errors.append(f"Plugin does not implement required method: {method_name}")
            else:
                method = getattr(plugin_class, method_name)
                if getattr(method, '__isabstractmethod__', False):
                    errors.append(f"Method {method_name} is still abstract in plugin")
        
        return errors
    
    @staticmethod
    def validate_plugin_metadata(manifest_data: Dict[str, Any]) -> List[str]:
        """Validate plugin manifest metadata."""
        errors = []
        required_fields = ["name", "version", "description", "author", "type", "entry_point"]
        
        for field in required_fields:
            if field not in manifest_data:
                errors.append(f"Missing required field: {field}")
            elif not manifest_data[field]:
                errors.append(f"Field {field} cannot be empty")
        
        # Validate plugin type
        if "type" in manifest_data:
            try:
                from frontend.core.plugin_system import PluginType
                PluginType(manifest_data["type"])
            except ValueError:
                from frontend.core.plugin_system import PluginType
                valid_types = [t.value for t in PluginType]
                errors.append(f"Invalid plugin type. Must be one of: {valid_types}")
        
        return errors
