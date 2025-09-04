"""
Core Plugin System for DataForge Frontend

This module implements a comprehensive plugin architecture that allows third-party
extensions, custom data sources, visualization plugins, and workflow automation
while maintaining security and performance.

Features:
- Hot-loading capabilities
- Plugin discovery and management
- Security validation and permission system
- Standardized plugin interfaces
- Dependency management
"""

from typing import Dict, List, Any, Optional, Type, Callable, Union
from pathlib import Path
from dataclasses import dataclass
from enum import Enum
import json
import importlib.util
import sys
import threading
import hashlib
import inspect
from abc import ABC, abstractmethod
import logging

from frontend.utils.error_handler import ErrorHandler


class PluginType(Enum):
    """Types of plugins supported by the system."""
    DATA_SOURCE = "data_source"
    VISUALIZATION = "visualization"
    WORKFLOW = "workflow"
    UI_COMPONENT = "ui_component"
    EXPORT = "export"
    VALIDATION = "validation"
    TRANSFORMATION = "transformation"


class PluginStatus(Enum):
    """Plugin lifecycle status."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"
    LOADING = "loading"
    UNLOADING = "unloading"
    DISABLED = "disabled"


@dataclass
class PluginInfo:
    """Plugin metadata and configuration."""
    name: str
    version: str
    description: str
    author: str
    plugin_type: PluginType
    entry_point: str
    dependencies: List[str]
    min_app_version: str
    configuration_schema: Dict[str, Any]
    permissions: List[str]
    tags: Optional[List[str]] = None
    homepage: str = ""
    documentation: str = ""
    license: str = ""

    def __post_init__(self):
        if self.tags is None:
            self.tags = []


@dataclass
class PluginManifest:
    """Complete plugin manifest including file system information."""
    info: PluginInfo
    path: Path
    manifest_data: Dict[str, Any]
    checksum: str = ""
    last_modified: float = 0


class PluginSecurity:
    """Plugin security and permission validation."""
    
    ALLOWED_PERMISSIONS = {
        "file_access", "network_access", "database_access",
        "ui_modification", "data_export", "data_import",
        "system_info", "configuration_read", "configuration_write"
    }
    
    @classmethod
    def validate_permissions(cls, permissions: List[str]) -> List[str]:
        """Validate plugin permissions against allowed list."""
        invalid = [p for p in permissions if p not in cls.ALLOWED_PERMISSIONS]
        return invalid
    
    @classmethod
    def generate_checksum(cls, file_path: Path) -> str:
        """Generate SHA256 checksum for plugin file."""
        if not file_path.exists():
            return ""
        
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()


class PluginLoader:
    """Plugin module loading and lifecycle management."""
    
    def __init__(self, error_handler: ErrorHandler):
        self.error_handler = error_handler
        self._loaded_modules: Dict[str, Any] = {}
        
    def load_module(self, manifest: PluginManifest) -> Optional[Any]:
        """Load plugin module from manifest."""
        try:
            plugin_dir = manifest.path.parent
            entry_point_file = plugin_dir / f"{manifest.info.entry_point.split('.')[0]}.py"
            
            if not entry_point_file.exists():
                raise FileNotFoundError(f"Plugin entry point not found: {entry_point_file}")
            
            # Generate unique module name
            module_name = f"plugin_{manifest.info.name.replace(' ', '_').lower()}_{manifest.info.version.replace('.', '_')}"
            
            # Load module
            spec = importlib.util.spec_from_file_location(module_name, entry_point_file)
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not load module spec for {entry_point_file}")
                
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
            
            # Store loaded module
            self._loaded_modules[manifest.info.name] = module
            
            return module
            
        except Exception as e:
            self.error_handler.handle_error(e, f"Failed to load plugin module: {manifest.info.name}")
            return None
    
    def unload_module(self, plugin_name: str) -> bool:
        """Unload plugin module."""
        try:
            if plugin_name in self._loaded_modules:
                module = self._loaded_modules[plugin_name]
                module_name = module.__name__
                
                # Remove from sys.modules
                if module_name in sys.modules:
                    del sys.modules[module_name]
                
                # Remove from our tracking
                del self._loaded_modules[plugin_name]
                
            return True
            
        except Exception as e:
            self.error_handler.handle_error(e, f"Failed to unload plugin module: {plugin_name}")
            return False
    
    def get_plugin_class(self, module: Any, entry_point: str) -> Optional[Type]:
        """Get plugin class from loaded module."""
        try:
            class_name = entry_point.split('.')[-1]
            return getattr(module, class_name)
        except AttributeError as e:
            self.error_handler.handle_error(e, f"Plugin class not found: {entry_point}")
            return None


class PluginRegistry:
    """Central registry for plugin discovery and manifest management."""
    
    def __init__(self, error_handler: ErrorHandler):
        self.error_handler = error_handler
        self.manifests: Dict[str, PluginManifest] = {}
        self.plugin_directories: List[Path] = []
        
    def add_plugin_directory(self, directory: Path) -> None:
        """Add directory for plugin discovery."""
        if directory.exists() and directory.is_dir():
            self.plugin_directories.append(directory)
            
    def discover_plugins(self) -> List[PluginManifest]:
        """Discover all plugins in registered directories."""
        discovered = []
        
        for plugin_dir in self.plugin_directories:
            if not plugin_dir.exists():
                continue
                
            for item in plugin_dir.iterdir():
                if item.is_dir():
                    manifest = self._load_plugin_manifest(item)
                    if manifest:
                        discovered.append(manifest)
                        self.manifests[manifest.info.name] = manifest
        
        return discovered
    
    def _load_plugin_manifest(self, plugin_path: Path) -> Optional[PluginManifest]:
        """Load and validate plugin manifest."""
        manifest_file = plugin_path / "plugin.json"
        
        if not manifest_file.exists():
            return None
            
        try:
            with open(manifest_file, 'r', encoding='utf-8') as f:
                manifest_data = json.load(f)
            
            # Validate required fields
            required_fields = ["name", "version", "description", "author", "type", "entry_point"]
            for field in required_fields:
                if field not in manifest_data:
                    raise ValueError(f"Missing required field: {field}")
            
            # Create plugin info
            plugin_info = PluginInfo(
                name=manifest_data["name"],
                version=manifest_data["version"],
                description=manifest_data["description"],
                author=manifest_data["author"],
                plugin_type=PluginType(manifest_data["type"]),
                entry_point=manifest_data["entry_point"],
                dependencies=manifest_data.get("dependencies", []),
                min_app_version=manifest_data.get("min_app_version", "1.0.0"),
                configuration_schema=manifest_data.get("configuration_schema", {}),
                permissions=manifest_data.get("permissions", []),
                tags=manifest_data.get("tags", []),
                homepage=manifest_data.get("homepage", ""),
                documentation=manifest_data.get("documentation", ""),
                license=manifest_data.get("license", "")
            )
            
            # Validate permissions
            invalid_permissions = PluginSecurity.validate_permissions(plugin_info.permissions)
            if invalid_permissions:
                raise ValueError(f"Invalid permissions: {invalid_permissions}")
            
            # Create manifest
            manifest = PluginManifest(
                info=plugin_info,
                path=plugin_path,
                manifest_data=manifest_data,
                checksum=PluginSecurity.generate_checksum(manifest_file),
                last_modified=manifest_file.stat().st_mtime
            )
            
            return manifest
            
        except Exception as e:
            self.error_handler.handle_error(e, f"Failed to load plugin manifest: {manifest_file}")
            return None


class PluginManager:
    """Central plugin management system implementing the core plugin architecture."""
    
    def __init__(self, error_handler: ErrorHandler):
        self.error_handler = error_handler
        self.registry = PluginRegistry(error_handler)
        self.loader = PluginLoader(error_handler)
        
        self.loaded_plugins: Dict[str, 'Plugin'] = {}
        self.plugin_interfaces: Dict[PluginType, Type] = {}
        self.plugin_instances: Dict[str, Any] = {}
        self.plugin_status: Dict[str, PluginStatus] = {}
        
        self._lock = threading.Lock()
        self._event_handlers: Dict[str, List[Callable]] = {}
        
        self._initialize_plugin_system()
    
    def _initialize_plugin_system(self) -> None:
        """Initialize the plugin system with core interfaces."""
        self._register_core_interfaces()
        self._setup_default_directories()
        
    def _register_core_interfaces(self) -> None:
        """Register core plugin interface types."""
        try:
            from frontend.core.interfaces.plugin_interfaces import (
                IDataSourcePlugin, IVisualizationPlugin, IWorkflowPlugin,
                IUIComponentPlugin, IExportPlugin, IValidationPlugin,
                ITransformationPlugin
            )
            
            self.plugin_interfaces = {
                PluginType.DATA_SOURCE: IDataSourcePlugin,
                PluginType.VISUALIZATION: IVisualizationPlugin,
                PluginType.WORKFLOW: IWorkflowPlugin,
                PluginType.UI_COMPONENT: IUIComponentPlugin,
                PluginType.EXPORT: IExportPlugin,
                PluginType.VALIDATION: IValidationPlugin,
                PluginType.TRANSFORMATION: ITransformationPlugin,
            }
        except ImportError as e:
            self.error_handler.handle_error(e, "Failed to import plugin interfaces")
    
    def _setup_default_directories(self) -> None:
        """Setup default plugin directories."""
        # Application plugins directory
        app_plugins_dir = Path(__file__).parent.parent.parent / "plugins"
        self.registry.add_plugin_directory(app_plugins_dir)
        
        # User plugins directory
        user_plugins_dir = Path.home() / ".dataforge" / "plugins"
        self.registry.add_plugin_directory(user_plugins_dir)
        
        # Create directories if they don't exist
        for plugin_dir in [app_plugins_dir, user_plugins_dir]:
            plugin_dir.mkdir(parents=True, exist_ok=True)
    
    def discover_plugins(self) -> List[PluginManifest]:
        """Discover all available plugins."""
        return self.registry.discover_plugins()
    
    def load_plugin(self, plugin_name: str) -> bool:
        """Load and initialize a plugin."""
        with self._lock:
            if plugin_name in self.loaded_plugins:
                return True  # Already loaded
            
            manifest = self.registry.manifests.get(plugin_name)
            if not manifest:
                self.error_handler.handle_error(
                    ValueError(f"Plugin not found: {plugin_name}"), 
                    "Plugin loading failed"
                )
                return False
            
            try:
                self.plugin_status[plugin_name] = PluginStatus.LOADING
                
                # Check dependencies
                if not self._check_dependencies(manifest.info.dependencies):
                    raise ValueError(f"Plugin dependencies not satisfied: {manifest.info.dependencies}")
                
                # Load module
                module = self.loader.load_module(manifest)
                if not module:
                    raise ValueError("Failed to load plugin module")
                
                # Get plugin class
                plugin_class = self.loader.get_plugin_class(module, manifest.info.entry_point)
                if not plugin_class:
                    raise ValueError("Plugin class not found")
                
                # Validate interface
                expected_interface = self.plugin_interfaces.get(manifest.info.plugin_type)
                if expected_interface and not issubclass(plugin_class, expected_interface):
                    raise ValueError(f"Plugin does not implement required interface: {expected_interface}")
                
                # Create plugin instance
                plugin_instance = plugin_class()
                
                # Create plugin wrapper
                plugin = Plugin(manifest, plugin_instance, self)
                
                # Initialize plugin
                if hasattr(plugin_instance, 'initialize'):
                    plugin_instance.initialize()
                
                # Store plugin
                self.loaded_plugins[plugin_name] = plugin
                self.plugin_instances[plugin_name] = plugin_instance
                self.plugin_status[plugin_name] = PluginStatus.ACTIVE
                
                # Fire event
                self._fire_event('plugin_loaded', plugin_name, plugin)
                
                return True
                
            except Exception as e:
                self.plugin_status[plugin_name] = PluginStatus.ERROR
                self.error_handler.handle_error(e, f"Failed to load plugin: {plugin_name}")
                return False
    
    def unload_plugin(self, plugin_name: str) -> bool:
        """Unload a plugin."""
        with self._lock:
            if plugin_name not in self.loaded_plugins:
                return True  # Not loaded
            
            try:
                self.plugin_status[plugin_name] = PluginStatus.UNLOADING
                
                plugin = self.loaded_plugins[plugin_name]
                plugin_instance = self.plugin_instances[plugin_name]
                
                # Call plugin cleanup if available
                if hasattr(plugin_instance, 'cleanup'):
                    plugin_instance.cleanup()
                
                # Unload module
                self.loader.unload_module(plugin_name)
                
                # Remove from tracking
                del self.loaded_plugins[plugin_name]
                del self.plugin_instances[plugin_name]
                del self.plugin_status[plugin_name]
                
                # Fire event
                self._fire_event('plugin_unloaded', plugin_name)
                
                return True
                
            except Exception as e:
                self.plugin_status[plugin_name] = PluginStatus.ERROR
                self.error_handler.handle_error(e, f"Failed to unload plugin: {plugin_name}")
                return False
    
    def reload_plugin(self, plugin_name: str) -> bool:
        """Reload a plugin (useful for development)."""
        if self.unload_plugin(plugin_name):
            return self.load_plugin(plugin_name)
        return False
    
    def get_plugins_by_type(self, plugin_type: PluginType) -> List['Plugin']:
        """Get all loaded plugins of a specific type."""
        return [
            plugin for plugin in self.loaded_plugins.values()
            if plugin.manifest.info.plugin_type == plugin_type
        ]
    
    def get_plugin(self, plugin_name: str) -> Optional['Plugin']:
        """Get a specific loaded plugin."""
        return self.loaded_plugins.get(plugin_name)
    
    def get_plugin_instance(self, plugin_name: str) -> Optional[Any]:
        """Get plugin instance directly."""
        return self.plugin_instances.get(plugin_name)
    
    def get_plugin_status(self, plugin_name: str) -> PluginStatus:
        """Get plugin status."""
        return self.plugin_status.get(plugin_name, PluginStatus.INACTIVE)
    
    def list_available_plugins(self) -> List[str]:
        """List all available plugin names."""
        return list(self.registry.manifests.keys())
    
    def list_loaded_plugins(self) -> List[str]:
        """List all loaded plugin names."""
        return list(self.loaded_plugins.keys())
    
    def _check_dependencies(self, dependencies: List[str]) -> bool:
        """Check if plugin dependencies are available."""
        for dependency in dependencies:
            try:
                if dependency.startswith("plugin:"):
                    # Plugin dependency
                    plugin_name = dependency[7:]
                    if plugin_name not in self.loaded_plugins:
                        return False
                else:
                    # Python package dependency
                    importlib.import_module(dependency)
            except ImportError:
                return False
        return True
    
    def add_event_handler(self, event_name: str, handler: Callable) -> None:
        """Add event handler for plugin events."""
        if event_name not in self._event_handlers:
            self._event_handlers[event_name] = []
        self._event_handlers[event_name].append(handler)
    
    def remove_event_handler(self, event_name: str, handler: Callable) -> None:
        """Remove event handler."""
        if event_name in self._event_handlers:
            try:
                self._event_handlers[event_name].remove(handler)
            except ValueError:
                pass
    
    def _fire_event(self, event_name: str, *args, **kwargs) -> None:
        """Fire plugin event to all registered handlers."""
        handlers = self._event_handlers.get(event_name, [])
        for handler in handlers:
            try:
                handler(*args, **kwargs)
            except Exception as e:
                self.error_handler.handle_error(e, f"Error in event handler for {event_name}")


class Plugin:
    """Plugin wrapper that provides metadata and lifecycle management."""
    
    def __init__(self, manifest: PluginManifest, instance: Any, manager: PluginManager):
        self.manifest = manifest
        self.instance = instance
        self.manager = manager
        self._configuration: Dict[str, Any] = {}
    
    @property
    def name(self) -> str:
        """Get plugin name."""
        return self.manifest.info.name
    
    @property
    def version(self) -> str:
        """Get plugin version."""
        return self.manifest.info.version
    
    @property
    def plugin_type(self) -> PluginType:
        """Get plugin type."""
        return self.manifest.info.plugin_type
    
    @property
    def permissions(self) -> List[str]:
        """Get plugin permissions."""
        return self.manifest.info.permissions
    
    def get_configuration(self) -> Dict[str, Any]:
        """Get plugin configuration."""
        return self._configuration.copy()
    
    def set_configuration(self, config: Dict[str, Any]) -> None:
        """Set plugin configuration."""
        # TODO: Validate against schema
        self._configuration = config.copy()
        
        # Notify plugin if it has configuration update method
        if hasattr(self.instance, 'on_configuration_updated'):
            try:
                self.instance.on_configuration_updated(config)
            except Exception as e:
                self.manager.error_handler.handle_error(
                    e, f"Error updating configuration for plugin: {self.name}"
                )
    
    def has_permission(self, permission: str) -> bool:
        """Check if plugin has specific permission."""
        return permission in self.permissions
    
    def call_method(self, method_name: str, *args, **kwargs) -> Any:
        """Safely call plugin method."""
        if not hasattr(self.instance, method_name):
            raise AttributeError(f"Plugin {self.name} does not have method: {method_name}")
        
        try:
            method = getattr(self.instance, method_name)
            return method(*args, **kwargs)
        except Exception as e:
            self.manager.error_handler.handle_error(
                e, f"Error calling {method_name} on plugin: {self.name}"
            )
            raise
