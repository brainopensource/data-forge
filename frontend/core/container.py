"""Dependency injection container for the frontend application."""

from typing import Type, TypeVar, Dict, Callable, Any, Optional, cast, List
import inspect
import threading
from abc import ABC

T = TypeVar('T')


class ServiceLifetime:
    """Service lifetime constants."""
    SINGLETON = "singleton"
    TRANSIENT = "transient"
    SCOPED = "scoped"


class ServiceDescriptor:
    """Describes a service registration."""
    
    def __init__(self, service_type: Type[Any], implementation: Type[Any], 
                 lifetime: str = ServiceLifetime.TRANSIENT, factory: Optional[Callable[[], Any]] = None):
        self.service_type = service_type
        self.implementation = implementation
        self.lifetime = lifetime
        self.factory = factory
        self.instance: Optional[Any] = None


class DIContainer:
    """Simple dependency injection container with lifecycle management."""
    
    def __init__(self):
        self._services: Dict[Type, ServiceDescriptor] = {}
        self._lock = threading.Lock()
        self._initialized = False
        
    def register_singleton(self, interface: Type[T], implementation: Type[T]) -> 'DIContainer':
        """
        Register singleton service.
        
        Args:
            interface: Service interface type
            implementation: Implementation type
            
        Returns:
            DIContainer: Self for method chaining
        """
        with self._lock:
            descriptor = ServiceDescriptor(interface, implementation, ServiceLifetime.SINGLETON)
            self._services[interface] = descriptor
        return self
        
    def register_transient(self, interface: Type[T], implementation: Type[T]) -> 'DIContainer':
        """
        Register transient service.
        
        Args:
            interface: Service interface type
            implementation: Implementation type
            
        Returns:
            DIContainer: Self for method chaining
        """
        with self._lock:
            descriptor = ServiceDescriptor(interface, implementation, ServiceLifetime.TRANSIENT)
            self._services[interface] = descriptor
        return self
        
    def register_factory(self, interface: Type[T], factory: Callable[[], T]) -> 'DIContainer':
        """
        Register factory function.
        
        Args:
            interface: Service interface type
            factory: Factory function
            
        Returns:
            DIContainer: Self for method chaining
        """
        with self._lock:
            descriptor = ServiceDescriptor(interface, interface, ServiceLifetime.TRANSIENT, factory)
            self._services[interface] = descriptor
        return self
        
    def register_instance(self, interface: Type[T], instance: T) -> 'DIContainer':
        """
        Register instance as singleton.
        
        Args:
            interface: Service interface type
            instance: Instance to register
            
        Returns:
            DIContainer: Self for method chaining
        """
        with self._lock:
            descriptor = ServiceDescriptor(interface, type(instance), ServiceLifetime.SINGLETON)
            descriptor.instance = instance
            self._services[interface] = descriptor
        return self
        
    def resolve(self, service_type: Type[T]) -> T:
        """
        Resolve service instance.
        
        Args:
            service_type: Service type to resolve
            
        Returns:
            T: Service instance
            
        Raises:
            ValueError: If service not registered or cannot be resolved
        """
        with self._lock:
            if service_type not in self._services:
                raise ValueError(f"Service {service_type.__name__} not registered")
                
            descriptor = self._services[service_type]
            
            # Check for factory
            if descriptor.factory:
                return descriptor.factory()
                
            # Check if it's a singleton and already instantiated
            if descriptor.lifetime == ServiceLifetime.SINGLETON and descriptor.instance is not None:
                return cast(T, descriptor.instance)
            
            # Create new instance
            instance = self._create_instance(descriptor.implementation)
            
            # Store singleton instance
            if descriptor.lifetime == ServiceLifetime.SINGLETON:
                descriptor.instance = instance
                
            return instance
        
    def try_resolve(self, service_type: Type[T]) -> Optional[T]:
        """
        Try to resolve service instance without raising exception.
        
        Args:
            service_type: Service type to resolve
            
        Returns:
            Optional[T]: Service instance or None if not found
        """
        try:
            return self.resolve(service_type)
        except (ValueError, TypeError):
            return None
            
    def is_registered(self, service_type: Type[T]) -> bool:
        """
        Check if service type is registered.
        
        Args:
            service_type: Service type to check
            
        Returns:
            bool: True if registered, False otherwise
        """
        return service_type in self._services
        
    def get_registered_services(self) -> List[Type]:
        """
        Get list of all registered service types.
        
        Returns:
            List[Type]: List of registered service types
        """
        return list(self._services.keys())
        
    def _create_instance(self, implementation: Type[T]) -> T:
        """
        Create instance with dependency injection.
        
        Args:
            implementation: Implementation type to instantiate
            
        Returns:
            T: New instance with dependencies injected
        """
        try:
            # Get constructor parameters
            signature = inspect.signature(implementation.__init__)
            params = {}
            
            for param_name, param in signature.parameters.items():
                if param_name == 'self':
                    continue
                    
                # Skip parameters without type annotations
                if param.annotation == inspect.Parameter.empty:
                    continue
                    
                # Try to resolve dependency
                try:
                    dependency = self.resolve(param.annotation)
                    params[param_name] = dependency
                except ValueError:
                    # Check if parameter has default value
                    if param.default != inspect.Parameter.empty:
                        continue
                    # Skip if dependency not found and no default
                    pass
                    
            return implementation(**params)
        except Exception as e:
            raise ValueError(f"Failed to create instance of {implementation.__name__}: {str(e)}")
    
    def clear(self) -> None:
        """Clear all service registrations."""
        with self._lock:
            self._services.clear()
            self._initialized = False


# Global container instance
container = DIContainer()


def configure_services() -> DIContainer:
    """
    Configure default services.
    
    Returns:
        DIContainer: Configured container
    """
    # Import here to avoid circular imports
    from frontend.utils.error_handler import ErrorHandler
    from frontend.utils.string_utils import StringUtils
    from frontend.utils.data_type_detector import DataTypeDetector
    from frontend.services.ui_framework_adapter import UIFrameworkAdapter
    from frontend.domain.services.data_validation_service import DataValidationService
    from frontend.core.plugin_system import PluginManager
    from frontend.application.handlers.validation_handlers import (
        ValidateDatasetCommandHandler, CleanDatasetCommandHandler, 
        GenerateQualityReportCommandHandler
    )
    from frontend.application.handlers.validation_query_handlers import (
        GetDataQualityMetricsQueryHandler, GetDataQualityRecommendationsQueryHandler
    )
    
    # Register utility services as singletons
    container.register_singleton(ErrorHandler, ErrorHandler)
    container.register_singleton(StringUtils, StringUtils)
    container.register_singleton(DataTypeDetector, DataTypeDetector)
    container.register_singleton(DataValidationService, DataValidationService)
    container.register_instance(UIFrameworkAdapter, UIFrameworkAdapter())
    
    # Register plugin system as singleton
    container.register_singleton(PluginManager, PluginManager)
    
    # Register command handlers as transients (they depend on the services above)
    container.register_transient(ValidateDatasetCommandHandler, ValidateDatasetCommandHandler)
    container.register_transient(CleanDatasetCommandHandler, CleanDatasetCommandHandler)
    container.register_transient(GenerateQualityReportCommandHandler, GenerateQualityReportCommandHandler)
    
    # Register query handlers as transients
    container.register_transient(GetDataQualityMetricsQueryHandler, GetDataQualityMetricsQueryHandler)
    container.register_transient(GetDataQualityRecommendationsQueryHandler, GetDataQualityRecommendationsQueryHandler)
    
    return container


def get_service(service_type: Type[T]) -> T:
    """
    Get service from global container.
    
    Args:
        service_type: Service type to resolve
        
    Returns:
        T: Service instance
    """
    return container.resolve(service_type)


def get_service_optional(service_type: Type[T]) -> Optional[T]:
    """
    Get service from global container without raising exception.
    
    Args:
        service_type: Service type to resolve
        
    Returns:
        Optional[T]: Service instance or None
    """
    return container.try_resolve(service_type)
