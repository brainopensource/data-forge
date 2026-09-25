"""Integration tests for app components."""

import pytest
from unittest.mock import Mock, patch
import asyncio

from frontend.core.container import DIContainer, configure_services
from frontend.services.ui_framework_adapter import UIFrameworkAdapter
from frontend.utils.error_handler import ErrorHandler
from frontend.utils.string_utils import StringUtils
from frontend.utils.data_type_detector import DataTypeDetector
from frontend.domain.entities.data_record import DataRecord
from frontend.domain.services.data_validation_service import DataValidationService


class TestDependencyInjection:
    """Test dependency injection container."""
    
    @pytest.fixture
    def container(self):
        """Create fresh container for each test."""
        return DIContainer()
    
    def test_singleton_registration(self, container):
        """Test singleton service registration."""
        container.register_singleton(ErrorHandler, ErrorHandler)
        
        instance1 = container.resolve(ErrorHandler)
        instance2 = container.resolve(ErrorHandler)
        
        assert instance1 is instance2
        assert isinstance(instance1, ErrorHandler)
    
    def test_transient_registration(self, container):
        """Test transient service registration."""
        container.register_transient(StringUtils, StringUtils)
        
        instance1 = container.resolve(StringUtils)
        instance2 = container.resolve(StringUtils)
        
        assert instance1 is not instance2
        assert isinstance(instance1, StringUtils)
        assert isinstance(instance2, StringUtils)
    
    def test_factory_registration(self, container):
        """Test factory service registration."""
        def create_error_handler():
            return ErrorHandler()
        
        container.register_factory(ErrorHandler, create_error_handler)
        
        instance = container.resolve(ErrorHandler)
        assert isinstance(instance, ErrorHandler)
    
    def test_instance_registration(self, container):
        """Test instance registration."""
        error_handler = ErrorHandler()
        container.register_instance(ErrorHandler, error_handler)
        
        resolved = container.resolve(ErrorHandler)
        assert resolved is error_handler
    
    def test_unregistered_service(self, container):
        """Test resolving unregistered service raises error."""
        with pytest.raises(ValueError, match="Service.*not registered"):
            container.resolve(DataTypeDetector)
    
    def test_is_registered(self, container):
        """Test service registration check."""
        assert not container.is_registered(ErrorHandler)
        
        container.register_singleton(ErrorHandler, ErrorHandler)
        assert container.is_registered(ErrorHandler)
    
    def test_configure_services(self):
        """Test default services configuration."""
        container = configure_services()
        
        # Check that default services are registered
        assert container.is_registered(ErrorHandler)
        assert container.is_registered(StringUtils)
        assert container.is_registered(DataTypeDetector)
        assert container.is_registered(UIFrameworkAdapter)
        
        # Test resolution
        error_handler = container.resolve(ErrorHandler)
        assert isinstance(error_handler, ErrorHandler)


class TestUIFrameworkAdapter:
    """Test UI framework adapter integration."""
    
    def test_framework_detection(self):
        """Test framework detection."""
        adapter = UIFrameworkAdapter()
        framework_name = adapter.get_framework_name()
        assert framework_name in ["CustomTkinter", "tkinter"]
    
    def test_color_scheme(self):
        """Test color scheme availability."""
        adapter = UIFrameworkAdapter()
        colors = adapter.get_color_scheme()
        
        assert 'primary' in colors
        assert 'background' in colors
        assert 'text_primary' in colors
        assert isinstance(colors['primary'], str)
    
    @patch('frontend.services.ui_framework_adapter.HAS_CTK', False)
    def test_tkinter_fallback(self):
        """Test tkinter fallback when CustomTkinter not available."""
        adapter = UIFrameworkAdapter()
        assert not adapter.is_customtkinter_available()
        assert adapter.get_framework_name() == "tkinter"


class TestDomainServices:
    """Test domain services integration."""
    
    @pytest.fixture
    def sample_records(self):
        """Create sample data records."""
        return [
            DataRecord(
                id="1",
                data={"name": "John", "age": 30, "email": "john@example.com"},
                schema_name="users"
            ),
            DataRecord(
                id="2", 
                data={"name": "Jane", "age": 25, "email": "jane@example.com"},
                schema_name="users"
            ),
            DataRecord(
                id="3",
                data={"name": "", "age": None, "email": "invalid-email"},
                schema_name="users"
            )
        ]
    
    def test_data_record_creation(self):
        """Test data record entity creation."""
        record = DataRecord()
        assert record.id is not None
        assert isinstance(record.data, dict)
        assert record.validate()
    
    def test_data_record_operations(self):
        """Test data record operations."""
        record = DataRecord(data={"test": "value"})
        
        # Test value operations
        assert record.get_value("test") == "value"
        assert record.get_value("missing", "default") == "default"
        
        record.set_value("new_field", "new_value")
        assert record.get_value("new_field") == "new_value"
        
        # Test column operations
        assert record.has_column("test")
        assert not record.has_column("missing")
        
        columns = record.get_columns()
        assert "test" in columns
        assert "new_field" in columns
    
    def test_data_validation_service(self, sample_records):
        """Test data validation service."""
        service = DataValidationService()
        
        # Test single record validation
        errors = service.validate_record(sample_records[0])
        assert len(errors) == 0  # Should be valid
        
        errors = service.validate_record(sample_records[2])
        assert len(errors) > 0  # Should have validation errors
        
        # Test multiple records validation
        validation_results = service.validate_records(sample_records)
        assert "3" in validation_results  # Record 3 should have errors
        
        # Test validation summary
        summary = service.get_validation_summary(sample_records)
        assert summary['total_records'] == 3
        assert summary['invalid_records'] > 0
        assert 'validation_rate' in summary


class TestApplicationIntegration:
    """Test full application integration."""
    
    @pytest.fixture
    def configured_container(self):
        """Get configured container."""
        return configure_services()
    
    def test_service_resolution_chain(self, configured_container):
        """Test that services can resolve dependencies."""
        # Test that we can resolve all registered services
        error_handler = configured_container.resolve(ErrorHandler)
        string_utils = configured_container.resolve(StringUtils)
        data_detector = configured_container.resolve(DataTypeDetector)
        ui_adapter = configured_container.resolve(UIFrameworkAdapter)
        
        assert all([
            isinstance(error_handler, ErrorHandler),
            isinstance(string_utils, StringUtils),
            isinstance(data_detector, DataTypeDetector),
            isinstance(ui_adapter, UIFrameworkAdapter)
        ])
    
    def test_error_handling_integration(self, configured_container):
        """Test error handling across components."""
        error_handler = configured_container.resolve(ErrorHandler)
        
        # Test error handling doesn't break
        try:
            error_handler.handle_error(Exception("Test error"), "Test context")
        except Exception as e:
            pytest.fail(f"Error handler should not raise: {e}")
    
    def test_data_flow_integration(self):
        """Test data flow from record to validation."""
        # Create a record
        record = DataRecord(
            data={"name": "Test User", "email": "test@example.com", "age": 25}
        )
        
        # Validate the record
        validator = DataValidationService()
        errors = validator.validate_record(record)
        
        # Should be valid
        assert len(errors) == 0
        
        # Test with invalid data
        invalid_record = DataRecord(
            data={"name": "", "email": "invalid", "age": "not_a_number"}
        )
        
        errors = validator.validate_record(invalid_record)
        assert len(errors) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
