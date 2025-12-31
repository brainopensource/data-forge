"""Domain package for business entities and services."""

from .entities.data_record import DataRecord
from .services.data_validation_service import DataValidationService

__all__ = ['DataRecord', 'DataValidationService']
