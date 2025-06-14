# app/domain/exceptions.py
class DomainException(Exception):
    """Base exception for all domain-related errors."""
    pass

class SchemaNotFoundException(DomainException):
    """Raised when a requested schema does not exist."""
    pass

class RecordNotFoundException(DomainException):
    """Raised when a requested data record does not exist."""
    pass

class InvalidDataException(DomainException):
    """Raised when data provided does not conform to the schema or business rules."""
    pass

class SchemaValidationException(DomainException):
    """Raised when schema validation fails during table creation or data operations."""
    pass 