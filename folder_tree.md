# Data Forge API - Project Structure

This document shows the essential folder structure of the Data Forge FastAPI project based on the dependencies used in `main.py` and the actual module imports.

## Essential Project Structure

```
react-fast-V13/
├── app/
│   ├── __init__.py
│   ├── main.py                              # Main FastAPI application entry point
│   │
│   ├── config/                              # Configuration modules
│   │   ├── __init__.py
│   │   ├── logging_config.py               # Logging configuration (USED)
│   │   ├── logging_utils.py                # Logging utilities (USED)
│   │   ├── settings.py                     # Application settings (USED)
│   │   └── api_limits.py                   # API rate limiting config
│   │
│   ├── core/                               # Core functionality
│   │   ├── __init__.py
│   │   ├── io_operations.py               # I/O operations (USED)
│   │   ├── config.py                      # Core configuration (USED)
│   │   ├── config_windows.py              # Windows-specific config (USED)
│   │   └── performance.py                 # Performance utilities (USED)
│   │
│   ├── application/                         # Application layer
│   │   ├── __init__.py
│   │   └── services/                       # Application services
│   │       ├── __init__.py
│   │       └── schema_service.py           # Schema management service (USED)
│   │
│   ├── domain/                             # Domain layer
│   │   ├── __init__.py
│   │   ├── entities/                       # Domain entities
│   │   │   ├── __init__.py
│   │   │   ├── schema.py                   # Schema domain models (USED)
│   │   │   └── write_models.py             # Write operation models (USED)
│   │   ├── exceptions/                     # Domain exceptions
│   │   │   ├── __init__.py
│   │   │   └── exceptions.py               # Custom exceptions
│   │   └── repositories/                   # Repository interfaces
│   │       └── __init__.py
│   │
│   ├── infrastructure/                     # Infrastructure layer
│   │   ├── __init__.py
│   │   └── persistence/                    # Data persistence
│   │       ├── __init__.py
│   │       └── metadata/                   # Metadata management
│   │           ├── __init__.py
│   │           └── schema_config.py        # Schema configuration (USED)
│   │
│   └── api/                                # API layer
│       ├── __init__.py
│       ├── routes/                         # API route handlers
│       │   ├── __init__.py
│       │   ├── health.py                   # Health check endpoints (USED)
│       │   ├── schemas.py                  # Schema endpoints (USED)
│       │   ├── reads.py                    # Read operation endpoints (USED)
│       │   └── writes.py                   # Write operation endpoints (USED)
│       ├── responses/                      # API response models
│       │   ├── __init__.py
│       │   └── response.py                 # Response utilities (USED)
│       └── routers/                        # API routers
│           └── __init__.py
│
├── requirements.txt                        # Python dependencies
├── requirements.in                         # Input requirements
├── requirements.lock                       # Locked requirements
├── README.md                              # Project documentation
└── app.bat                                # Windows batch file for running the app
```

## Currently Used Files

### **ACTIVELY USED**
- `app/main.py` - Main application entry point
- `app/config/logging_config.py` - Logging configuration
- `app/config/logging_utils.py` - Logging utilities
- `app/config/settings.py` - Application settings
- `app/config/api_limits.py` - API rate limiting
- `app/core/io_operations.py` - I/O operations
- `app/core/config.py` - Core configuration
- `app/core/config_windows.py` - Windows-specific configuration
- `app/core/performance.py` - Performance utilities
- `app/application/services/schema_service.py` - Schema management
- `app/domain/entities/schema.py` - Schema domain models
- `app/domain/entities/write_models.py` - Write operation models
- `app/domain/exceptions/exceptions.py` - Custom exceptions
- `app/infrastructure/persistence/metadata/schema_config.py` - Schema definitions
- `app/api/routes/health.py` - Health check endpoints
- `app/api/routes/schemas.py` - Schema endpoints
- `app/api/routes/reads.py` - Read operation endpoints
- `app/api/routes/writes.py` - Write operation endpoints
- `app/api/responses/response.py` - Response utilities
