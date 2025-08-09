# Data Forge API V13 - Project Structure

This document shows the essential folder structure of the Data Forge FastAPI project based on the dependencies used in `main.py` and the actual module imports.

## Essential Project Structure

```
react-fast-V13/
├── app/
│   ├── main.py                              # Main FastAPI application entry point
│   │
│   ├── config/                              # Configuration modules
│   │   ├── logging_config.py               # Logging configuration (USED)
│   │   ├── logging_utils.py                # Logging utilities (USED)
│   │   ├── settings.py                     # Application settings (USED)
│   │   └── api_limits.py                   # API rate limiting config
│   │
│   ├── core/                               # Core functionality
│   │   ├── io_operations.py               # I/O operations (USED)
│   │   ├── config.py                      # Core configuration (USED)
│   │   ├── config_windows.py              # Windows-specific config (USED)
│   │   ├── performance.py                 # Performance utilities (USED)
│   │   ├── health.py                      # Health check utilities (USED)
│   │   └── startup.py                     # Startup metrics (USED)
│   │
│   ├── application/                       # Application layer
│   │   └── services/                      # Application services
│   │       └── schema_service.py          # Schema management service (USED)
│   │
│   ├── domain/                            # Domain layer
│   │   ├── entities/                      # Domain entities
│   │   │   ├── schema.py                  # Schema domain models (USED)
│   │   │   └── write_models.py            # Write operation models (USED)
│   │   ├── exceptions/                    # Domain exceptions
│   │   │   └── exceptions.py              # Custom exceptions
│   │   └── repositories/                  # Repository interfaces
│   │
│   ├── infrastructure/                    # Infrastructure layer
│   │   └── persistence/                   # Data persistence
│   │       └── metadata/                  # Metadata management
│   │           └── schema_config.py       # Schema configuration (USED)
│   │
│   └── api/                              # API layer
│       ├── routes/                       # API route handlers
│       │   ├── health.py                 # Health check endpoints (USED)
│       │   ├── info.py                   # System and info endpoints (USED)
│       │   ├── schemas.py                # Schema endpoints (USED)
│       │   ├── reads.py                  # Read operation endpoints (USED)
│       │   └── writes.py                 # Write operation endpoints (USED)
│       ├── responses/                    # API response models
│       │   └── response.py               # Response utilities (USED)
│       └── routers/                      # API routers
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
- `app/core/health.py` - Health check utilities
- `app/core/startup.py` - Startup metrics
- `app/application/services/schema_service.py` - Schema management
- `app/domain/entities/schema.py` - Schema domain models
- `app/domain/entities/write_models.py` - Write operation models
- `app/domain/exceptions/exceptions.py` - Custom exceptions
- `app/infrastructure/persistence/metadata/schema_config.py` - Schema definitions
- `app/api/routes/health.py` - Health check endpoints
- `app/api/routes/info.py` - System and info endpoints
- `app/api/routes/schemas.py` - Schema endpoints
- `app/api/routes/reads.py` - Read operation endpoints
- `app/api/routes/writes.py` - Write operation endpoints
- `app/api/responses/response.py` - Response utilities
