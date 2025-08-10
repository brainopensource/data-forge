# Data Forge API V13 - Project Structure

This document shows the essential folder structure of the Data Forge FastAPI project after the configuration refactoring. The project now uses a unified global configuration system for optimal performance.

## Essential Project Structure

```
react-fast-V13/
├── app/
│   ├── main.py                              # Main FastAPI application entry point
│   │
│   ├── config/                              # Unified configuration system
│   │   ├── global_settings.py              # Global configuration (CORE - replaces all legacy configs)
│   │   ├── logging_config.py               # Logging configuration (USED)
│   │   └── logging_utils.py                # Logging utilities (USED)
│   │
│   ├── core/                               # Core functionality
│   │   ├── application.py                  # FastAPI application factory (USED)
│   │   ├── startup.py                      # Unified startup manager (USED)
│   │   ├── server.py                       # Server configuration (USED)
│   │   ├── io_operations.py               # High-performance I/O operations (USED)
│   │   ├── performance.py                 # Performance monitoring utilities (USED)
│   │   ├── health.py                      # Health check utilities (USED)
│   │   └── init.py                        # Core initialization (USED)
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
│   │   │   └── exceptions.py              # Custom exceptions (USED)
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
│       │   ├── info.py                   # System and performance info endpoints (USED)
│       │   ├── schemas.py                # Schema management endpoints (USED)
│       │   ├── reads.py                  # Read operation endpoints (USED)
│       │   ├── writes.py                 # Write operation endpoints (USED)
│       │   └── docs.py                   # Documentation endpoints (USED)
│       └── responses/                    # API response models
│           └── response.py               # Response utilities (USED)
│
├── frontend/                              # Frontend application
│   └── app.py                            # Frontend server
│
├── static/                               # Static assets
│   ├── css/                             # Stylesheets
│   ├── js/                              # JavaScript files
│   └── images/                          # Image assets
│
├── logs/                                 # Application logs
├── temp/                                # Temporary files
├── tests/                               # Test suite
├── requirements.txt                      # Python dependencies
├── requirements.in                       # Input requirements
├── requirements.lock                     # Locked requirements
├── uv.lock                              # UV lock file
├── pyproject.toml                       # Project configuration
├── README.md                            # Project documentation
└── DataForge.bat                        # Windows batch file for running the app
```

### 🏗️ **Core System Architecture**

**Configuration Classes**:
- `APIConfig` - Server settings and resource limits
- `DataConfig` - Data directories and file templates
- `PerformanceConfig` - Ultra-high performance parameters (10M+ rows/second)
- `WriteProfiles` - ULTRA_FAST, BALANCED, HIGH_COMPRESSION profiles
- `LibraryConfig` - Polars, DuckDB, Arrow optimizations
- `PlatformOptimizations` - Automatic Windows/Unix optimizations

**Performance Features**:
- Auto system detection (CPU, Memory, Platform)
- Intelligent threading (14 cores on 16-core system)
- Memory optimization (8GB DuckDB, 4GB Arrow)
- Platform-specific I/O optimizations
- Zero-dependency configuration system

## Currently Used Files

### **CORE CONFIGURATION**
- `app/config/global_settings.py` - **CENTRAL CONFIG** (replaces 4+ legacy files)
- `app/config/logging_config.py` - Logging configuration
- `app/config/logging_utils.py` - Logging utilities

### **CORE SYSTEM**
- `app/main.py` - Main application entry point
- `app/core/application.py` - FastAPI application factory
- `app/core/startup.py` - Unified startup manager
- `app/core/server.py` - Production server configuration
- `app/core/io_operations.py` - High-performance I/O operations
- `app/core/performance.py` - Performance monitoring
- `app/core/health.py` - System health checks
- `app/core/init.py` - Core initialization

### **BUSINESS LOGIC**
- `app/application/services/schema_service.py` - Schema management service
- `app/domain/entities/schema.py` - Schema domain models
- `app/domain/entities/write_models.py` - Write operation models
- `app/domain/exceptions/exceptions.py` - Custom exceptions
- `app/infrastructure/persistence/metadata/schema_config.py` - Schema definitions

### **API ENDPOINTS**
- `app/api/routes/health.py` - Health check endpoints
- `app/api/routes/info.py` - System and performance info
- `app/api/routes/schemas.py` - Schema management endpoints
- `app/api/routes/reads.py` - Read operation endpoints
- `app/api/routes/writes.py` - Write operation endpoints
- `app/api/routes/docs.py` - Documentation endpoints
- `app/api/responses/response.py` - Response utilities

## Performance Optimizations

### **System Auto-Detection**
- Platform: Windows (auto-detected)
- CPU Cores: 16 (using 14 for processing)
- Memory: 31.9 GB
- Optimal settings applied automatically

### **High-Performance Defaults**
- DuckDB: 14 threads, 8GB memory
- Arrow: 4GB memory pool
- Batch Processing: 900K records
- Row Groups: 1M rows
- Streaming: 1M chunk size
- Compression: zstd (ultra-fast) / snappy (balanced)

### **Write Optimization Profiles**
- `ULTRA_FAST`: Maximum speed, minimal compression
- `BALANCED`: Good speed/compression balance
- `HIGH_COMPRESSION`: Maximum compression for storage

This architecture provides a **clean, efficient, and high-performance foundation** for the SOTA GOD API with automatic system optimization and 10M+ rows/second throughput capability.
