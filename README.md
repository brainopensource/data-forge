# Data Forge - Dynamic Data Platform v0.4.0

## High-Level Project Description

This document presents an overview of the architecture, design principles, and strategic decisions behind our data platform project, designed to manage and interact with a wide variety of "tables" (or *datasets*) in a generic and efficient way.

---

### Overview and Purpose

Our goal is to build a **flexible and scalable data platform** that supports ingestion and querying across different datasets. The key concept is **genericity**: instead of creating a specific API and database schema for each data type (users, products, sales, etc.), the platform will be **schema-driven**. This means the structure of each "table" will be defined by metadata (the schema), and the application will dynamically adapt to that definition to perform read/write operations.

---

### Key Decisions and Principles

1.  **FastAPI as Web Framework:** Chosen for its high performance, ease of use, strong typing (via Pydantic), automatic documentation (Swagger/OpenAPI), and asynchronous support—ideal for efficient I/O operations.
2.  **DuckDB as Database:** For the MVP, we use DuckDB due to its embeddable nature, strong analytical performance for tabular data, and flexibility in handling dynamic schemas (creating tables "on-the-fly" or using "schema-on-read"). This simplifies early persistence management.
3.  **Schemas Defined in File:** For the MVP, schema definitions will be stored in local Python files (e.g., `schemas_description.py`). This reduces complexity during early development by avoiding the need for a schema management API, accelerating delivery. Complex schema management and the us of AI to auto-improve the project will be considered in another phase, not in the MVP.
4.  **Focus on Genericity and DRY (Don't Repeat Yourself):** The architecture is designed so that adding a new dataset (a new "table") requires **minimal coding effort**. Data handling logic is generic, driven by schema definitions rather than hardcoded data types.
5.  **Code Quality and Maintainability:** From the beginning, the project is structured to support collaboration, code clarity, and future evolution.

---

### Architecture and Design Patterns

The project architecture is built around the following principles and patterns:

1.  **Hexagonal Architecture (Ports & Adapters):**
    * **Core Principle:** The application core (business logic) is isolated and independent from external technologies (web frameworks, databases).
    * **Structure:** The project is divided into distinct layers:
        * **Domain (`app/domain/`):** Contains **pure business logic**, entities (`Schema`, `DataRecord`), and interface definitions (Ports) to interact with the external world (Repositories). This is the "why" of the system.
        * **Application (`app/application/`):** Defines **use cases**, orchestrating operations from the domain. This is the "what" the system does.
        * **Infrastructure (`app/infrastructure/`):** Contains **concrete implementations (Adapters)** of domain interfaces, handling technical details like FastAPI, DuckDB, and reading schemas from files. This is the "how" the system does it.
        * **Container (`app/container/`):** Responsible for assembling the application by connecting infrastructure implementations to domain interfaces via Dependency Injection.

2.  **Domain-Driven Design (DDD):**
    * **Core Principle:** Software complexity is managed by aligning code to a rich domain model.
    * **Application:** Reflected in explicit entities like `Schema` and `DataRecord`, which are the conceptual building blocks of our data system. **Domain Services** encapsulate business logic involving multiple entities.

3.  **Command Query Responsibility Segregation (CQRS):**
    * **Core Principle:** Separates state-changing operations (Commands) from read-only operations (Queries).
    * **Application:** Each use case in the Application layer is typically a Command (e.g., `create_data_record`) or a Query (e.g., `get_data_record`). This allows independent optimization of read/write paths in the future.

4.  **Dependency Injection (DI):**
    * **Core Principle:** Classes receive their dependencies from an external source (the DI Container) instead of creating them.
    * **Benefits:** Improves testability, maintainability, and flexibility (e.g., switching from DuckDB to PostgreSQL without changing the Domain or Application layers).

5.  **Pydantic for Validation and Modeling:**
    * **Core Principle:** Leverages static typing and data models for robust input/output validation and entity modeling.
    * **Application:** Widely used in DTOs (Data Transfer Objects) in the Application layer and FastAPI request/response models. Crucial for `Schema` and `DataRecord` entities to ensure metadata and data integrity.

---

### High-Level Operation Flow

1.  **Initialization:**
    * The FastAPI application starts (`main.py`).
    * Configuration is loaded (`config/settings.py`).
    * The **DI Container** (`container/container.py`) initializes and configures all dependencies, including DuckDB connection, file-based schema repository, and the generic data repository.
    * `FileSchemaRepository` loads schema definitions from `schemas_description.py` into memory.
    * The DuckDB `SchemaManager` is injected and can be used to **ensure tables corresponding to defined schemas exist in DuckDB** during startup or on the first write operation.

2.  **Data Request (e.g., Create a Record):**

3.  **Data Insert**

---

## Folder Structure

```
react-fast-V12/
├── app/
│   ├── application/
│   │   ├── dto/                    # Data Transfer Objects for API contracts
│   │   │   ├── create_data_dto.py
│   │   │   ├── data_dto.py
│   │   │   ├── query_dto.py
│   │   │   ├── query_request_dto.py
│   │   │   └── schema_dto.py
│   │   └── use_cases/              # Business use cases (CQRS pattern)
│   │       ├── create_bulk_data_records.py
│   │       ├── create_data_record.py
│   │       ├── get_data_record.py
│   │       └── query_data_records.py  # Includes Query, Stream, Count use cases
│   ├── config/                     # Configuration and settings
│   │   ├── api_limits.py
│   │   ├── logging_config.py
│   │   └── settings.py
│   ├── container/                  # Dependency injection
│   │   └── container.py
│   ├── domain/                     # Business core (Domain layer)
│   │   ├── entities/
│   │   ├── exceptions.py
│   │   ├── repositories/
│   │   └── services/
│   ├── infrastructure/             # External adapters
│   │   ├── metadata/
│   │   ├── persistence/
│   │   │   ├── duckdb/
│   │   │   │   ├── connection_pool.py
│   │   │   │   ├── query_builder.py
│   │   │   │   └── schema_manager.py
│   │   │   └── repositories/
│   │   └── web/
│   │       ├── dependencies/
│   │       │   ├── common.py
│   │       │   ├── profiling.py
│   │       │   └── timing.py
│   │       └── routers/
│   └── main.py
├── data/                          # DuckDB database files
├── docs/                          # Project documentation
├── external/                      # Mock data and external resources
├── frontend/                      # Web UI components
├── logs/                          # Application logs
├── tests/                         # Test suite
├── complete_tests.py              # Integration tests
├── performance_tests.py           # Performance benchmarks
└── requirements.txt
```

---

## Main Layers and Architecture Patterns

The project follows a **Hexagonal Architecture (Ports & Adapters)** that ensures clear separation of concerns and isolates core business logic from external technologies.

### `app/domain/` (Business Core - Domain Layer)

* **Purpose:** Contains **pure business logic**, rules, behaviors, and core entities of the system. Completely agnostic to frameworks and persistence technologies.
* **Patterns Used:**
    * **Domain-Driven Design (DDD)**: Entities (`Schema`, `DataRecord`), **Repository Interfaces** (Ports), and **Domain Services** (`data_management.py`).
    * **SOLID Principles** and **Object Calisthenics** compliance for clean modeling.

### `app/application/` (Use Case Orchestrator - Application Layer)

* **Purpose:** Defines the **use cases** of the system. Orchestrates domain operations to fulfill user requirements. Contains no complex business rules itself.
* **Patterns Used:**
    * **CQRS (Command Query Responsibility Segregation)**: Separate use cases for commands (CreateDataRecordUseCase, CreateBulkDataRecordsUseCase) and queries (QueryDataRecordsUseCase, StreamDataRecordsUseCase, CountDataRecordsUseCase, GetDataRecordUseCase).
    * **DTOs (Data Transfer Objects)**: Pydantic models for request/response validation and API contracts.

### `app/infrastructure/` (External Adapters - Infrastructure Layer)

* **Purpose:** Contains all **external adapters** and technical implementations. This layer "adapts" external technologies to the domain interfaces.
* **Components:**
    * **Web Layer**: FastAPI routers, dependencies, and HTTP adapters
    * **Persistence Layer**: DuckDB repositories, connection pooling, and query building
    * **Metadata Layer**: Schema definitions and management

### Key Architecture Improvements

* **Performance Optimizations:**
    * Connection pooling for DuckDB
    * Streaming responses for large datasets
    * Bulk operations for high-throughput writes
    * Query builder for optimized SQL generation

* **Enhanced CQRS Implementation:**
    * Dedicated use cases for streaming (`StreamDataRecordsUseCase`)
    * Count operations (`CountDataRecordsUseCase`)
    * Bulk operations (`CreateBulkDataRecordsUseCase`)

* **Advanced Web Features:**
    * Profiling middleware for performance monitoring
    * Comprehensive error handling with proper HTTP status codes
    * NDJSON streaming for efficient data transfer
    * **CQRS (Command Query Responsibility Segregation)** using separate handlers for commands and queries (`create_data_record.py`, `get_data_record.py`).
    * **DTOs (Data Transfer Objects)** to map external inputs/outputs to internal entities.

### `app/infrastructure/` (Adapters - Infrastructure Layer)

* **Purpose:** Concrete implementations of interfaces declared in the domain layer. Connects business logic to technologies such as DuckDB, FastAPI, file systems, etc.
* **Patterns Used:**
    * Implements **Adapters** in the hexagonal architecture.
    * Uses **Dependency Injection (DI)** to bind implementations to use cases.
    * Includes:
        * `persistence/`: Concrete repositories and `schema_manager` for dynamic DDL.
        * `web/`: FastAPI endpoints and dependencies.
        * `metadata/`: Static schema declarations.

### `app/container/` (Dependency Injection)

* **Purpose:** Centralizes dependency configuration and wiring of components.
* **Patterns Used:**
    * **Inversion of Control (IoC)** and **Dependency Injection (DI)** using a modular container system.

### `app/config/` and `app/main.py`

* **Purpose:**
    * `settings.py`: Environment-based configuration management.
    * `main.py`: Initializes the FastAPI application, mounts routes, and loads container dependencies.

---

## Current Implementation Status

The Data Forge platform is now fully operational with a comprehensive set of API endpoints and advanced features:

### Implemented API Endpoints

* **POST /api/v1/records** - Create single data record
* **POST /api/v1/records/bulk** - High-performance bulk record creation
* **GET /api/v1/records/{schema_name}** - Paginated record retrieval with filtering and sorting
* **GET /api/v1/records/{schema_name}/stream** - High-performance streaming for large datasets (NDJSON)
* **GET /api/v1/records/{schema_name}/count** - Record count with optional filtering
* **GET /api/v1/records/{schema_name}/{record_id}** - Retrieve specific record by ID
* **GET /api/v1/schemas** - List all available schemas with detailed metadata

### Advanced Features

* **High-Performance Streaming**: NDJSON streaming for efficient processing of large datasets
* **Bulk Operations**: Optimized batch inserts for high-throughput scenarios
* **Connection Pooling**: Efficient DuckDB connection management
* **Query Builder**: Dynamic SQL generation with proper parameterization
* **Comprehensive Filtering**: Support for multiple operators (eq, ne, gt, gte, lt, lte, in, like, ilike, is_null, is_not_null)
* **Performance Monitoring**: Built-in profiling middleware for execution time tracking
* **Robust Error Handling**: Proper HTTP status codes and detailed error messages

### Use Cases Implemented

1. **CreateDataRecordUseCase** - Single record creation with validation
2. **CreateBulkDataRecordsUseCase** - Batch record creation for performance
3. **QueryDataRecordsUseCase** - Paginated queries with filtering and sorting
4. **StreamDataRecordsUseCase** - Async streaming for large result sets
5. **CountDataRecordsUseCase** - Efficient record counting with filters
6. **GetDataRecordUseCase** - Single record retrieval by ID

### Testing and Quality Assurance

* **Integration Tests**: Complete API endpoint testing in `complete_tests.py`
* **Performance Tests**: Benchmarking for bulk operations and streaming in `performance_tests.py`
* **Error Handling Tests**: Comprehensive validation of error scenarios
* **Streaming Tests**: NDJSON format validation and large dataset handling

### Performance Characteristics

* **Bulk Insert**: Optimized for high-throughput batch operations using DuckDB's `executemany()`
* **Streaming**: Memory-efficient processing of large datasets with async generators
* **Query Performance**: Advanced query builder with proper indexing on `id` and `created_at` fields
* **Connection Efficiency**: Connection pooling reduces overhead for concurrent requests

For detailed API documentation, see [API_ENDPOINTS.md](./API_ENDPOINTS.md).
