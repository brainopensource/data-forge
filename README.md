# Data Forge - Modern Architecture for Future Growth

## 1. Executive Summary & Goals
- **Primary Objective:** Modular Application to achieve **State-of-the-Art (SOTA) performance** for critical data operations while maintaining a modern, robust, and simple design that supports future feature expansion and rapid development, capable to handle 1 bilion rows of data.

- **Key Goals:**
    1. **Optimize SOTA Performance:** Performance for critical operations like bulk reads and writes by minimizing abstraction layers and maximizing direct data paths, leveraging Parquet, Polars, PyArrow, Pydantic or DuckDB.
    2. **Pragmatic Modularity:** Establish a clear middle ground between over-abstraction and pragmatic design to enhance maintainability, extensibility, and the efficient integration of new, diverse features.
    3. **Schema-Driven Flexibility:** Retain the powerful schema-driven design to allow unparalleled user flexibility in database management and dynamic data handling.

## 2. Project Structure

```
src/
├── api/
│   ├── routers/
│   │   ├── analytics.py
│   │   ├── bulk_operations.py
│   │   └── external_fetch.py
│   ├── dependencies/
│   │   └── commom.py
├── application/
│   ├── commands/
│   ├── services/
│   │   ├── analytics_service.py
│   │   ├── bulk_data_service.py
│   │   ├── external_connector_service.py
│   │   ├── ml_service.py
│   │   └── query_service.py
├── config/
│   ├── api_limits.py
│   ├── logging_config.py
│   └── settings.py
├── container/
│   └── container.py
├── domain/
│   ├── entities/
│   │   └── schema.py
│   ├── exceptions/
│   │   └── exceptions.py
│   ├── repositories/
│   │   └── schema_repository.py
├── infrastructure/
│   ├── analytics/
│   │   └── analytics_operations.py
│   ├── jobs/
│   │   └── scheduler.py
│   ├── persistence/
│   │   ├── arrow_operations.py
│   │   ├── databases/
│   │   │   ├── connection_pool.py
│   │   │   └── schema_manager.py
│   │   ├── external/
│   │   │   └── external_layer.py
│   │   ├── metadata/
│   │   │   └── schema_config.py
│   │   ├── repositories/
│   │   │   └── file_schema_repository.py
│   ├── web/
│   │   ├── crud.py
│   │   └── responses/
│   │       └── crud.py
├── main.py
```

## 3. Architectural Overview

- **API Layer (`src/api/`):** Handles HTTP request/response cycles, input validation (via FastAPI models), and delegates to the appropriate application services. This layer is kept thin and efficient.
    - **Routers (`src/api/routers/`):** Dedicated files for different API concerns, ensuring clear separation for new features.
        - `bulk_operations.py`: Optimized bulk insert/read operations.
        - `external_fetch.py`: Fetching data from external sources.
        - `analytics.py`: Complex analytical queries.
    - **Dependencies (`src/api/dependencies/`):** Common dependencies for FastAPI routes.

- **Application Layer (`src/application/`):** Contains the core business logic, orchestrating interactions between the domain and infrastructure layers.
    - **Services (`src/application/services/`):**
        - `bulk_data_service.py`: Ultra-fast bulk insert/read operations.
        - `query_service.py`: Complex queries, aggregations, and data retrieval.
        - `external_connector_service.py`: External database connections and sync.
        - `analytics_service.py`: Advanced analytics and data transformations.
        - `ml_service.py`: Machine learning related services.
    - **Commands (`src/application/commands/`):** Command pattern implementations (if any).

- **Domain Layer (`src/domain/`):** Defines core business entities, repositories, and exceptions.
    - **Entities (`src/domain/entities/`):** Schema-driven definitions and business rules (`schema.py`).
    - **Repositories (`src/domain/repositories/`):** Interfaces for data access (`schema_repository.py`).
    - **Exceptions (`src/domain/exceptions/`):** Domain-specific exceptions.

- **Infrastructure Layer (`src/infrastructure/`):** Manages persistence, external integrations, and web adapters.
    - **Analytics (`src/infrastructure/analytics/`):** Aggregations and reporting logic (`analytics_operations.py`).
    - **Jobs (`src/infrastructure/jobs/`):** Scheduled tasks and background processing (`scheduler.py`).
    - **Persistence (`src/infrastructure/persistence/`):**
        - `arrow_operations.py`: Optimized Arrow-based bulk operations.
        - **Databases (`src/infrastructure/persistence/databases/`):**
            - `connection_pool.py`: Connection pooling.
            - `schema_manager.py`: Schema management.
        - **External (`src/infrastructure/persistence/external/`):**
            - `external_layer.py`: External database adapters.
        - **Metadata (`src/infrastructure/persistence/metadata/`):**
            - `schema_config.py`: Schema configuration.
        - **Repositories (`src/infrastructure/persistence/repositories/`):**
            - `file_schema_repository.py`: Concrete repository implementations.
    - **Web (`src/infrastructure/web/`):**
        - `crud.py`: Web utilities.
        - **Responses (`src/infrastructure/web/responses/`):**
            - `crud.py`: Arrow-native or custom responses.

- **Configuration (`src/config/`):** Centralized settings, logging, and API limits.
    - `settings.py`, `logging_config.py`, `api_limits.py`

- **Dependency Container (`src/container/`):** Lightweight DI container for managing dependencies (`container.py`).

- **Main Entry Point (`src/main.py`):** Application startup and FastAPI app instantiation.

## 4. Key Considerations & Risk Mitigation

- **Performance vs. Modularity:** Strictly minimize abstraction in performance-critical paths, but maintain modularity for extensibility.
- **Complexity in New Features:** Isolate feature-specific logic to prevent core performance degradation.
- **Dependencies:** Focus on a stable, high-performance core before expanding integrations.
- **Non-Functional Requirements:** SOTA performance, scalability, maintainability, extensibility, and usability are all addressed by the modular, schema-driven design.

## 5. Assumptions
- The application is local-first, prioritizing performance for core operations (e.g., using Parquet, DuckDB).
- Future features will be developed incrementally.
- Schema flexibility and interactive data exploration are highly valued by users.