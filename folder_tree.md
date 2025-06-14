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