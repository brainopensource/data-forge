react-fast-V12\
└── app
    ├── application
    │   ├── command_handlers
    │   │   └── bulk_data_command_handlers.py
    │   └── commands
    │       └── bulk_data_commands.py
    ├── config
    │   ├── api_limits.py
    │   ├── logging_config.py
    │   └── settings.py
    ├── container
    │   └── container.py
    ├── domain
    │   ├── entities
    │   │   └── schema.py
    │   ├── repositories
    │   │   └── schema_repository.py 
    │   └── exceptions.py
    ├── infrastructure
    │   ├── metadata
    │   │   └── schemas_description.py
    │   ├── persistence
    │   │   ├── duckdb 
    │   │   │   ├── connection_pool.py
    │   │   │   └── schema_manager.py
    │   │   ├── repositories
    │   │   │   └── file_schema_repository.py
    │   │   └── arrow_bulk_operations.py
    │   └── web
    │       ├── dependencies 
    │       │   └── common.py
    │       ├── routers 
    │       │   └── arrow_performance_data.py
    │       └── arrow.py
    └── main.py