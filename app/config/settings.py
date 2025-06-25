"""
Centralized settings configuration for Data Forge API.
Using Pydantic for validation and environment variable support.
"""
try:
    from pydantic_settings import BaseSettings
except ImportError:
    # Fallback for older pydantic versions
    from pydantic import BaseSettings

from pydantic import Field
from typing import Optional
import os


class Settings(BaseSettings):
    """
    Application settings with environment variable support.
    """
    # API Configuration
    api_host: str = Field(default="0.0.0.0", env="API_HOST")
    api_port: int = Field(default=8080, env="API_PORT")
    debug: bool = Field(default=False, env="DEBUG")
    
    # Performance Configuration
    duckdb_threads: int = Field(default=8, env="DUCKDB_THREADS")
    duckdb_memory_limit: str = Field(default="8GB", env="DUCKDB_MEMORY_LIMIT")
    arrow_memory_pool_size: str = Field(default="4GB", env="ARROW_MEMORY_POOL_SIZE")
    
    # Data Configuration
    data_dir: str = Field(default="data", env="DATA_DIR")
    temp_dir: str = Field(default="temp", env="TEMP_DIR")
    n_rows: int = Field(default=1000, env="N_ROWS")
    
    # Performance Tuning
    parquet_row_group_size: int = Field(default=1000000, env="PARQUET_ROW_GROUP_SIZE")
    polars_infer_schema_length: int = Field(default=20, env="POLARS_INFER_SCHEMA_LENGTH")
    ultra_fast_infer_length: int = Field(default=50, env="ULTRA_FAST_INFER_LENGTH")
    default_batch_size: int = Field(default=900000, env="DEFAULT_BATCH_SIZE")
    
    # Validation Settings
    max_validation_errors: int = Field(default=100, env="MAX_VALIDATION_ERRORS")
    validation_batch_size: int = Field(default=10000, env="VALIDATION_BATCH_SIZE")
    skip_validation_threshold: int = Field(default=1000000, env="SKIP_VALIDATION_THRESHOLD")
    
    # Logging Configuration
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_dir: str = Field(default="logs", env="LOG_DIR")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global settings instance
settings = Settings()
