# Data Forge – Code Duplication & Consistency Review (2025-08-09)

This report highlights duplicated functions/classes/modules and critical inconsistencies detected across the codebase. Each item includes impact and concrete remediation suggestions.

## High severity

- Duplicate routes in `app/api/routes/health.py`
  - Two handlers are registered on the same path: `@router.get("/")`
    - `health_check()` and `root()` conflict under prefix `/health`.
  - Impact: Route override/registration error; unstable behavior in health endpoints.
  - Fix: Keep only one `GET /health/` route. Rename the second to e.g. `@router.get("/info")` or remove it if redundant.

- Config split and inconsistent usage (`config.py` vs `config_windows.py`)
  - Duplicated constants and helpers: `get_parquet_path`, `get_write_parquet_path`, `get_write_feather_path`, `get_file_size_mb`, `ensure_directories` exist in both modules.
  - Mixed imports:
    - `app.core.io_operations` imports from `app.core.config` (generic)
    - App lifecycle and info endpoints import from `app.core.config_windows` (Windows)
  - Impact: Reads/writes and directory resolution can drift (e.g., generic `DATA_DIR` vs Windows `TABLES_DIR`). Different compression/row group defaults can be applied unpredictably.
  - Fix:
    - Pick a single source of truth (prefer `settings.py` + `config_windows.py` for Windows), and update `io_operations.py` to import the same helpers/paths.
    - Optionally make `app.core.config` a thin proxy to `config_windows` on Windows to avoid duplication.

- Missing import in `app/application/services/schema_service.py`
  - `_load_schemas()` calls `logging.warning(...)` but `import logging` is missing.
  - Impact: NameError on startup or when a schema load fails.
  - Fix: Add `import logging` at top of file.

- Unreachable/erroneous tail code in `app/api/routes/reads.py`
  - After the `duckdb_read_ultra_fast` handler, there’s a stray docstring and `return await duckdb_read_ultra_fast(schema_name)` at function tail.
  - Impact: Dead code and potential confusion; if ever reached, it would recurse indefinitely.
  - Fix: Remove the stray lines. If a legacy alias is needed, add a new, explicit route like `@router.get("/duckdb-read/{schema_name}")` delegating to the main handler.

## Medium severity

- Response model vs raw Response mismatch (schemas endpoints)
  - Endpoints in `app/api/routes/schemas.py` declare `response_model` but return `FastJSONResponse` directly.
  - Impact: OpenAPI may misrepresent schemas; FastAPI skips model validation/coercion.
  - Fix: Either return plain dict/list objects (let FastAPI serialize) or drop `response_model` hints when returning `Response` objects.

- Logging label inconsistency in `io_operations`
  - `log_operation("read ", ...)` (note trailing space) in DuckDB read differs from `"read"` used elsewhere.
  - Impact: Aggregation/dashboards split by operation label.
  - Fix: Standardize to `"read"`.

- Windows DuckDB configuration consistency
  - `config_windows.py` defines `DUCKDB_MEMORY_LIMIT = 8192` (MB), while `settings.py` uses a string (`"8GB"`). In `startup.py`, memory settings are applied as `'{value}MB'`.
  - Impact: Confusing dual sources; drift between displayed and applied settings.
  - Fix: Normalize on `settings` as the single source and convert units in one place when applying.

- Directory constants duplicated
  - `FileSchemaRepository` uses hardcoded `"data/schemas"` and `"data/schemas_archive"` while `settings.py` and `config_windows.py` also define schema paths.
  - Impact: Changes to data layout require touching multiple places.
  - Fix: Inject paths from `settings` (or config) into `FileSchemaRepository` as defaults.

- Batch write approach in `batch_write_parquet`
  - Each batch reads the entire existing file and rewrites it with concatenation.
  - Impact: O(N^2) I/O, high memory/CPU for large datasets.
  - Fix: Use append-friendly writers (PyArrow ParquetWriter with `append=True` or Polars `sink_parquet`/streaming), or stage batches then concatenate once.

## Low severity / hygiene

- Stale references in `DataForge.bat`
  - Mentions `requirements_windows.txt` and `test_windows.py` that aren't in the repo.
  - Fix: Update help text or provide those files.

- Info endpoint lists non-existent “legacy” paths
  - `app/api/routes/info.py` includes `legacy_polars_reads` and `legacy_duckdb_reads` that don’t exist.
  - Fix: Align docs with actual routes or add the routes.

- Minor unused/extra imports in a few modules
  - Consider trimming to keep import times and lints clean.

## Concrete collisions and duplicates (by file)

- `app/api/routes/health.py`
  - Duplicate: two `@router.get("/")` handlers (conflict)
- `app/core/config.py` vs `app/core/config_windows.py`
  - Duplicate helpers and constants; diverging defaults
- `app/api/routes/reads.py`
  - Stray legacy docstring + recursive return at function tail
- `app/application/services/schema_service.py`
  - Missing `import logging`

## Recommended immediate fixes (safe, small diffs)

1) Health routes
- Remove/rename the second `@router.get("/")` in `app/api/routes/health.py`.

2) Schema service import
- Add `import logging` to `app/application/services/schema_service.py`.

3) Reads route cleanup
- Delete trailing legacy lines in `app/api/routes/reads.py` and, if needed, add explicit legacy route.

4) Unify config usage
- Update `app/core/io_operations.py` to import from `app.core.config_windows` (or make `config.py` delegate to it) so all paths/compression settings match.

5) Logging label
- Change `log_operation("read ", ...)` to `log_operation("read", ...)` in `io_operations.py`.

## Strategic follow-ups

- Consolidate config
  - Create a single `config` facade driven by `settings.py`, with OS-specific overrides internally, eliminating duplicate helpers.

- Repository path injection
  - Parameterize `FileSchemaRepository` with paths from `settings` by default to avoid hardcoded strings.

- Parquet append strategy
  - Replace read-concat-write with a proper append writer or staged merge to handle very large datasets efficiently.

## Quick spot-checks to add to CI

- Lint for duplicate FastAPI routes within a router.
- Static check for missing imports when using `logging`/`os`/etc.
- Unit test asserting the single `/health/` JSON shape and presence of `/` root info endpoint.

## Requirements coverage

- Duplicated functions/classes/modules: Reviewed and listed (config helpers, health routes, reads tail code)
- Critical inconsistencies: Identified with fixes (route conflicts, config drift, missing import)

---
Prepared on 2025-08-09.
