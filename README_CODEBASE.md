# Codebase Guide

## What This Project Does
This project migrates data between heterogeneous sources and destinations with a shared pipeline.

Primary use cases:
- SQL Server -> PostgreSQL
- CSV -> SQL Server
- SQL Server -> GeoPackage
- GeoPackage -> PostgreSQL

The entry point is `migrate.py`, which reads a YAML config and dispatches work through `UnifiedMigrationProcessor`.

## Core Architecture
- `base_interfaces.py`: Shared importer/exporter contracts.
- `unified_processor.py`: Orchestrates all migration modes.
- `config_loader.py`: Loads and validates YAML configuration.
- `status_tracker.py`: Persists migration state and progress.

Importer/Exporter implementations:
- `mssql_adapter.py`: SQL Server source importer.
- `postgres_adapter.py`: PostgreSQL destination exporter.
- `csv_importer_handler.py`: CSV source importer.
- `mssql_exporter.py`: SQL Server destination exporter.
- `gpkg_handler.py`: GeoPackage import/export.

Supporting components:
- `schema_extractor.py`: SQL Server schema extraction helpers.
- `bcp_exporter.py`: BCP-based extraction and chunked export utilities.
- `postgres_loader.py`: COPY-based PostgreSQL loading logic.
- `flatfile_handler.py`: Flat-file adapters.

## Data Flow (Typical)
1. Load config (`config.yaml` or a mode-specific config).
2. Validate source/destination and migration settings.
3. Build table-level jobs.
4. Export schema and data to intermediate paths.
5. Create destination schema/table objects.
6. Load data, verify row counts, and produce reports.

## Configuration Files
- `config.yaml`: Main migration configuration.
- `config_csv_import.yaml`: CSV -> SQL Server mode configuration.
- `config.example.yaml`: Starter template.

Important sections in config:
- `source`
- `destination`
- `directories`
- `migration`
- `performance`
- `logging`
- `mode` (for explicit mode routing)

## Running the Project
Install dependencies:

```powershell
pip install -r requirements.txt
```

Run default mode:

```powershell
python migrate.py
```

Run CSV import mode:

```powershell
python migrate.py config_csv_import.yaml
```

## Output and Intermediate Artifacts
- `sql2pg_copy/intermediate/`: table-level data and generated SQL
- `sql2pg_copy/output/`: logs, status, and migration reports

## Notes for Contributors
- Keep adapters behind the shared interfaces in `base_interfaces.py`.
- Prefer mode-specific behavior in `unified_processor.py` rather than one-off scripts.
- Preserve path and table naming conventions already used by intermediate/output folders.
- Use structured logging and avoid print-based control flow in core pipeline modules.
