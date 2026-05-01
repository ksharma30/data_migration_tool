"""
SQL Server to PostgreSQL Migration Tool

A high-performance migration tool using BCP and PostgreSQL COPY
for efficient data transfer of large tables.

Modules:
    - config_loader: Configuration loading and validation
    - schema_extractor: SQL Server schema extraction
    - bcp_exporter: BCP export handler
    - postgres_loader: PostgreSQL COPY loader
    - migration_processor: Main migration orchestrator
    - migrate: Main entry point

Usage:
    python migrate.py [config_file]
"""

__version__ = "1.0.0"
__author__ = "Database Migration Team"

# Import main components for easier access
from .config_loader import load_config, validate_config, setup_logging
from .schema_extractor import SchemaExtractor
from .bcp_exporter import BCPExporter
from .postgres_loader import PostgreSQLLoader
from .migration_processor import MigrationProcessor

__all__ = [
    'load_config',
    'validate_config', 
    'setup_logging',
    'SchemaExtractor',
    'BCPExporter',
    'PostgreSQLLoader',
    'MigrationProcessor',
]

