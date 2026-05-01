"""
Unified Migration Processor
Uses new importer/exporter interfaces with db.schema.table structure
"""

import os
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

from base_interfaces import Importer, Exporter
from status_tracker import StatusTracker
from mssql_adapter import MSSQLImporter
from mssql_exporter import MSSQLExporter
from postgres_adapter import PostgreSQLExporter
from flatfile_handler import FlatFileImporter, FlatFileExporter
from gpkg_handler import GPKGImporter, GPKGExporter
from csv_importer_handler import CSVImporter

logger = logging.getLogger(__name__)


class MigrationStatus:
    """Migration status constants"""
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    WARNING = "WARNING"


class TableMigrationJob:
    """Represents a single table migration job"""
    
    def __init__(self, database: str, schema: str, table: str):
        self.database = database
        self.schema = schema
        self.table = table
        self.status = "PENDING"
        self.input_count = 0
        self.flatfile_count = 0
        self.output_count = 0
        self.start_time = None
        self.end_time = None
        self.duration = 0
        self.error_message = None
        
    @property
    def full_name(self) -> str:
        """Get fully qualified table name"""
        return f"{self.database}.{self.schema}.{self.table}"
        
    def start(self):
        """Mark job as started"""
        self.start_time = datetime.now()
        self.status = "RUNNING"
        
    def complete(self, status: str, error_message: Optional[str] = None):
        """Mark job as completed"""
        self.end_time = datetime.now()
        self.status = status
        self.error_message = error_message
        if self.start_time:
            self.duration = (self.end_time - self.start_time).total_seconds()


class UnifiedMigrationProcessor:
    """Unified migration processor using new interfaces"""
    
    def __init__(self, config: Dict):
        """
        Initialize migration processor
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.importer: Optional[Importer] = None
        self.exporter: Optional[Exporter] = None
        self.jobs: List[TableMigrationJob] = []
        self.overall_status = MigrationStatus.SUCCESS
        
        # Setup directories
        self._setup_directories()
        
        # Initialize status tracker
        status_file = Path(self.config['directories']['output']) / 'status.yaml'
        self.status_tracker = StatusTracker(status_file)
        
    def _setup_directories(self):
        """Create necessary directories"""
        dirs = self.config['directories']
        for dir_key, dir_path in dirs.items():
            Path(dir_path).mkdir(parents=True, exist_ok=True)
            logger.info(f"Directory ready: {dir_path}")
            
    def _create_importer(self) -> Importer:
        """Create importer based on source type"""
        source_type = self.config['source'].get('type', 'mssql')
        
        if source_type == 'mssql':
            return MSSQLImporter(self.config)
        elif source_type == 'postgres':
            # TODO: Create PostgreSQL importer
            raise NotImplementedError("PostgreSQL importer not yet implemented")
        elif source_type == 'flatfile':
            return FlatFileImporter(self.config)
        elif source_type == 'gpkg':
            return GPKGImporter(self.config)
        elif source_type == 'csv':
            return CSVImporter(self.config)
        else:
            raise ValueError(f"Unknown source type: {source_type}")
            
    def _create_exporter(self) -> Exporter:
        """Create exporter based on destination type"""
        dest_type = self.config['destination'].get('type', 'postgres')
        
        if dest_type == 'postgres':
            return PostgreSQLExporter(self.config)
        elif dest_type == 'mssql':
            return MSSQLExporter(self.config)
        elif dest_type == 'flatfile':
            return FlatFileExporter(self.config)
        elif dest_type == 'gpkg':
            return GPKGExporter(self.config)
        else:
            raise ValueError(f"Unknown destination type: {dest_type}")
            
    def initialize_connections(self):
        """Initialize connections to source and destination"""
        logger.info("Initializing connections")
        
        self.importer = self._create_importer()
        if not self.importer.connect():
            raise ConnectionError("Failed to connect to source")
            
        self.exporter = self._create_exporter()
        if not self.exporter.connect():
            raise ConnectionError("Failed to connect to destination")
            
        logger.info("All connections initialized successfully")
        
    def close_connections(self):
        """Close all connections"""
        logger.info("Closing connections")
        
        if self.importer:
            self.importer.disconnect()
            
        if self.exporter:
            self.exporter.disconnect()
            
    def get_tables_to_migrate(self) -> List[str]:
        """Get list of tables to migrate"""
        migration_config = self.config['migration']
        tables = migration_config.get('tables', [])
        exclude_tables = migration_config.get('exclude_tables', [])
        
        # If no tables specified, get all tables
        if not tables:
            logger.info("No tables specified, fetching all tables from source")
            tables = self.importer.get_tables()
        
        # Remove excluded tables
        tables = [t for t in tables if t not in exclude_tables]
        
        logger.info(f"Found {len(tables)} tables to migrate: {', '.join(tables)}")
        return tables
        
    def get_table_directory(self, database: str, schema: str, table: str) -> Path:
        """
        Get directory path for table intermediate files
        Uses new structure: database/table/
        
        Args:
            database: Database name
            schema: Schema name (not used in path but kept for compatibility)
            table: Table name
            
        Returns:
            Path to table directory
        """
        intermediate_dir = Path(self.config['directories']['intermediate'])
        table_dir = intermediate_dir / database / table
        table_dir.mkdir(parents=True, exist_ok=True)
        return table_dir
        
    def migrate_table(self, table_name: str, source_schema: str = 'dbo',
                     dest_schema: str = 'public') -> TableMigrationJob:
        """
        Migrate a single table
        
        Args:
            table_name: Name of the table
            source_schema: Source schema name
            dest_schema: Destination schema name
            
        Returns:
            TableMigrationJob with results
        """
        # Get database name from source
        source_db = self.config['source'].get('database', 'unknown')
        
        job = TableMigrationJob(source_db, source_schema, table_name)
        job.start()
        
        logger.info("=" * 80)
        logger.info(f"Starting migration for table: {job.full_name}")
        logger.info("=" * 80)
        
        try:
            # Get table directory
            table_dir = self.get_table_directory(source_db, source_schema, table_name)
            
            # Step 1: Get source row count
            job.input_count = self.importer.get_row_count(table_name, source_schema)
            logger.info(f"Source table has {job.input_count} rows")
            self.status_tracker.set_input_count(source_schema, table_name, job.input_count)
            
            # Step 2: Export schema
            schema_file = table_dir / "creation.sql"
            if not self.importer.export_schema(table_name, schema_file, source_schema, target_schema=dest_schema):
                job.complete(MigrationStatus.FAILED, "Failed to export schema")
                return job
                
            # Step 3: Export data (with optimization check)
            data_file = table_dir / "data.csv"
            bcp_config = self.config['migration'].get('bcp', {})
            csv_separator = self.config['migration'].get('csv_separator', ',')
            
            # Check if we can skip data export (optimization)
            skip_export = False
            if data_file.exists():
                existing_flatfile_count = self._count_csv_rows(data_file)
                previous_input_count = self.status_tracker.get_input_count(source_schema, table_name)
                
                if existing_flatfile_count > 0 and previous_input_count == job.input_count:
                    logger.info(f"Data file exists with matching row count ({existing_flatfile_count}), skipping export")
                    job.flatfile_count = existing_flatfile_count
                    skip_export = True
            
            if not skip_export:
                # Check for large table chunking configuration
                large_table_config = self.config.get('performance', {}).get('large_table', {})
                enable_chunking = large_table_config.get('enable_chunking', False)
                chunk_threshold = large_table_config.get('chunk_threshold', 50000000)
                
                # Determine if we should use chunked export
                use_chunked = enable_chunking and job.input_count > chunk_threshold
                
                if use_chunked:
                    chunk_size = large_table_config.get('chunk_size', 5000000)
                    parallel_chunks = large_table_config.get('parallel_chunks', 2)
                    use_row_partitioning = large_table_config.get('use_row_partitioning', True)
                    
                    logger.info(f"Very large table detected ({job.input_count:,} rows), using chunked export: {chunk_size:,} rows/chunk, {parallel_chunks} parallel")
                    
                    # Create chunks directory
                    chunks_dir = table_dir / "chunks"
                    
                    # Use chunked export with known row count
                    if not self.importer.export_table_chunked(
                        table_name, chunks_dir, source_schema,
                        chunk_size=chunk_size,
                        parallel_chunks=parallel_chunks,
                        use_row_partitioning=use_row_partitioning,
                        total_rows=job.input_count,  # Pass known row count
                        field_delimiter=csv_separator,
                        text_qualifier='"',
                        batch_size=bcp_config.get('batch_size', 100000),
                        code_page='1252',
                        timeout=self.config.get('performance', {}).get('bcp_timeout', 3600)
                    ):
                        job.complete(MigrationStatus.FAILED, "Failed to export data using chunked method")
                        return job
                else:
                    # Use standard export with extended timeout for large tables
                    bcp_timeout = self.config.get('performance', {}).get('bcp_timeout', 3600)
                    if job.input_count > 100000000:  # 100M rows
                        large_table_timeout = self.config.get('performance', {}).get('large_table_timeout', 10800)
                        bcp_timeout = max(bcp_timeout, large_table_timeout)
                        logger.info(f"Large table detected ({job.input_count:,} rows), using extended timeout: {bcp_timeout}s")
                    
                    if not self.importer.export_data(
                        table_name, data_file, source_schema,
                        field_delimiter=csv_separator,  # Use global CSV separator
                        text_qualifier='"',  # Use double quotes to wrap fields
                        batch_size=bcp_config.get('batch_size', 100000),
                        code_page='1252',  # Use Windows-1252 encoding instead of UTF-8 to avoid null byte issues
                        timeout=bcp_timeout
                    ):
                        job.complete(MigrationStatus.FAILED, "Failed to export data")
                        return job
                    
                # Count rows in flatfile
                job.flatfile_count = self._count_csv_rows(data_file)
                logger.info(f"Flatfile has {job.flatfile_count} rows")
            
            self.status_tracker.set_flatfile_count(source_schema, table_name, job.flatfile_count)
            
            # Step 4: Create table in destination
            drop_if_exists = self.config['migration'].get('drop_if_exists', True)
            
            if self.exporter.table_exists(table_name, dest_schema):
                if drop_if_exists:
                    logger.info(f"Table {table_name} exists, dropping...")
                    if not self.exporter.drop_table(table_name, dest_schema):
                        logger.error(f"Failed to drop table {table_name}")
                        job.complete(MigrationStatus.FAILED, "Failed to drop existing table")
                        return job
                else:
                    logger.warning(f"Table {table_name} exists but drop_if_exists is False")
                    job.complete(MigrationStatus.SKIPPED, "Table already exists")
                    return job
                    
            # Create destination schema if it doesn't exist and create table
            drop_if_exists = self.config['migration'].get('drop_if_exists', True)
            if not self.exporter.create_schema(schema_file, schema=dest_schema, drop_if_exists=drop_if_exists):
                job.complete(MigrationStatus.FAILED, "Failed to create table")
                return job
                
            # Step 5: Import data
            copy_config = self.config['migration'].get('copy', {})
            csv_separator = self.config['migration'].get('csv_separator', ',')
            
            if not self.exporter.import_data(
                table_name, data_file, dest_schema,
                delimiter=csv_separator,  # Use global CSV separator
                quote=copy_config.get('quote', '"'),
                escape=copy_config.get('escape', '"'),
                null=copy_config.get('null', ''),
                header=copy_config.get('header', True)
            ):
                job.complete(MigrationStatus.FAILED, "Failed to import data")
                return job
                
            # Step 6: Get target row count
            job.output_count = self.exporter.get_row_count(table_name, dest_schema)
            logger.info(f"Target table has {job.output_count} rows")
            self.status_tracker.set_output_count(source_schema, table_name, job.output_count)
            
            # Verify row counts
            if job.input_count != job.output_count:
                logger.warning(
                    f"Row count mismatch: input={job.input_count}, "
                    f"flatfile={job.flatfile_count}, output={job.output_count}"
                )
                job.complete(
                    MigrationStatus.WARNING,
                    f"Row count mismatch: {job.input_count} vs {job.output_count}"
                )
            else:
                # Step 7: Apply post-creation scripts if available
                self._apply_post_creation(table_name, table_dir, dest_schema)
                
                # Step 8: Optimize (if PostgreSQL)
                if isinstance(self.exporter, PostgreSQLExporter):
                    try:
                        self.exporter.vacuum_analyze(table_name, dest_schema)
                    except Exception as e:
                        logger.warning(f"VACUUM ANALYZE failed for {table_name}, but migration completed successfully: {e}")
                
                job.complete(MigrationStatus.SUCCESS)
                logger.info(f"✓ Successfully migrated table {job.full_name}")
                
        except Exception as e:
            logger.error(f"✗ Error migrating table {job.full_name}: {e}", exc_info=True)
            job.complete(MigrationStatus.FAILED, str(e))
            
        return job
        
    def _count_csv_rows(self, csv_file: Path) -> int:
        """Count rows in CSV file (excluding header)"""
        try:
            # For large files with encoding issues, use binary mode to count newlines
            with open(csv_file, 'rb') as f:
                count = sum(1 for line in f if b'\n' in line or b'\r' in line)
                # Add one more if file doesn't end with newline
                f.seek(-1, 2)
                if f.read(1) not in [b'\n', b'\r']:
                    count += 1
                    
                # Subtract header if present
                if self.config['migration'].get('copy', {}).get('header', True):
                    count = max(0, count - 1)
                logger.debug(f"Counted {count} rows in binary mode")
                return count
                
        except Exception as e:
            logger.error(f"Error counting CSV rows: {e}")
            return 0
            
    def _apply_post_creation(self, table_name: str, table_dir: Path, schema: str):
        """Apply post-creation scripts (indexes, FKs)"""
        try:
            post_file = table_dir / "2_post_creation.sql"
            
            if not post_file.exists():
                logger.info(f"No post-creation scripts for {table_name}")
                return
                
            schema_config = self.config['migration'].get('schema', {})
            
            if not schema_config.get('create_indexes', True) and \
               not schema_config.get('create_foreign_keys', True):
                logger.info(f"Skipping post-creation scripts (disabled in config)")
                return
                
            logger.info(f"Applying post-creation scripts for {table_name}")
            
            if isinstance(self.exporter, PostgreSQLExporter):
                success = self.exporter.execute_sql_file(post_file)
                if success:
                    logger.info(f"Post-creation scripts applied successfully")
                else:
                    logger.warning(f"Some post-creation scripts failed")
                    
        except Exception as e:
            logger.error(f"Error applying post-creation scripts: {e}")
            
    def run(self):
        """Run the migration process"""
        start_time = datetime.now()
        
        logger.info("=" * 80)
        logger.info("Unified Migration Process Starting")
        logger.info("=" * 80)
        
        try:
            # Check operation mode from config
            mode = int(self.config.get('mode', 0))

            # Mode dispatch:
            # 0 = default pipeline (SQL -> PostgreSQL or configured paths)
            # 3 = SQL Server -> GeoPackage (export tables to gpkg)
            # 6 = GeoPackage -> PostgreSQL (load all .gpkg files into PostGIS)
            # 8 = CSV -> MSSQL (Import CSV file to SQL Server)
            
            if mode == 8:
                # CSV -> MSSQL: Import CSV file to SQL Server
                logger.info("="*80)
                logger.info("CSV to SQL Server Import Mode")
                logger.info("="*80)
                
                # Show BCP settings
                bcp_config = self.config['migration'].get('bcp', {})
                use_bcp = bcp_config.get('use_bcp', True)
                batch_size = bcp_config.get('batch_size', 100000)
                
                logger.info(f"Import Method: {'BCP (FAST)' if use_bcp else 'BULK INSERT'}")
                logger.info(f"Batch Size: {batch_size:,} rows/batch")
                logger.info(f"Expected Performance: {'Very Fast (recommended for 60M+ rows)' if use_bcp else 'Slower (consider BCP)'}")
                logger.info("="*80)
                
                # Initialize connections
                self.initialize_connections()
                
                # Get tables to import (table name from CSV config)
                tables = self.get_tables_to_migrate()
                
                if not tables:
                    logger.warning("No tables to import")
                    self.overall_status = MigrationStatus.FAILED
                    return
                
                # Process each table
                for table in tables:
                    logger.info(f"\nImporting CSV as table: {table}")
                    
                    # Get destination schema
                    dest_schema = self.config['destination'].get('schema', 'dbo')
                    
                    # Create migration job
                    job = TableMigrationJob(
                        self.config['destination'].get('database', 'unknown'),
                        dest_schema,
                        table
                    )
                    job.start()
                    
                    try:
                        # Get table directory
                        table_dir = self.get_table_directory(
                            'csv',
                            'public',
                            table
                        )
                        
                        # Step 1: Get source row count from CSV
                        job.input_count = self.importer.get_row_count(table)
                        logger.info(f"CSV file has {job.input_count} rows")
                        
                        # Step 2: Export schema from CSV headers
                        schema_file = table_dir / "creation.sql"
                        if not self.importer.export_schema(table, schema_file, target_schema=dest_schema):
                            job.complete(MigrationStatus.FAILED, "Failed to export schema from CSV")
                            self.jobs.append(job)
                            self.overall_status = MigrationStatus.FAILED
                            continue
                        
                        # Step 3: Get data file
                        data_file = table_dir / "data.csv"
                        if not self.importer.export_data(table, data_file):
                            job.complete(MigrationStatus.FAILED, "Failed to copy CSV file")
                            self.jobs.append(job)
                            self.overall_status = MigrationStatus.FAILED
                            continue
                        
                        job.flatfile_count = job.input_count
                        
                        # Step 4: Drop existing table if configured
                        drop_if_exists = self.config['migration'].get('drop_if_exists', True)
                        if self.exporter.table_exists(table, dest_schema):
                            if drop_if_exists:
                                logger.info(f"Dropping existing table {table}")
                                if not self.exporter.drop_table(table, dest_schema):
                                    job.complete(MigrationStatus.FAILED, "Failed to drop existing table")
                                    self.jobs.append(job)
                                    self.overall_status = MigrationStatus.FAILED
                                    continue
                            else:
                                logger.warning(f"Table {table} exists but drop_if_exists is False")
                                job.complete(MigrationStatus.SKIPPED, "Table already exists")
                                self.jobs.append(job)
                                continue
                        
                        # Step 5: Create table from schema
                        if not self.exporter.create_schema(schema_file, schema=dest_schema):
                            job.complete(MigrationStatus.FAILED, "Failed to create table")
                            self.jobs.append(job)
                            self.overall_status = MigrationStatus.FAILED
                            continue
                        
                        # Step 6: Import CSV data with BCP (optimized for large files)
                        csv_separator = self.config['migration'].get('csv_separator', ',')
                        bcp_config = self.config['migration'].get('bcp', {})
                        
                        # Get BCP settings
                        use_bcp = bcp_config.get('use_bcp', True)
                        batch_size = bcp_config.get('batch_size', 100000)
                        code_page = bcp_config.get('code_page', '65001')
                        row_delimiter = bcp_config.get('row_delimiter', '\\n')
                        timeout = bcp_config.get('timeout', 3600)
                        
                        logger.info(f"Importing CSV data using {'BCP' if use_bcp else 'BULK INSERT'} method")
                        
                        if not self.exporter.import_data(
                            table,
                            data_file,
                            schema=dest_schema,
                            delimiter=csv_separator,
                            header=self.config['source'].get('header', True),
                            use_bcp=use_bcp,
                            batch_size=batch_size,
                            code_page=code_page,
                            row_delimiter=row_delimiter,
                            timeout=timeout
                        ):
                            job.complete(MigrationStatus.FAILED, "Failed to import CSV data")
                            self.jobs.append(job)
                            self.overall_status = MigrationStatus.FAILED
                            continue
                        
                        # Step 7: Get target row count
                        job.output_count = self.exporter.get_row_count(table, dest_schema)
                        logger.info(f"Target table has {job.output_count} rows")
                        
                        # Step 8: Verify row counts
                        if job.input_count == job.output_count:
                            job.complete(MigrationStatus.SUCCESS)
                            logger.info(f"✓ Successfully imported {table} with {job.output_count} rows")
                        else:
                            logger.warning(f"Row count mismatch: CSV={job.input_count}, Table={job.output_count}")
                            job.complete(
                                MigrationStatus.WARNING,
                                f"Row count mismatch: {job.input_count} vs {job.output_count}"
                            )
                        
                        self.jobs.append(job)
                        
                    except Exception as e:
                        logger.error(f"Error importing {table}: {e}", exc_info=True)
                        job.complete(MigrationStatus.FAILED, str(e))
                        self.jobs.append(job)
                        self.overall_status = MigrationStatus.FAILED
                
                # Save status
                self.status_tracker.save()
                self.generate_report()
                return
                # Load all gpkg files into Postgres and exit
                gpkg_conf = self.config.get('gpkg', {})
                gpkg_dir = Path(gpkg_conf.get('directory', 'sql2pg_copy/input/gpkg'))
                target_schema = gpkg_conf.get('target_schema', self.config['destination'].get('schema', 'public'))
                ogr_opts = gpkg_conf.get('ogr_options', '')

                # Build gpkg options to pass through
                gpkg_opts = {
                    'use_docker_for_gdal': gpkg_conf.get('use_docker_for_gdal', False),
                    'dry_run': gpkg_conf.get('dry_run', False),
                    'parallel_jobs': gpkg_conf.get('parallel_jobs', 1)
                }

                # Use GPKGImporter helper to run ogr2ogr
                gpkg_importer = GPKGImporter(self.config)
                # Pass destination config and nested gpkg options
                dest_conf = dict(self.config['destination'])
                dest_conf['gpkg_options'] = gpkg_opts
                ok = gpkg_importer.load_all_gpkg_to_postgres(gpkg_dir, dest_conf, target_schema, ogr_opts)
                if ok:
                    logger.info("All GPKG files loaded into Postgres successfully")
                    self.overall_status = MigrationStatus.SUCCESS
                else:
                    logger.error("One or more GPKG imports failed")
                    self.overall_status = MigrationStatus.FAILED

                # Save status and finish
                self.status_tracker.save()
                return

            if mode == 3:
                # SQL Server -> GeoPackage: export each configured table to a gpkg file
                # We'll create GPKG files under gpkg directory
                gpkg_conf = self.config.get('gpkg', {})
                gpkg_dir = Path(gpkg_conf.get('directory', 'sql2pg_copy/input/gpkg'))
                gpkg_dir.mkdir(parents=True, exist_ok=True)

                # Initialize connections for exporter (we'll use GPKGExporter as destination)
                self.initialize_connections()

                tables = self.get_tables_to_migrate()
                for table in tables:
                    # export schema and data into a gpkg
                    out_file = gpkg_dir / f"{table}.gpkg"
                    logger.info(f"Exporting table {table} to GeoPackage {out_file}")
                    # use exporter (which is GPKGExporter when destination configured) or create one
                    gpkg_exporter = GPKGExporter(self.config)
                    if not gpkg_exporter.connect():
                        logger.error("Failed to connect to GeoPackage exporter")
                        self.overall_status = MigrationStatus.FAILED
                        break

                    # Create schema/ddl then import data via exporter
                    table_dir = self.get_table_directory(self.config['source'].get('database','unknown'), self.config['source'].get('schema','dbo'), table)
                    schema_file = table_dir / 'creation.sql'
                    if not self.importer.export_schema(table, schema_file, self.config['source'].get('schema','dbo'), target_schema=self.config['destination'].get('schema','public')):
                        logger.warning(f"Failed to export schema for {table}, continuing")

                    # Export data from source to intermediate CSV then import into gpkg
                    data_file = table_dir / 'data.csv'
                    if not self.importer.export_data(table, data_file, self.config['source'].get('schema','dbo')):
                        logger.error(f"Failed to export data for {table}")
                        self.overall_status = MigrationStatus.FAILED
                        gpkg_exporter.disconnect()
                        break

                    # Import into gpkg
                    if not gpkg_exporter.import_data(table, data_file, schema=None):
                        logger.error(f"Failed to import {table} into GeoPackage {out_file}")
                        self.overall_status = MigrationStatus.FAILED
                        gpkg_exporter.disconnect()
                        break

                    gpkg_exporter.disconnect()

                # Save status and finish
                self.status_tracker.save()
                return

            # Initialize connections (default pipeline and other modes will need connections)
            self.initialize_connections()
            
            # Get tables to migrate
            tables = self.get_tables_to_migrate()
            
            if not tables:
                logger.warning("No tables to migrate")
                return
                
            # Determine source schema
            source_schema = self.config['source'].get('schema', 'dbo')
            dest_schema = self.config['destination'].get('schema', 'public')
            
            # Migrate each table
            for i, table in enumerate(tables, 1):
                logger.info(f"\nMigrating table {i}/{len(tables)}: {table}")
                job = self.migrate_table(table, source_schema, dest_schema)
                self.jobs.append(job)
                
                if job.status == MigrationStatus.FAILED:
                    self.overall_status = MigrationStatus.FAILED
                elif job.status == MigrationStatus.WARNING:
                    if self.overall_status == MigrationStatus.SUCCESS:
                        self.overall_status = MigrationStatus.WARNING
                        
            # Save status
            self.status_tracker.save()
            
            # Generate summary report
            self.generate_report()
            
        except Exception as e:
            logger.error(f"Migration failed with error: {e}", exc_info=True)
            self.overall_status = MigrationStatus.FAILED
            
        finally:
            # Close connections
            self.close_connections()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("=" * 80)
            logger.info(f"Migration completed in {duration:.2f} seconds")
            logger.info(f"Overall status: {self.overall_status}")
            logger.info("=" * 80)
            
    def generate_report(self):
        """Generate migration report"""
        logger.info("\n" + "=" * 80)
        logger.info("MIGRATION SUMMARY REPORT")
        logger.info("=" * 80)
        
        success_count = sum(1 for job in self.jobs if job.status == MigrationStatus.SUCCESS)
        warning_count = sum(1 for job in self.jobs if job.status == MigrationStatus.WARNING)
        failed_count = sum(1 for job in self.jobs if job.status == MigrationStatus.FAILED)
        
        logger.info(f"\nTotal tables: {len(self.jobs)}")
        logger.info(f"  ✓ Successful: {success_count}")
        logger.info(f"  ⚠ Warnings: {warning_count}")
        logger.info(f"  ✗ Failed: {failed_count}")
        
        # Detailed results
        logger.info("\nDetailed Results:")
        logger.info("-" * 80)
        
        for job in self.jobs:
            status_symbol = "✓" if job.status == MigrationStatus.SUCCESS else \
                          "⚠" if job.status == MigrationStatus.WARNING else "✗"
                          
            logger.info(
                f"{status_symbol} {job.full_name}: {job.status} "
                f"(in:{job.input_count} flat:{job.flatfile_count} out:{job.output_count}, {job.duration:.2f}s)"
            )
            
            if job.error_message:
                logger.info(f"    Error: {job.error_message}")
                
        # Save report to file
        output_dir = Path(self.config['directories']['output'])
        report_file = output_dir / f"migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("Unified Migration Report\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Source: {self.config['source'].get('type')} - {self.config['source'].get('database')}\n")
            f.write(f"Destination: {self.config['destination'].get('type')} - {self.config['destination'].get('database')}\n\n")
            
            f.write(f"Total tables: {len(self.jobs)}\n")
            f.write(f"  Successful: {success_count}\n")
            f.write(f"  Warnings: {warning_count}\n")
            f.write(f"  Failed: {failed_count}\n\n")
            
            f.write("Detailed Results:\n")
            f.write("-" * 80 + "\n")
            
            for job in self.jobs:
                f.write(f"\nTable: {job.full_name}\n")
                f.write(f"  Status: {job.status}\n")
                f.write(f"  Input rows: {job.input_count}\n")
                f.write(f"  Flatfile rows: {job.flatfile_count}\n")
                f.write(f"  Output rows: {job.output_count}\n")
                f.write(f"  Duration: {job.duration:.2f}s\n")
                if job.error_message:
                    f.write(f"  Error: {job.error_message}\n")
                    
        logger.info(f"\nReport saved to: {report_file}")

