"""
Migration Job Processor
Orchestrates the entire migration process from SQL Server to PostgreSQL
"""

import os
import sys
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

from schema_extractor import SchemaExtractor
from bcp_exporter import BCPExporter
from postgres_loader import PostgreSQLLoader

logger = logging.getLogger(__name__)


class MigrationStatus:
    """Migration status tracking"""
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    WARNING = "WARNING"


class TableMigrationJob:
    """Represents a single table migration job"""
    
    def __init__(self, table_name: str, schema: str = 'dbo'):
        self.table_name = table_name
        self.schema = schema
        self.status = "PENDING"
        self.row_count_source = 0
        self.row_count_target = 0
        self.start_time = None
        self.end_time = None
        self.duration = 0
        self.error_message = None
        
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
            
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'table_name': self.table_name,
            'schema': self.schema,
            'status': self.status,
            'row_count_source': self.row_count_source,
            'row_count_target': self.row_count_target,
            'duration_seconds': self.duration,
            'error_message': self.error_message
        }


class MigrationProcessor:
    """Main migration processor"""
    
    def __init__(self, config: Dict):
        """
        Initialize migration processor
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.schema_extractor = None
        self.bcp_exporter = None
        self.pg_loader = None
        self.jobs = []
        self.overall_status = MigrationStatus.SUCCESS
        
        # Setup directories
        self._setup_directories()
        
    def _setup_directories(self):
        """Create necessary directories"""
        dirs = self.config['directories']
        for dir_key, dir_path in dirs.items():
            Path(dir_path).mkdir(parents=True, exist_ok=True)
            logger.info(f"Directory ready: {dir_path}")
            
    def _build_mssql_connection_string(self) -> str:
        """Build SQL Server connection string"""
        src = self.config['source']
        
        if src.get('windows_auth', False):
            conn_str = (
                f"DRIVER={{{src['driver']}}};"
                f"SERVER={src['host']},{src['port']};"
                f"DATABASE={src['database']};"
                f"Trusted_Connection=yes;"
            )
        else:
            conn_str = (
                f"DRIVER={{{src['driver']}}};"
                f"SERVER={src['host']},{src['port']};"
                f"DATABASE={src['database']};"
                f"UID={src['username']};"
                f"PWD={src['password']};"
            )
            
        return conn_str
        
    def initialize_connections(self):
        """Initialize connections to source and destination databases"""
        logger.info("Initializing database connections")
        
        # Initialize SQL Server schema extractor
        conn_str = self._build_mssql_connection_string()
        self.schema_extractor = SchemaExtractor(conn_str)
        self.schema_extractor.connect()
        
        # Initialize BCP exporter
        src = self.config['source']
        self.bcp_exporter = BCPExporter(
            server=f"{src['host']},{src['port']}",
            database=src['database'],
            username=src.get('username'),
            password=src.get('password'),
            trusted_connection=src.get('windows_auth', False)
        )
        
        # Check if BCP is available
        if not BCPExporter.check_bcp_available():
            logger.warning("BCP utility not found. Make sure it's installed and in PATH.")
        
        # Initialize PostgreSQL loader
        dst = self.config['destination']
        self.pg_loader = PostgreSQLLoader(
            host=dst['host'],
            port=dst['port'],
            database=dst['database'],
            username=dst['username'],
            password=dst['password'],
            ssl=dst.get('ssl', False)
        )
        self.pg_loader.connect()
        
        logger.info("All connections initialized successfully")
        
    def close_connections(self):
        """Close all database connections"""
        logger.info("Closing database connections")
        
        if self.schema_extractor:
            self.schema_extractor.disconnect()
            
        if self.pg_loader:
            self.pg_loader.disconnect()
            
    def get_tables_to_migrate(self) -> List[str]:
        """
        Get list of tables to migrate based on configuration
        
        Returns:
            List of table names
        """
        migration_config = self.config['migration']
        tables = migration_config.get('tables', [])
        exclude_tables = migration_config.get('exclude_tables', [])
        
        # If no tables specified, get all tables
        if not tables:
            logger.info("No tables specified in config, fetching all tables from database")
            tables = self.schema_extractor.get_all_tables()
        
        # Remove excluded tables
        tables = [t for t in tables if t not in exclude_tables]
        
        logger.info(f"Found {len(tables)} tables to migrate: {', '.join(tables)}")
        return tables
        
    def validate_tables(self, tables: List[str]) -> List[str]:
        """
        Validate that tables exist in source database
        
        Args:
            tables: List of table names
            
        Returns:
            List of valid table names
        """
        valid_tables = []
        
        for table in tables:
            if self.schema_extractor.table_exists(table):
                valid_tables.append(table)
                logger.info(f"✓ Table '{table}' exists in source database")
            else:
                logger.warning(f"✗ Table '{table}' does not exist in source database - skipping")
                
        return valid_tables
        
    def get_table_directory(self, table_name: str) -> Path:
        """
        Get directory path for table intermediate files
        
        Args:
            table_name: Name of the table
            
        Returns:
            Path to table directory
        """
        db_name = self.config['source']['database']
        intermediate_dir = Path(self.config['directories']['intermediate'])
        table_dir = intermediate_dir / db_name / table_name
        table_dir.mkdir(parents=True, exist_ok=True)
        return table_dir
        
    def migrate_table_schema(self, table_name: str, schema: str = 'dbo',
                            pg_schema: str = 'public') -> bool:
        """
        Migrate table schema
        
        Args:
            table_name: Name of the table
            schema: SQL Server schema name
            pg_schema: PostgreSQL schema name
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Migrating schema for table: {table_name}")
            
            # Get table directory
            table_dir = self.get_table_directory(table_name)
            
            # Generate CREATE TABLE DDL
            create_ddl = self.schema_extractor.generate_create_table_ddl(
                table_name, schema, pg_schema
            )
            
            # Save to file
            schema_file = table_dir / "1_schema_creation.sql"
            with open(schema_file, 'w', encoding='utf-8') as f:
                f.write(create_ddl)
            logger.info(f"Schema DDL saved to: {schema_file}")
            
            # Generate indexes DDL
            indexes_ddl = self.schema_extractor.generate_indexes_ddl(
                table_name, schema, pg_schema
            )
            
            # Generate foreign keys DDL
            fk_ddl = self.schema_extractor.generate_foreign_keys_ddl(
                table_name, schema, pg_schema
            )
            
            # Combine post-creation scripts
            post_creation_ddl = ""
            if indexes_ddl:
                post_creation_ddl += "-- Indexes\n" + indexes_ddl + "\n"
            if fk_ddl:
                post_creation_ddl += "-- Foreign Keys\n" + fk_ddl + "\n"
                
            # Save post-creation script
            if post_creation_ddl:
                post_file = table_dir / "2_post_creation.sql"
                with open(post_file, 'w', encoding='utf-8') as f:
                    f.write(post_creation_ddl)
                logger.info(f"Post-creation DDL saved to: {post_file}")
            
            # Check if table exists in PostgreSQL
            migration_config = self.config['migration']
            drop_if_exists = migration_config.get('drop_if_exists', True)
            
            if self.pg_loader.table_exists(table_name, pg_schema):
                if drop_if_exists:
                    logger.info(f"Table {table_name} exists in PostgreSQL, dropping...")
                    if not self.pg_loader.drop_table(table_name, pg_schema):
                        logger.error(f"Failed to drop table {table_name}")
                        return False
                else:
                    logger.warning(f"Table {table_name} exists in PostgreSQL but drop_if_exists is False")
                    return False
            
            # Create table in PostgreSQL
            if not self.pg_loader.create_table_from_file(str(schema_file)):
                logger.error(f"Failed to create table {table_name} in PostgreSQL")
                return False
                
            logger.info(f"Table {table_name} created successfully in PostgreSQL")
            return True
            
        except Exception as e:
            logger.error(f"Error migrating schema for {table_name}: {e}")
            return False
            
    def export_table_data(self, table_name: str, schema: str = 'dbo') -> Optional[str]:
        """
        Export table data using BCP
        
        Args:
            table_name: Name of the table
            schema: Schema name
            
        Returns:
            Path to CSV file if successful, None otherwise
        """
        try:
            logger.info(f"Exporting data for table: {table_name}")
            
            # Get table directory
            table_dir = self.get_table_directory(table_name)
            
            # Get column list for header
            columns = self.schema_extractor.get_column_list(table_name, schema)
            
            # CSV file path
            csv_file = table_dir / "data.csv"
            
            # BCP settings
            bcp_config = self.config['migration']['bcp']
            
            # Export data with header
            success = self.bcp_exporter.export_table_with_header(
                table_name=table_name,
                output_file=str(csv_file),
                column_list=columns,
                schema=schema,
                field_delimiter=bcp_config.get('field_delimiter', ','),
                row_delimiter=bcp_config.get('row_delimiter', r'\n'),
                code_page=bcp_config.get('code_page', '65001'),
                batch_size=bcp_config.get('batch_size', 100000),
                timeout=self.config['performance'].get('bcp_timeout', 3600)
            )
            
            if success:
                file_size = csv_file.stat().st_size / (1024 * 1024)  # Size in MB
                logger.info(f"Data exported successfully: {csv_file} ({file_size:.2f} MB)")
                return str(csv_file)
            else:
                logger.error(f"Failed to export data for {table_name}")
                return None
                
        except Exception as e:
            logger.error(f"Error exporting data for {table_name}: {e}")
            return None
            
    def load_table_data(self, table_name: str, csv_file: str, 
                       pg_schema: str = 'public') -> bool:
        """
        Load table data into PostgreSQL with fallback strategies
        
        Args:
            table_name: Name of the table
            csv_file: Path to CSV file
            pg_schema: PostgreSQL schema name
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Loading data into table: {table_name}")
            
            # COPY settings
            copy_config = self.config['migration']['copy']
            
            # Try direct load first
            success = self.pg_loader.load_csv_with_copy(
                table_name=table_name,
                csv_file=csv_file,
                schema=pg_schema,
                delimiter=copy_config.get('delimiter', ','),
                quote=copy_config.get('quote', '"'),
                escape=copy_config.get('escape', '"'),
                null_string=copy_config.get('null', ''),
                header=copy_config.get('header', True),
                encoding='UTF8'
            )
            
            if success:
                logger.info(f"Data loaded successfully into {table_name}")
                return True
            
            # FALLBACK: If direct load fails, try quoted re-export approach
            logger.warning(f"Direct load failed for {table_name}, attempting quoted re-export fallback")
            return self._load_with_quoted_fallback(table_name, pg_schema)
                
        except Exception as e:
            logger.error(f"Error loading data into {table_name}: {e}")
            # Try fallback on exception too
            logger.warning(f"Attempting quoted re-export fallback due to exception")
            try:
                return self._load_with_quoted_fallback(table_name, pg_schema)
            except:
                return False
    
    def _load_with_quoted_fallback(self, table_name: str, pg_schema: str) -> bool:
        """
        Fallback: Re-export with quotes and use chunked import
        This is used when normal COPY fails due to data issues
        
        Args:
            table_name: Name of the table
            pg_schema: PostgreSQL schema name
            
        Returns:
            True if successful, False otherwise
        """
        try:
            from pathlib import Path
            import subprocess
            
            logger.info(f"FALLBACK STRATEGY: Re-exporting {table_name} with text qualifiers")
            
            # Run export_with_quotes.py
            export_script = Path(__file__).parent / 'export_with_quotes.py'
            if not export_script.exists():
                logger.error("export_with_quotes.py not found - fallback unavailable")
                return False
            
            logger.info("Step 1/3: Exporting with quotes...")
            result = subprocess.run(
                [sys.executable, str(export_script)],
                capture_output=True,
                text=True,
                timeout=7200
            )
            
            if result.returncode != 0:
                logger.error(f"Quoted export failed: {result.stderr}")
                return False
            
            # Run fix_column_types.py
            fix_types_script = Path(__file__).parent / 'fix_column_types.py'
            if fix_types_script.exists():
                logger.info("Step 2/3: Fixing column types to VARCHAR...")
                subprocess.run(
                    [sys.executable, str(fix_types_script)],
                    capture_output=True,
                    timeout=300
                )
            
            # Run chunked_import_quoted.py
            import_script = Path(__file__).parent / 'chunked_import_quoted.py'
            if not import_script.exists():
                logger.error("chunked_import_quoted.py not found")
                return False
            
            logger.info("Step 3/3: Importing quoted CSV in chunks...")
            result = subprocess.run(
                [sys.executable, str(import_script)],
                capture_output=True,
                text=True,
                timeout=10800
            )
            
            if result.returncode == 0:
                logger.info(f"Fallback successful - {table_name} loaded with quoted CSV approach")
                return True
            else:
                logger.error(f"Chunked import failed: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Fallback strategy failed: {e}")
            return False
            
    def apply_post_creation_scripts(self, table_name: str, pg_schema: str = 'public') -> bool:
        """
        Apply post-creation scripts (indexes, foreign keys)
        
        Args:
            table_name: Name of the table
            pg_schema: PostgreSQL schema name
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get table directory
            table_dir = self.get_table_directory(table_name)
            post_file = table_dir / "2_post_creation.sql"
            
            if not post_file.exists():
                logger.info(f"No post-creation scripts for {table_name}")
                return True
                
            # Check if we should create indexes and foreign keys
            schema_config = self.config['migration']['schema']
            
            if not schema_config.get('create_indexes', True) and \
               not schema_config.get('create_foreign_keys', True):
                logger.info(f"Skipping post-creation scripts for {table_name} (disabled in config)")
                return True
                
            logger.info(f"Applying post-creation scripts for {table_name}")
            
            # Read and filter the script based on config
            with open(post_file, 'r', encoding='utf-8') as f:
                script = f.read()
                
            # If we need to filter, we'd do it here
            # For now, just execute the whole script
            
            success = self.pg_loader.execute_sql_file(str(post_file))
            
            if success:
                logger.info(f"Post-creation scripts applied successfully for {table_name}")
            else:
                logger.warning(f"Some post-creation scripts failed for {table_name}")
                
            return success
            
        except Exception as e:
            logger.error(f"Error applying post-creation scripts for {table_name}: {e}")
            return False
            
    def migrate_table(self, table_name: str, schema: str = 'dbo',
                     pg_schema: str = 'public') -> TableMigrationJob:
        """
        Migrate a single table
        
        Args:
            table_name: Name of the table
            schema: SQL Server schema name
            pg_schema: PostgreSQL schema name
            
        Returns:
            TableMigrationJob with results
        """
        job = TableMigrationJob(table_name, schema)
        job.start()
        
        logger.info(f"=" * 80)
        logger.info(f"Starting migration for table: {table_name}")
        logger.info(f"=" * 80)
        
        try:
            # Get source row count
            job.row_count_source = self.schema_extractor.get_row_count(table_name, schema)
            logger.info(f"Source table has {job.row_count_source} rows")
            
            # Step 1: Migrate schema
            if not self.migrate_table_schema(table_name, schema, pg_schema):
                job.complete(MigrationStatus.FAILED, "Failed to migrate schema")
                return job
                
            # Step 2: Export data
            csv_file = self.export_table_data(table_name, schema)
            if not csv_file:
                job.complete(MigrationStatus.FAILED, "Failed to export data")
                return job
                
            # Step 3: Load data
            if not self.load_table_data(table_name, csv_file, pg_schema):
                job.complete(MigrationStatus.FAILED, "Failed to load data")
                return job
                
            # Get target row count
            job.row_count_target = self.pg_loader.get_row_count(table_name, pg_schema)
            logger.info(f"Target table has {job.row_count_target} rows")
            
            # Verify row counts match
            if job.row_count_source != job.row_count_target:
                logger.warning(
                    f"Row count mismatch: source={job.row_count_source}, "
                    f"target={job.row_count_target}"
                )
                job.complete(
                    MigrationStatus.WARNING,
                    f"Row count mismatch: {job.row_count_source} vs {job.row_count_target}"
                )
            else:
                # Step 4: Apply post-creation scripts (indexes, FKs)
                # Note: Foreign keys should typically be added after all tables are migrated
                # For now, we'll try to apply them but won't fail if they don't work
                self.apply_post_creation_scripts(table_name, pg_schema)
                
                # Step 5: Vacuum analyze
                self.pg_loader.vacuum_analyze(table_name, pg_schema)
                
                job.complete(MigrationStatus.SUCCESS)
                logger.info(f"✓ Successfully migrated table {table_name}")
                
        except Exception as e:
            logger.error(f"✗ Error migrating table {table_name}: {e}")
            job.complete(MigrationStatus.FAILED, str(e))
            
        return job
        
    def run(self):
        """Run the migration process"""
        start_time = datetime.now()
        
        logger.info("=" * 80)
        logger.info("SQL Server to PostgreSQL Migration Starting")
        logger.info("=" * 80)
        
        try:
            # Initialize connections
            self.initialize_connections()
            
            # Get tables to migrate
            tables = self.get_tables_to_migrate()
            
            if not tables:
                logger.warning("No tables to migrate")
                return
                
            # Validate tables
            valid_tables = self.validate_tables(tables)
            
            if not valid_tables:
                logger.error("No valid tables found to migrate")
                return
                
            # Migrate each table
            for i, table in enumerate(valid_tables, 1):
                logger.info(f"\nMigrating table {i}/{len(valid_tables)}: {table}")
                job = self.migrate_table(table)
                self.jobs.append(job)
                
                if job.status == MigrationStatus.FAILED:
                    self.overall_status = MigrationStatus.FAILED
                elif job.status == MigrationStatus.WARNING:
                    if self.overall_status == MigrationStatus.SUCCESS:
                        self.overall_status = MigrationStatus.WARNING
                        
            # Generate summary report
            self.generate_report()
            
        except Exception as e:
            logger.error(f"Migration failed with error: {e}")
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
                f"{status_symbol} {job.table_name}: {job.status} "
                f"({job.row_count_source} rows, {job.duration:.2f}s)"
            )
            
            if job.error_message:
                logger.info(f"    Error: {job.error_message}")
                
        # Save report to file
        output_dir = Path(self.config['directories']['output'])
        report_file = output_dir / f"migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("SQL Server to PostgreSQL Migration Report\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Source: {self.config['source']['database']}\n")
            f.write(f"Target: {self.config['destination']['database']}\n\n")
            
            f.write(f"Total tables: {len(self.jobs)}\n")
            f.write(f"  Successful: {success_count}\n")
            f.write(f"  Warnings: {warning_count}\n")
            f.write(f"  Failed: {failed_count}\n\n")
            
            f.write("Detailed Results:\n")
            f.write("-" * 80 + "\n")
            
            for job in self.jobs:
                f.write(f"\nTable: {job.table_name}\n")
                f.write(f"  Status: {job.status}\n")
                f.write(f"  Source rows: {job.row_count_source}\n")
                f.write(f"  Target rows: {job.row_count_target}\n")
                f.write(f"  Duration: {job.duration:.2f}s\n")
                if job.error_message:
                    f.write(f"  Error: {job.error_message}\n")
                    
        logger.info(f"\nReport saved to: {report_file}")

