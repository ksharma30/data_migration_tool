"""
PostgreSQL adapter implementing Exporter interface
Wraps existing postgres_loader
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from base_interfaces import Exporter
from postgres_loader import PostgreSQLLoader

logger = logging.getLogger(__name__)


class PostgreSQLExporter(Exporter):
    """PostgreSQL exporter using existing loader"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize PostgreSQL exporter
        
        Args:
            config: Configuration dictionary with destination settings
        """
        super().__init__(config)
        self.loader = None
        
        # Get destination config
        dst = config.get('destination', config)
        self.host = dst['host']
        self.port = dst['port']
        self.database = dst['database']
        self.username = dst['username']
        self.password = dst['password']
        self.ssl = dst.get('ssl', False)
        
    def connect(self) -> bool:
        """Establish connection to PostgreSQL"""
        try:
            self.loader = PostgreSQLLoader(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password,
                ssl=self.ssl
            )
            self.loader.connect()
            self.connected = True
            logger.info("Connected to PostgreSQL")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
            
    def disconnect(self):
        """Close connection to PostgreSQL"""
        if self.loader:
            self.loader.disconnect()
        self.connected = False
        logger.info("Disconnected from PostgreSQL")
        
    def create_schema(self, schema_file: Path, **kwargs) -> bool:
        """
        Create table from DDL file
        
        Args:
            schema_file: Path to DDL file
            **kwargs: Additional options
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # First, create the schema if specified
            schema = kwargs.get('schema', 'public')
            if schema and schema != 'public':
                create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
                logger.info(f"Creating schema: {schema}")
                if not self.loader.execute_sql(create_schema_sql):
                    logger.error(f"Failed to create schema {schema}")
                    return False
            
            # Check if drop_if_exists is enabled
            drop_if_exists = kwargs.get('drop_if_exists', True)
            if drop_if_exists:
                # Extract table name from DDL file to drop if exists
                with open(schema_file, 'r', encoding='utf-8') as f:
                    ddl_content = f.read()
                    
                # Simple regex to extract table name from CREATE TABLE statement
                import re
                match = re.search(r'CREATE TABLE\s+(\w+\.)?([\w_]+)\s*\(', ddl_content, re.IGNORECASE)
                if match:
                    table_name = match.group(2)
                    drop_sql = f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE;"
                    logger.info(f"Dropping existing table: {schema}.{table_name}")
                    self.loader.execute_sql(drop_sql)
                    
            return self.loader.create_table_from_file(str(schema_file))
        except Exception as e:
            logger.error(f"Error creating schema: {e}")
            return False
            
    def import_data(self, table_name: str, data_file: Path,
                   schema: str = 'public', **kwargs) -> bool:
        """
        Import data from CSV using COPY with fallback to quoted re-export
        
        Args:
            table_name: Name of the table
            data_file: Path to CSV file
            schema: Schema name
            **kwargs: COPY options
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get expected column count for validation
            expected_columns = None
            try:
                cursor = self.loader.conn.cursor()
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.columns 
                    WHERE table_schema = %s AND table_name = %s
                """, (schema, table_name.lower()))
                expected_columns = cursor.fetchone()[0]
                cursor.close()
                logger.info(f"Expected columns for {schema}.{table_name}: {expected_columns}")
            except Exception as e:
                logger.warning(f"Could not get column count for validation: {e}")
            
            # Try direct COPY first
            success = self.loader.load_csv_with_copy(
                table_name=table_name,
                csv_file=str(data_file),
                schema=schema,
                delimiter=kwargs.get('delimiter', ','),
                quote=kwargs.get('quote', '"'),
                escape=kwargs.get('escape', '"'),
                null_string=kwargs.get('null', ''),
                header=kwargs.get('header', True),
                encoding='UTF8',
                expected_columns=expected_columns
            )
            
            if success:
                return True
            
            # FALLBACK: Try quoted re-export approach
            logger.warning(f"Direct COPY failed for {table_name}, attempting quoted re-export fallback")
            return self._import_with_quoted_fallback(table_name, schema)
            
        except Exception as e:
            logger.error(f"Error importing data: {e}")
            # Try fallback on exception too
            logger.warning(f"Attempting quoted re-export fallback due to exception")
            try:
                return self._import_with_quoted_fallback(table_name, schema)
            except:
                return False
    
    def _import_with_quoted_fallback(self, table_name: str, schema: str) -> bool:
        """
        Fallback: Re-export with quotes and use chunked import
        This is used when normal COPY fails due to data issues
        
        Args:
            table_name: Name of the table
            schema: PostgreSQL schema name
            
        Returns:
            True if successful, False otherwise
        """
        try:
            import sys
            import subprocess
            from pathlib import Path
            
            logger.info(f"FALLBACK STRATEGY: Re-exporting {table_name} with text qualifiers")
            
            # Get script directory
            script_dir = Path(__file__).parent
            
            # Run export_with_quotes.py
            export_script = script_dir / 'export_with_quotes.py'
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
            fix_types_script = script_dir / 'fix_column_types.py'
            if fix_types_script.exists():
                logger.info("Step 2/3: Fixing column types to VARCHAR...")
                subprocess.run(
                    [sys.executable, str(fix_types_script)],
                    capture_output=True,
                    timeout=300
                )
            
            # Run chunked_import_quoted.py
            import_script = script_dir / 'chunked_import_quoted.py'
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
            
    def get_row_count(self, table_name: str, schema: str = 'public') -> int:
        """Get row count for a table"""
        try:
            return self.loader.get_row_count(table_name, schema)
        except Exception as e:
            logger.error(f"Error getting row count: {e}")
            return 0
            
    def table_exists(self, table_name: str, schema: str = 'public') -> bool:
        """Check if table exists"""
        try:
            return self.loader.table_exists(table_name, schema)
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False
            
    def drop_table(self, table_name: str, schema: str = 'public') -> bool:
        """Drop a table"""
        try:
            return self.loader.drop_table(table_name, schema)
        except Exception as e:
            logger.error(f"Error dropping table: {e}")
            return False
            
    def execute_sql(self, sql: str, commit: bool = True) -> bool:
        """Execute SQL statement"""
        try:
            return self.loader.execute_sql(sql, commit)
        except Exception as e:
            logger.error(f"Error executing SQL: {e}")
            return False
            
    def execute_sql_file(self, sql_file: Path, commit: bool = True) -> bool:
        """Execute SQL from file"""
        try:
            return self.loader.execute_sql_file(str(sql_file), commit)
        except Exception as e:
            logger.error(f"Error executing SQL file: {e}")
            return False
            
    def vacuum_analyze(self, table_name: str, schema: str = 'public') -> bool:
        """Run VACUUM ANALYZE on table"""
        try:
            return self.loader.vacuum_analyze(table_name, schema)
        except Exception as e:
            logger.error(f"Error running VACUUM ANALYZE: {e}")
            return False

