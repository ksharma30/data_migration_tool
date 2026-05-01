"""
SQL Server Exporter - writes data to SQL Server
Implements Exporter interface for unified migration processor
"""

import logging
import subprocess
import re
from pathlib import Path
from typing import Dict, List, Optional, Any
import pyodbc
from base_interfaces import Exporter

logger = logging.getLogger(__name__)


class MSSQLExporter(Exporter):
    """SQL Server exporter for importing data"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize MSSQL exporter
        
        Args:
            config: Configuration dictionary with destination settings
        """
        super().__init__(config)
        self.conn = None
        self.cursor = None
        
        # Get destination config (or source if being used as fallback)
        dest = config.get('destination', config.get('source', {}))
        
        # Build connection string
        if dest.get('windows_auth', True):
            self.conn_str = (
                f"Driver={{{dest.get('driver', 'ODBC Driver 17 for SQL Server')}}};"
                f"Server={dest['host']},{dest.get('port', 1433)};"
                f"Database={dest['database']};"
                f"Trusted_Connection=yes;"
            )
        else:
            self.conn_str = (
                f"Driver={{{dest.get('driver', 'ODBC Driver 17 for SQL Server')}}};"
                f"Server={dest['host']},{dest.get('port', 1433)};"
                f"Database={dest['database']};"
                f"UID={dest.get('username', '')};"
                f"PWD={dest.get('password', '')};"
            )
        
        self.server = f"{dest['host']},{dest.get('port', 1433)}"
        self.database = dest['database']
        self.username = dest.get('username')
        self.password = dest.get('password')
        self.windows_auth = dest.get('windows_auth', True)
        self.driver = dest.get('driver', 'ODBC Driver 17 for SQL Server')
    
    def connect(self) -> bool:
        """
        Establish connection to SQL Server
        
        Returns:
            True if successful
        """
        try:
            logger.info(f"Connecting to SQL Server: {self.server}, DB: {self.database}")
            self.conn = pyodbc.connect(self.conn_str, timeout=30)
            self.cursor = self.conn.cursor()
            logger.info("Connected to SQL Server successfully")
            self.connected = True
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {e}")
            self.connected = False
            return False
    
    def disconnect(self):
        """Close connection to SQL Server"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            logger.info("Disconnected from SQL Server")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
        finally:
            self.connected = False
    
    def create_schema(self, schema_file: Path, schema: str = 'dbo', drop_if_exists: bool = False) -> bool:
        """
        Create table from SQL script
        
        Args:
            schema_file: Path to SQL file with CREATE TABLE statement
            schema: Target schema
            drop_if_exists: Drop table if it exists
            
        Returns:
            True if successful
        """
        try:
            if not self.connected:
                logger.error("Not connected to SQL Server")
                return False
            
            if not schema_file.exists():
                logger.error(f"Schema file not found: {schema_file}")
                return False
            
            # Read SQL file
            with open(schema_file, 'r', encoding='utf-8') as f:
                sql = f.read()
            
            # Execute script
            logger.info(f"Executing schema script: {schema_file}")
            
            # Split on SQL batch separators where GO appears alone on a line.
            batches = []
            current_batch = []
            for line in sql.splitlines():
                if re.match(r"^\s*GO\s*$", line, re.IGNORECASE):
                    batch_text = "\n".join(current_batch).strip()
                    if batch_text:
                        batches.append(batch_text)
                    current_batch = []
                else:
                    current_batch.append(line)

            trailing_batch = "\n".join(current_batch).strip()
            if trailing_batch:
                batches.append(trailing_batch)

            if not batches:
                logger.error(f"No executable SQL statements found in {schema_file}")
                return False

            for i, batch in enumerate(batches):
                logger.debug(f"Executing batch {i + 1}/{len(batches)}: {batch[:100]}...")
                self.cursor.execute(batch)
            
            self.conn.commit()
            logger.info("Schema created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating schema: {e}")
            if self.conn:
                self.conn.rollback()
            return False
    
    def drop_table(self, table_name: str, schema: str = 'dbo') -> bool:
        """
        Drop table if it exists
        
        Args:
            table_name: Name of the table
            schema: Schema name
            
        Returns:
            True if successful
        """
        try:
            if not self.connected:
                logger.error("Not connected to SQL Server")
                return False
            
            sql = f"IF OBJECT_ID('[{schema}].[{table_name}]', 'U') IS NOT NULL DROP TABLE [{schema}].[{table_name}]"
            logger.info(f"Dropping table: {schema}.{table_name}")
            self.cursor.execute(sql)
            self.conn.commit()
            logger.info(f"Table dropped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error dropping table: {e}")
            return False
    
    def table_exists(self, table_name: str, schema: str = 'dbo') -> bool:
        """
        Check if table exists
        
        Args:
            table_name: Name of the table
            schema: Schema name
            
        Returns:
            True if table exists
        """
        try:
            if not self.connected:
                return False
            
            sql = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'"
            self.cursor.execute(sql)
            result = self.cursor.fetchone()
            exists = result[0] > 0 if result else False
            logger.info(f"Table {schema}.{table_name} exists: {exists}")
            return exists
            
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False
    
    def import_data(self, table_name: str, data_file: Path, schema: str = 'dbo',
                   delimiter: str = ',', quote: str = '"', escape: str = '"',
                   null: str = '', header: bool = True, use_bcp: bool = True,
                   **kwargs) -> bool:
        """
        Import data from CSV using BCP (preferred) or BULK INSERT
        
        Args:
            table_name: Name of the table
            data_file: Path to CSV file
            schema: Schema name
            delimiter: CSV delimiter
            quote: Quote character
            escape: Escape character
            null: Null string representation
            header: Has header row
            use_bcp: Use BCP for import (default: True, faster)
            **kwargs: Additional options
            
        Returns:
            True if successful
        """
        try:
            # PRE-IMPORT VALIDATION
            if not self.connected:
                logger.error("✗ Not connected to SQL Server")
                return False
            
            if not data_file.exists():
                logger.error(f"✗ Data file not found: {data_file}")
                return False
            
            # Verify SQL Server connection is still active
            try:
                self.cursor.execute("SELECT 1")
                self.cursor.fetchone()
                logger.info(f"✓ SQL Server connection verified")
            except Exception as e:
                logger.error(f"✗ SQL Server connection check failed: {e}")
                logger.info("  Attempting to reconnect...")
                if not self.connect():
                    logger.error("✗ Failed to reconnect to SQL Server")
                    return False
            
            # Verify table exists
            try:
                if not self.table_exists(table_name, schema):
                    logger.error(f"✗ Target table does not exist: {schema}.{table_name}")
                    return False
                logger.info(f"✓ Target table verified: {schema}.{table_name}")
            except Exception as e:
                logger.error(f"✗ Could not verify target table: {e}")
                return False
            
            # Try BCP first if requested
            if use_bcp:
                logger.info("Attempting import using BCP (faster method)...")
                result = self.import_data_with_bcp(
                    table_name, data_file, schema, delimiter, **kwargs
                )
                if result:
                    return True
                else:
                    logger.warning("BCP import failed, attempting fallback with BULK INSERT...")
                    return self.import_data_with_bulk_insert(
                        table_name, data_file, schema, delimiter, header, **kwargs
                    )
            else:
                logger.info("Using BULK INSERT import method...")
                return self.import_data_with_bulk_insert(
                    table_name, data_file, schema, delimiter, header, **kwargs
                )
            
        except Exception as e:
            logger.error(f"✗ Error during import preparation: {e}")
            import traceback
            logger.error(f"  Traceback: {traceback.format_exc()}")
            if self.conn:
                try:
                    self.conn.rollback()
                except:
                    pass
            return False
    
    def import_data_with_bulk_insert(self, table_name: str, data_file: Path, 
                                     schema: str = 'dbo', delimiter: str = ',',
                                     header: bool = True, **kwargs) -> bool:
        """
        Import data using T-SQL BULK INSERT (fallback method)
        
        Args:
            table_name: Name of the table
            data_file: Path to CSV file
            schema: Schema name
            delimiter: CSV delimiter
            header: Has header row
            **kwargs: Additional options
            
        Returns:
            True if successful
        """
        try:
            if not self.connected:
                logger.error("Not connected to SQL Server")
                return False
            
            if not data_file.exists():
                logger.error(f"Data file not found: {data_file}")
                return False
            
            logger.info(f"Importing data from {data_file}")
            
            data_file_abs = data_file.absolute()
            full_table_name = f"[{schema}].[{table_name}]"
            
            # Build BULK INSERT statement
            bulk_sql = f"""
            BULK INSERT {full_table_name}
            FROM '{data_file_abs}'
            WITH (
                FIRSTROW = {2 if header else 1},
                FIELDTERMINATOR = '{delimiter}',
                ROWTERMINATOR = '\\n',
                CODEPAGE = 'UTF8',
                TABLOCK
            )
            """
            
            logger.info(f"Executing BULK INSERT for {table_name}")
            self.cursor.execute(bulk_sql)
            self.conn.commit()
            
            # Get row count
            row_count_sql = f"SELECT COUNT(*) FROM {full_table_name}"
            self.cursor.execute(row_count_sql)
            result = self.cursor.fetchone()
            row_count = result[0] if result else 0
            
            logger.info(f"Successfully imported {row_count} rows into {full_table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error with BULK INSERT: {e}")
            if self.conn:
                self.conn.rollback()
            return False
    
    def import_data_with_bcp(self, table_name: str, data_file: Path, schema: str = 'dbo',
                            delimiter: str = ',', batch_size: int = 10000, **kwargs) -> bool:
        """
        Import data using BCP utility (faster for large files)
        
        Recommended method - BCP is optimized for bulk operations
        
        Args:
            table_name: Name of the table
            data_file: Path to CSV file
            schema: Schema name
            delimiter: CSV delimiter
            batch_size: Batch size for import
            **kwargs: Additional options (code_page, row_delimiter, etc.)
            
        Returns:
            True if successful
        """
        try:
            # PRE-FLIGHT CHECKS
            if not data_file.exists():
                logger.error(f"✗ Data file not found: {data_file}")
                return False
            
            file_size_mb = data_file.stat().st_size / (1024 * 1024)
            logger.info(f"  CSV file size: {file_size_mb:.2f} MB")
            
            # Verify file is readable
            try:
                with open(data_file, 'rb') as f:
                    first_byte = f.read(1)
                    if not first_byte:
                        logger.error(f"✗ CSV file is empty: {data_file}")
                        return False
            except Exception as e:
                logger.error(f"✗ Cannot read CSV file: {e}")
                return False
            
            full_table_name = f"{self.database}.{schema}.{table_name}"
            data_file_abs = str(data_file.absolute())
            
            # Get optional parameters
            code_page = kwargs.get('code_page', '65001')  # UTF-8
            skip_header = kwargs.get('header', True)
            
            # Build BCP import command
            # For BCP: use character mode (-c) with UTF-8 code page
            # Row terminator: \n (newline) - BCP will interpret this correctly
            bcp_cmd = [
                'bcp',
                full_table_name,
                'in',  # Import mode (not export)
                data_file_abs,
                '-c',  # Character data type (NOT -w which is UTF-16)
                '-t', delimiter,  # Field terminator
                '-r', '\n',  # Row terminator (actual newline, not escaped string)
                '-b', str(batch_size),  # Batch size (rows per batch)
                '-C', 'UTF8',  # UTF-8 code page for proper encoding
                '-S', self.server,  # Server
                '-q',  # Quoted identifiers
                '-e', data_file_abs + '.errors',  # Error log file
            ]
            
            # Add authentication
            if self.windows_auth:
                bcp_cmd.append('-T')  # Trusted connection
            else:
                if self.username:
                    bcp_cmd.extend(['-U', self.username])
                if self.password:
                    bcp_cmd.extend(['-P', self.password])
            
            # Skip first row if header is present
            if skip_header:
                bcp_cmd.extend(['-F', '2'])  # Start from row 2
            
            logger.info(f"Importing data via BCP for {table_name}")
            logger.info(f"  Mode: Character (-c) with UTF-8 code page")
            logger.info(f"  Batch size: {batch_size} rows")
            logger.debug(f"BCP command: {' '.join(bcp_cmd)}")
            
            # Execute BCP command with timeout
            timeout = kwargs.get('timeout', 3600)
            result = subprocess.run(
                bcp_cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            # Parse BCP output for status
            if result.stdout:
                logger.debug(f"BCP output:\n{result.stdout}")
            
            if result.returncode == 0:
                logger.info(f"✓ BCP import completed successfully for {table_name}")
                
                # Get row count to confirm
                try:
                    row_count = self.get_row_count(table_name, schema)
                    logger.info(f"  Verified {row_count} rows loaded into {full_table_name}")
                except Exception as count_error:
                    logger.warning(f"  Could not verify row count: {count_error}")
                
                return True
            else:
                logger.error(f"✗ BCP import failed with return code {result.returncode}")
                
                # Log stderr and stdout
                if result.stderr:
                    logger.error(f"  STDERR: {result.stderr}")
                if result.stdout:
                    logger.error(f"  STDOUT (first 2000 chars): {result.stdout[:2000]}")
                
                # Parse and log error file
                error_file = Path(data_file_abs + '.errors')
                if error_file.exists():
                    try:
                        with open(error_file, 'r', encoding='utf-8', errors='ignore') as f:
                            error_content = f.read()
                            if error_content.strip():
                                # Show first 2000 chars of error file
                                logger.error(f"  BCP Error file (first 2000 chars):\n{error_content[:2000]}")
                                
                                # Parse for specific error patterns
                                if "Incorrect syntax" in error_content:
                                    logger.error("  --> SQL syntax error: check delimiter, encoding, or field specification")
                                elif "conversion error" in error_content.lower():
                                    logger.error("  --> Data conversion error: check data types and column definitions")
                                elif "not found" in error_content.lower():
                                    logger.error("  --> Table or column not found: verify schema and table name")
                            else:
                                logger.warning(f"  BCP error file is empty")
                    except Exception as e:
                        logger.error(f"  Could not read BCP error file: {e}")
                else:
                    logger.warning(f"  No error file generated at: {error_file}")
                
                return False
            
        except subprocess.TimeoutExpired:
            logger.error(f"✗ BCP import timed out after {kwargs.get('timeout', 3600)} seconds")
            logger.error("  Consider increasing timeout or reducing batch size for large files")
            return False
        except Exception as e:
            logger.error(f"✗ Unexpected error with BCP import: {e}")
            import traceback
            logger.error(f"  Traceback: {traceback.format_exc()}")
            return False
    
    def get_row_count(self, table_name: str, schema: str = 'dbo') -> int:
        """
        Get row count from table
        
        Args:
            table_name: Name of the table
            schema: Schema name
            
        Returns:
            Row count
        """
        try:
            if not self.connected:
                return 0
            
            sql = f"SELECT COUNT(*) FROM [{schema}].[{table_name}]"
            self.cursor.execute(sql)
            result = self.cursor.fetchone()
            count = result[0] if result else 0
            logger.info(f"Table {schema}.{table_name} has {count} rows")
            return count
            
        except Exception as e:
            logger.error(f"Error getting row count: {e}")
            return 0
    
    def vacuum_analyze(self, table_name: str, schema: str = 'dbo'):
        """
        Not applicable for SQL Server
        
        Args:
            table_name: Name of the table
            schema: Schema name
        """
        # SQL Server uses different optimization commands
        try:
            if not self.connected:
                return
            
            full_table_name = f"[{schema}].[{table_name}]"
            sql = f"UPDATE STATISTICS {full_table_name}"
            logger.info(f"Running UPDATE STATISTICS for {table_name}")
            self.cursor.execute(sql)
            self.conn.commit()
            logger.info("UPDATE STATISTICS completed")
            
        except Exception as e:
            logger.warning(f"Error updating statistics: {e}")
