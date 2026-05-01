"""
PostgreSQL Loader
Handles loading data into PostgreSQL using COPY command
"""

import psycopg2
import logging
from typing import Dict, List, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class PostgreSQLLoader:
    """Handles PostgreSQL data loading operations"""
    
    def __init__(self, host: str, port: int, database: str, 
                 username: str, password: str, ssl: bool = False):
        """
        Initialize PostgreSQL loader
        
        Args:
            host: PostgreSQL host
            port: PostgreSQL port
            database: Database name
            username: PostgreSQL username
            password: PostgreSQL password
            ssl: Use SSL connection
        """
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.ssl = ssl
        self.conn = None
        
    def connect(self):
        """Establish connection to PostgreSQL"""
        try:
            sslmode = 'require' if self.ssl else 'prefer'
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password,
                sslmode=sslmode,
                connect_timeout=30
            )
            self.conn.autocommit = False
            logger.info("Connected to PostgreSQL successfully")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
            
    def disconnect(self):
        """Close connection to PostgreSQL"""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from PostgreSQL")
            
    def execute_sql(self, sql: str, commit: bool = True) -> bool:
        """
        Execute SQL statement
        
        Args:
            sql: SQL statement to execute
            commit: Whether to commit the transaction
            
        Returns:
            True if successful, False otherwise
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            if commit:
                self.conn.commit()
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"Error executing SQL: {e}")
            self.conn.rollback()
            return False
            
    def execute_sql_file(self, sql_file: str, commit: bool = True) -> bool:
        """
        Execute SQL from file
        
        Args:
            sql_file: Path to SQL file
            commit: Whether to commit the transaction
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql = f.read()
            return self.execute_sql(sql, commit)
        except Exception as e:
            logger.error(f"Error executing SQL file {sql_file}: {e}")
            return False
            
    def table_exists(self, table_name: str, schema: str = 'public') -> bool:
        """
        Check if a table exists in PostgreSQL
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: public)
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)",
                (schema, table_name)
            )
            exists = cursor.fetchone()[0]
            cursor.close()
            return exists
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False
            
    def drop_table(self, table_name: str, schema: str = 'public', cascade: bool = True) -> bool:
        """
        Drop a table from PostgreSQL
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: public)
            cascade: Use CASCADE option
            
        Returns:
            True if successful, False otherwise
        """
        try:
            cascade_option = "CASCADE" if cascade else ""
            sql = f"DROP TABLE IF EXISTS {schema}.{table_name} {cascade_option}"
            logger.info(f"Dropping table {schema}.{table_name}")
            return self.execute_sql(sql)
        except Exception as e:
            logger.error(f"Error dropping table {schema}.{table_name}: {e}")
            return False
            
    def create_table_from_ddl(self, ddl: str) -> bool:
        """
        Create table from DDL
        
        Args:
            ddl: CREATE TABLE DDL statement
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Creating table from DDL")
            return self.execute_sql(ddl)
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            return False
            
    def create_table_from_file(self, ddl_file: str) -> bool:
        """
        Create table from DDL file
        
        Args:
            ddl_file: Path to DDL file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Creating table from DDL file: {ddl_file}")
            return self.execute_sql_file(ddl_file)
        except Exception as e:
            logger.error(f"Error creating table from file {ddl_file}: {e}")
            return False
            
    def load_csv_with_copy(self, table_name: str, csv_file: str, schema: str = 'public',
                           delimiter: str = ',', quote: str = '"', escape: str = '"',
                           null_string: str = '', header: bool = True,
                           encoding: str = 'UTF8', expected_columns: int = None,
                           skip_cleaning: bool = True) -> bool:
        """
        Load CSV file into PostgreSQL table using COPY command
        
        Args:
            table_name: Name of the table
            csv_file: Path to CSV file
            schema: Schema name (default: public)
            delimiter: Field delimiter
            quote: Quote character
            escape: Escape character
            null_string: String representing NULL
            header: Whether CSV has header row
            encoding: File encoding
            expected_columns: Expected number of columns (for validation)
            skip_cleaning: If True, only try direct COPY (default: True)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Verify file exists
            if not Path(csv_file).exists():
                logger.error(f"CSV file not found: {csv_file}")
                return False
            
            # Build COPY command - skip all validation and cleaning
            copy_sql = f"COPY {schema}.{table_name} FROM STDIN WITH ("
            copy_sql += f"FORMAT CSV, "
            copy_sql += f"DELIMITER '{delimiter}', "
            copy_sql += f"QUOTE '{quote}', "
            copy_sql += f"ESCAPE '{escape}', "
            if header:
                copy_sql += f"HEADER TRUE, "
            if null_string:
                copy_sql += f"NULL '{null_string}', "
            else:
                copy_sql += f"NULL '', "  # Treat empty strings as NULL
            
            copy_sql += f"ENCODING '{encoding}'"
            copy_sql += ")"
            
            logger.info(f"Loading data into {schema}.{table_name} from {csv_file}")
            logger.debug(f"COPY command: {copy_sql}")
            
            # Direct COPY with multiple encoding attempts
            logger.info(f"Running direct COPY (skip_cleaning=True, no validation)")
            
            encodings_to_try = [
                ('cp1252', 'LATIN1'), 
                ('utf-8', 'UTF-8'),
                ('latin1', 'LATIN1'),
                ('iso-8859-1', 'LATIN1')
            ]
            
            for file_encoding, pg_encoding in encodings_to_try:
                try:
                    copy_sql_encoded = copy_sql.replace(f"ENCODING '{encoding}'", f"ENCODING '{pg_encoding}'")
                    
                    cursor = self.conn.cursor()
                    
                    # Stream file and strip null bytes on-the-fly
                    from io import StringIO
                    cleaned_buffer = StringIO()
                    
                    with open(csv_file, 'r', encoding=file_encoding, errors='replace') as f:
                        for line in f:
                            # Remove null bytes and other problematic characters
                            cleaned_line = line.replace('\x00', '').replace('\x1a', '')
                            cleaned_buffer.write(cleaned_line)
                    
                    # Reset buffer to beginning
                    cleaned_buffer.seek(0)
                    
                    # Execute COPY with cleaned data
                    cursor.copy_expert(copy_sql_encoded, cleaned_buffer)
                    self.conn.commit()
                    cursor.close()
                    logger.info(f"✅ COPY successful with {file_encoding} file encoding / {pg_encoding} PostgreSQL encoding")
                    return True
                    
                except Exception as e:
                    logger.warning(f"{file_encoding} encoding failed: {str(e)[:150]}")
                    self.conn.rollback()
                    
            logger.error("All encoding attempts failed")
            return False
            
        except Exception as e:
            logger.error(f"Error loading CSV into {schema}.{table_name}: {e}")
            if self.conn:
                self.conn.rollback()
            return False
            
    def get_row_count(self, table_name: str, schema: str = 'public') -> int:
        """
        Get row count for a table
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: public)
            
        Returns:
            Row count
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            logger.error(f"Error getting row count: {e}")
            return 0
            
    def create_indexes_from_ddl(self, ddl: str) -> bool:
        """
        Create indexes from DDL
        
        Args:
            ddl: CREATE INDEX DDL statements
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not ddl.strip():
                logger.info("No indexes to create")
                return True
                
            logger.info("Creating indexes")
            # Split by semicolon and execute each statement
            statements = [s.strip() for s in ddl.split(';') if s.strip()]
            for stmt in statements:
                if not self.execute_sql(stmt + ';'):
                    return False
            return True
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
            return False
            
    def create_indexes_from_file(self, ddl_file: str) -> bool:
        """
        Create indexes from DDL file
        
        Args:
            ddl_file: Path to DDL file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Creating indexes from DDL file: {ddl_file}")
            with open(ddl_file, 'r', encoding='utf-8') as f:
                ddl = f.read()
            return self.create_indexes_from_ddl(ddl)
        except Exception as e:
            logger.error(f"Error creating indexes from file {ddl_file}: {e}")
            return False
            
    def create_foreign_keys_from_ddl(self, ddl: str) -> bool:
        """
        Create foreign keys from DDL
        
        Args:
            ddl: ALTER TABLE ADD FOREIGN KEY DDL statements
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not ddl.strip():
                logger.info("No foreign keys to create")
                return True
                
            logger.info("Creating foreign keys")
            # Split by semicolon and execute each statement
            statements = [s.strip() for s in ddl.split(';') if s.strip()]
            for stmt in statements:
                if not self.execute_sql(stmt + ';'):
                    return False
            return True
        except Exception as e:
            logger.error(f"Error creating foreign keys: {e}")
            return False
            
    def create_foreign_keys_from_file(self, ddl_file: str) -> bool:
        """
        Create foreign keys from DDL file
        
        Args:
            ddl_file: Path to DDL file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Creating foreign keys from DDL file: {ddl_file}")
            with open(ddl_file, 'r', encoding='utf-8') as f:
                ddl = f.read()
            return self.create_foreign_keys_from_ddl(ddl)
        except Exception as e:
            logger.error(f"Error creating foreign keys from file {ddl_file}: {e}")
            return False
            
    def vacuum_analyze(self, table_name: str, schema: str = 'public') -> bool:
        """
        Run VACUUM ANALYZE on a table
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: public)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure any active transaction is committed first
            if not self.conn.autocommit:
                self.conn.commit()
            
            # VACUUM cannot run inside a transaction block
            old_autocommit = self.conn.autocommit
            self.conn.autocommit = True
            
            cursor = self.conn.cursor()
            
            # Execute VACUUM ANALYZE as separate statement
            vacuum_sql = f"VACUUM ANALYZE {schema}.{table_name}"
            logger.debug(f"Running: {vacuum_sql}")
            cursor.execute(vacuum_sql)
            cursor.close()
            
            # Restore original autocommit setting
            self.conn.autocommit = old_autocommit
            logger.info(f"Vacuumed and analyzed {schema}.{table_name}")
            return True
        except Exception as e:
            logger.error(f"Error running VACUUM ANALYZE on {schema}.{table_name}: {e}")
            # Make sure to restore autocommit setting even on error
            try:
                self.conn.autocommit = old_autocommit
            except:
                pass
            return False
            
    def disable_triggers(self, table_name: str, schema: str = 'public') -> bool:
        """
        Disable triggers on a table
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: public)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            sql = f"ALTER TABLE {schema}.{table_name} DISABLE TRIGGER ALL"
            logger.info(f"Disabling triggers on {schema}.{table_name}")
            return self.execute_sql(sql)
        except Exception as e:
            logger.error(f"Error disabling triggers: {e}")
            return False
            
    def enable_triggers(self, table_name: str, schema: str = 'public') -> bool:
        """
        Enable triggers on a table
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: public)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            sql = f"ALTER TABLE {schema}.{table_name} ENABLE TRIGGER ALL"
            logger.info(f"Enabling triggers on {schema}.{table_name}")
            return self.execute_sql(sql)
        except Exception as e:
            logger.error(f"Error enabling triggers: {e}")
            return False

    def _validate_and_fix_csv_structure(self, csv_file: str, delimiter: str, 
                                      expected_columns: int, has_header: bool) -> bool:
        """
        Validate and fix CSV structure to match expected column count
        
        Args:
            csv_file: Path to CSV file
            delimiter: Field delimiter 
            expected_columns: Expected number of columns
            has_header: Whether CSV should have header
            
        Returns:
            True if validation/fix successful
        """
        try:
            # Read first few lines to analyze structure
            with open(csv_file, 'r', encoding='cp1252', errors='replace') as f:
                first_line = f.readline().strip()
                if not first_line:
                    logger.error("CSV file is empty")
                    return False
                    
                fields = first_line.split(delimiter)
                actual_columns = len(fields)
                
                logger.info(f"CSV structure check: {actual_columns} columns found, {expected_columns} expected")
                
                # Check if first line looks like data (not header)
                looks_like_data = any(field.strip().isdigit() for field in fields[:3]) if fields else False
                
                if looks_like_data and has_header:
                    logger.warning("First line appears to be data but header=True - CSV may be missing header")
                
                if actual_columns == expected_columns:
                    logger.info("✓ Column count matches expected")
                    return True
                elif actual_columns > expected_columns:
                    logger.warning(f"⚠️  CSV has {actual_columns - expected_columns} extra columns, will truncate")
                    return self._fix_csv_column_count(csv_file, delimiter, expected_columns)
                else:
                    logger.error(f"❌ CSV has {expected_columns - actual_columns} missing columns")
                    return False
                    
        except Exception as e:
            logger.error(f"Error validating CSV structure: {e}")
            return False
    
    def _fix_csv_column_count(self, csv_file: str, delimiter: str, target_columns: int) -> bool:
        """
        Fix CSV file to have the correct number of columns by truncating extra columns
        
        Args:
            csv_file: Path to CSV file
            delimiter: Field delimiter
            target_columns: Target number of columns
            
        Returns:
            True if fix successful
        """
        try:
            fixed_file = csv_file + ".fixed"
            lines_processed = 0
            
            logger.info(f"Fixing CSV column count: truncating to {target_columns} columns")
            
            with open(csv_file, 'r', encoding='cp1252', errors='replace') as infile, \
                 open(fixed_file, 'w', encoding='cp1252', errors='replace') as outfile:
                
                for line in infile:
                    line = line.strip()
                    if line:
                        fields = line.split(delimiter)
                        # Keep only the first target_columns
                        fixed_fields = fields[:target_columns]
                        fixed_line = delimiter.join(fixed_fields)
                        outfile.write(fixed_line + '\n')
                        
                        lines_processed += 1
                        if lines_processed % 1000000 == 0:
                            logger.info(f"Fixed {lines_processed:,} lines...")
            
            # Replace original file with fixed file
            import shutil
            shutil.move(fixed_file, csv_file)
            
            logger.info(f"CSV column count fix completed: {lines_processed:,} lines processed")
            return True
            
        except Exception as e:
            logger.error(f"Error fixing CSV column count: {e}")
            # Clean up partial fixed file
            Path(csv_file + ".fixed").unlink(missing_ok=True)
            return False

    def _check_csv_has_headers(self, csv_file: str, delimiter: str) -> bool:
        """
        Check if CSV file has proper column headers vs data in first row
        
        Args:
            csv_file: Path to CSV file
            delimiter: Field delimiter
            
        Returns:
            True if first row appears to be headers
        """
        try:
            with open(csv_file, 'r', encoding='cp1252', errors='replace') as f:
                first_line = f.readline().strip()
                if not first_line:
                    return False
                    
            fields = first_line.split(delimiter)
            
            # Count header-like vs data-like fields
            header_indicators = 0
            data_indicators = 0
            
            for field in fields[:10]:  # Check first 10 fields
                field = field.strip()
                if not field:
                    continue
                    
                # Header indicators: contains letters but not purely alphabetic
                if any(c.isalpha() for c in field):
                    if not field.replace('_', '').replace(' ', '').isalpha():
                        header_indicators += 1
                
                # Data indicators: purely numeric, empty, or very long numeric IDs
                if field.isdigit() and len(field) > 10:
                    data_indicators += 2  # Long numeric IDs are strong data indicators
                elif field.isdigit():
                    data_indicators += 1
                    
            logger.debug(f"Header check - Header indicators: {header_indicators}, Data indicators: {data_indicators}")
            return header_indicators > data_indicators
            
        except Exception as e:
            logger.warning(f"Could not check CSV headers: {e}")
            return True  # Default to assuming headers exist
    
    def _get_table_columns(self, table_name: str, schema: str) -> List[str]:
        """
        Get column names for a table in correct order
        
        Args:
            table_name: Name of the table
            schema: Schema name
            
        Returns:
            List of column names in order
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table_name.lower()))
            
            columns = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            logger.debug(f"Retrieved {len(columns)} column names for {schema}.{table_name}")
            return columns
            
        except Exception as e:
            logger.error(f"Error getting table columns: {e}")
            return []

