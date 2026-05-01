"""
BCP Export Handler
Handles exporting data from SQL Server using BCP utility
"""

import os
import subprocess
import logging
from typing import Dict, List, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class BCPExporter:
    """Handles BCP export operations"""
    
    def __init__(self, server: str, database: str, username: Optional[str] = None,
                 password: Optional[str] = None, trusted_connection: bool = False):
        """
        Initialize BCP exporter
        
        Args:
            server: SQL Server instance
            database: Database name
            username: SQL Server username (optional if using trusted connection)
            password: SQL Server password (optional if using trusted connection)
            trusted_connection: Use Windows Authentication
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.trusted_connection = trusted_connection
        
    def build_bcp_command(self, table_name: str, output_file: str, schema: str = 'dbo',
                          field_delimiter: str = ',', row_delimiter: str = r'\n',
                          text_qualifier: str = '"', code_page: str = '65001',
                          batch_size: int = 100000) -> List[str]:
        """
        Build BCP command
        
        Args:
            table_name: Name of the table to export
            output_file: Output CSV file path
            schema: Schema name (default: dbo)
            field_delimiter: Field delimiter (default: comma)
            row_delimiter: Row delimiter (default: newline)
            text_qualifier: Text qualifier (default: double quote)
            code_page: Code page (default: 65001 for UTF-8)
            batch_size: Batch size for export
            
        Returns:
            List of command arguments
        """
        # Build fully qualified table name
        full_table_name = f"[{self.database}].[{schema}].[{table_name}]"
        
        # Base command
        cmd = [
            'bcp',
            full_table_name,
            'out',
            output_file,
            '-c',  # Character type
            '-t', field_delimiter,  # Field terminator
            '-r', row_delimiter,  # Row terminator
            '-b', str(batch_size),  # Batch size
            '-C', code_page,  # Code page
            '-q',  # Quoted identifiers
        ]
        
        # Add server
        cmd.extend(['-S', self.server])
        
        # Add authentication
        if self.trusted_connection:
            cmd.append('-T')
        else:
            if self.username:
                cmd.extend(['-U', self.username])
            if self.password:
                cmd.extend(['-P', self.password])
                
        return cmd
        
    def export_table(self, table_name: str, output_file: str, schema: str = 'dbo',
                    field_delimiter: str = ',', row_delimiter: str = r'\n',
                    text_qualifier: str = '"', code_page: str = '65001',
                    batch_size: int = 100000, timeout: int = 3600) -> bool:
        """
        Export table data using BCP
        
        Args:
            table_name: Name of the table to export
            output_file: Output CSV file path
            schema: Schema name (default: dbo)
            field_delimiter: Field delimiter
            row_delimiter: Row delimiter
            text_qualifier: Text qualifier
            code_page: Code page
            batch_size: Batch size for export
            timeout: Command timeout in seconds
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure output directory exists
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Build BCP command
            cmd = self.build_bcp_command(
                table_name, output_file, schema,
                field_delimiter, row_delimiter, text_qualifier,
                code_page, batch_size
            )
            
            logger.info(f"Exporting table {schema}.{table_name} to {output_file}")
            logger.debug(f"BCP command: {' '.join(cmd)}")
            
            # Execute BCP command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            if result.returncode == 0:
                logger.info(f"Successfully exported {schema}.{table_name}")
                if result.stdout:
                    logger.debug(f"BCP output: {result.stdout}")
                return True
            else:
                logger.error(f"Failed to export {schema}.{table_name}")
                logger.error(f"BCP return code: {result.returncode}")
                if result.stderr:
                    logger.error(f"BCP stderr: {result.stderr}")
                if result.stdout:
                    logger.error(f"BCP stdout: {result.stdout}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"BCP export timed out after {timeout} seconds for {schema}.{table_name}")
            return False
        except Exception as e:
            logger.error(f"Error exporting {schema}.{table_name}: {e}")
            return False
            
    def export_query(self, query: str, output_file: str,
                    field_delimiter: str = ',', row_delimiter: str = r'\n',
                    code_page: str = '65001', batch_size: int = 100000,
                    timeout: int = 3600) -> bool:
        """
        Export query results using BCP
        
        Args:
            query: SQL query to execute
            output_file: Output CSV file path
            field_delimiter: Field delimiter
            row_delimiter: Row delimiter
            code_page: Code page
            batch_size: Batch size for export
            timeout: Command timeout in seconds
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure output directory exists
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Base command for query
            cmd = [
                'bcp',
                query,
                'queryout',
                output_file,
                '-c',  # Character type
                '-t', field_delimiter,  # Field terminator
                '-r', row_delimiter,  # Row terminator
                '-b', str(batch_size),  # Batch size
                '-C', code_page,  # Code page
                '-d', self.database,  # Database
                '-S', self.server  # Server
            ]
            
            # Add authentication
            if self.trusted_connection:
                cmd.append('-T')
            else:
                if self.username:
                    cmd.extend(['-U', self.username])
                if self.password:
                    cmd.extend(['-P', self.password])
            
            logger.info(f"Exporting query results to {output_file}")
            logger.debug(f"BCP command: {' '.join(cmd)}")
            
            # Execute BCP command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            if result.returncode == 0:
                logger.info(f"Successfully exported query results")
                logger.debug(f"BCP output: {result.stdout}")
                return True
            else:
                logger.error(f"Failed to export query results")
                logger.error(f"BCP error: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"BCP export timed out after {timeout} seconds")
            return False
        except Exception as e:
            logger.error(f"Error exporting query results: {e}")
            return False
            
    def export_table_with_header(self, table_name: str, output_file: str, 
                                 column_list: List[str], schema: str = 'dbo',
                                 field_delimiter: str = ',', row_delimiter: str = r'\n',
                                 text_qualifier: str = '"', code_page: str = '65001', 
                                 batch_size: int = 100000, timeout: int = 3600) -> bool:
        """
        Export table data with header row using BCP queryout
        
        Args:
            table_name: Name of the table to export
            output_file: Output CSV file path
            column_list: List of column names
            schema: Schema name (default: dbo)
            field_delimiter: Field delimiter
            row_delimiter: Row delimiter
            code_page: Code page
            batch_size: Batch size for export
            timeout: Command timeout in seconds
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure output directory exists
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write header to file first with proper encoding
            with open(output_file, 'w', encoding='cp1252', errors='replace') as f:
                f.write(field_delimiter.join(column_list) + '\n')
            
            # Create query to export data (without header)
            # For each column, handle spatial data types specially
            columns_quoted = []
            for col in column_list:
                # Check if this might be a spatial column (contains 'geom' in name)
                if 'geom' in col.lower():
                    # For spatial columns, convert to WKT (Well-Known Text) format to avoid binary issues
                    quoted_col = f"CASE WHEN [{col}] IS NULL THEN NULL ELSE REPLACE(REPLACE([{col}].STAsText(), '{field_delimiter}', ' '), '\"', '\"\"') END AS [{col}]"
                else:
                    # Quote text fields to handle embedded delimiters
                    quoted_col = f"CASE WHEN [{col}] IS NULL THEN NULL WHEN CHARINDEX('{field_delimiter}', CAST([{col}] AS NVARCHAR(MAX))) > 0 OR CHARINDEX('\"', CAST([{col}] AS NVARCHAR(MAX))) > 0 THEN '{text_qualifier}' + REPLACE(CAST([{col}] AS NVARCHAR(MAX)), '{text_qualifier}', '{text_qualifier}{text_qualifier}') + '{text_qualifier}' ELSE CAST([{col}] AS NVARCHAR(MAX)) END AS [{col}]"
                columns_quoted.append(quoted_col)
            query = f"SELECT {', '.join(columns_quoted)} FROM [{schema}].[{table_name}]"
            
            # Export data to temporary file
            temp_file = output_file + '.tmp'
            
            # Base command for query
            cmd = [
                'bcp',
                query,
                'queryout',
                temp_file,
                '-c',  # Character type
                '-t', field_delimiter,  # Field terminator
                '-r', row_delimiter,  # Row terminator
                '-b', str(batch_size),  # Batch size
                '-C', code_page,  # Code page
                '-d', self.database,  # Database
                '-S', self.server  # Server
            ]
            
            # Add authentication
            if self.trusted_connection:
                cmd.append('-T')
            else:
                if self.username:
                    cmd.extend(['-U', self.username])
                if self.password:
                    cmd.extend(['-P', self.password])
            
            logger.info(f"Exporting table {schema}.{table_name} with header to {output_file}")
            logger.debug(f"BCP command: {' '.join(cmd)}")
            
            # Execute BCP command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            if result.returncode == 0:
                # Append data to file with header
                with open(output_file, 'ab') as outf:
                    with open(temp_file, 'rb') as inf:
                        outf.write(inf.read())
                
                # Remove temporary file
                os.remove(temp_file)
                
                logger.info(f"Successfully exported {schema}.{table_name} with header")
                logger.debug(f"BCP output: {result.stdout}")
                return True
            else:
                logger.error(f"Failed to export {schema}.{table_name}")
                logger.error(f"BCP error: {result.stderr}")
                # Clean up temp file if it exists
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"BCP export timed out after {timeout} seconds for {schema}.{table_name}")
            return False
        except Exception as e:
            logger.error(f"Error exporting {schema}.{table_name}: {e}")
            return False
            
    @staticmethod
    def check_bcp_available() -> bool:
        """
        Check if BCP utility is available
        
        Returns:
            True if BCP is available, False otherwise
        """
        try:
            result = subprocess.run(
                ['bcp', '-v'],
                capture_output=True,
                text=True,
                timeout=5
            )
            return result.returncode == 0 or 'BCP' in result.stdout or 'BCP' in result.stderr
        except:
            return False
            
    def export_table_chunked(self, table_name: str, output_dir: Path, schema: str = 'dbo',
                           chunk_size: int = 5000000, parallel_chunks: int = 2,
                           use_row_partitioning: bool = True, total_rows: int = None,
                           column_list: List[str] = None, **kwargs) -> bool:
        """
        Export large table in chunks to avoid timeout and memory issues
        
        Args:
            table_name: Name of the table to export
            output_dir: Directory for chunk files
            schema: Schema name
            chunk_size: Number of rows per chunk
            parallel_chunks: Number of chunks to process in parallel
            use_row_partitioning: Use ROW_NUMBER() for balanced partitioning
            total_rows: Total row count (if known, avoids re-query)
            column_list: List of columns to export (if not provided, gets from schema)
            **kwargs: BCP options
            
        Returns:
            True if all chunks exported successfully
        """
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        try:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Get total row count (use provided value if available)
            if total_rows is not None and total_rows > 0:
                logger.info(f"Using provided row count: {total_rows:,}")
            else:
                # Get total row count using BCP queryout (more reliable than schema extractor)
                logger.info("Getting table row count...")
                count_query = f"SELECT COUNT(*) FROM [{schema}].[{table_name}]"
                temp_count_file = output_dir / "temp_count.txt"
                
                try:
                    success = self.export_query(
                        count_query, str(temp_count_file),
                        field_delimiter="|", timeout=300
                    )
                    if success and temp_count_file.exists():
                        with open(temp_count_file, 'r') as f:
                            total_rows = int(f.read().strip())
                        temp_count_file.unlink()
                        logger.info(f"Table has {total_rows:,} rows")
                    else:
                        logger.error(f"Failed to get row count for {table_name}")
                        return False
                except Exception as e:
                    logger.error(f"Row count query failed: {e}")
                    return False
            total_chunks = (total_rows + chunk_size - 1) // chunk_size
            
            logger.info(f"Chunked export: {total_rows:,} rows → {total_chunks} chunks of ~{chunk_size:,} rows")
            
            # Get column list if not provided (needed for proper SELECT statements)
            if column_list is None:
                # Import here to avoid circular dependencies
                from schema_extractor import SchemaExtractor
                try:
                    extractor = SchemaExtractor(
                        connection_string=f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server};DATABASE={self.database};{'Trusted_Connection=yes;' if self.trusted_connection else f'UID={self.username};PWD={self.password};'}"
                    )
                    column_list = extractor.get_column_list(table_name, schema)
                    logger.info(f"Retrieved {len(column_list)} columns for chunked export")
                except Exception as e:
                    logger.warning(f"Could not get column list, falling back to SELECT *: {e}")
                    column_list = None
            
            # Create column selection for queries
            if column_list:
                columns_sql = ', '.join([f'[{col}]' for col in column_list])
                logger.debug(f"Using specific columns: {len(column_list)} columns")
            else:
                columns_sql = '*'
                logger.debug("Using SELECT * (column list not available)")
            
            # Define chunk export function
            def export_chunk(chunk_num: int) -> bool:
                try:
                    start_row = chunk_num * chunk_size
                    end_row = min((chunk_num + 1) * chunk_size, total_rows)
                    
                    chunk_file = output_dir / f"chunk_{chunk_num:04d}.csv"
                    
                    if use_row_partitioning:
                        # Use ROW_NUMBER() for balanced distribution with specific columns
                        query = f"""
                        SELECT {columns_sql} FROM (
                            SELECT {columns_sql}, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as rn
                            FROM [{schema}].[{table_name}]
                        ) ranked 
                        WHERE rn > {start_row} AND rn <= {end_row}
                        ORDER BY rn
                        """
                    else:
                        # Simple OFFSET approach with specific columns
                        query = f"""
                        SELECT {columns_sql} FROM [{schema}].[{table_name}]
                        ORDER BY (SELECT NULL)
                        OFFSET {start_row} ROWS
                        FETCH NEXT {min(chunk_size, end_row - start_row)} ROWS ONLY
                        """
                    
                    # Export chunk using BCP queryout
                    # Filter kwargs to only include parameters that export_query accepts
                    export_query_kwargs = {
                        k: v for k, v in kwargs.items() 
                        if k in ['row_delimiter', 'code_page', 'batch_size'] 
                    }
                    
                    # First export the chunk data to a temporary file
                    temp_file = str(chunk_file) + '.tmp'
                    success = self.export_query(
                        query, temp_file,
                        field_delimiter=kwargs.get('field_delimiter', '|'),
                        timeout=kwargs.get('timeout', 3600),
                        **export_query_kwargs
                    )
                    
                    if success and column_list:
                        # Add header to chunk file that matches PostgreSQL column order
                        field_delimiter = kwargs.get('field_delimiter', '|')
                        with open(str(chunk_file), 'w', encoding='cp1252', errors='replace') as outfile:
                            # Write header using the exact column list from schema
                            # This ensures PostgreSQL COPY maps columns correctly
                            outfile.write(field_delimiter.join(column_list) + '\n')
                            # Append data
                            with open(temp_file, 'r', encoding='cp1252', errors='replace') as infile:
                                outfile.write(infile.read())
                        
                        # Clean up temp file
                        Path(temp_file).unlink(missing_ok=True)
                    elif success:
                        # No column list, just rename temp file to chunk file
                        Path(temp_file).rename(chunk_file)
                    
                    if success:
                        logger.info(f"Chunk {chunk_num+1}/{total_chunks} exported with header: {start_row:,}-{end_row:,} rows")
                    else:
                        logger.error(f"Failed to export chunk {chunk_num+1}/{total_chunks}")
                        # Clean up temp file on failure
                        Path(temp_file).unlink(missing_ok=True)
                    
                    return success
                    
                except Exception as e:
                    logger.error(f"Error exporting chunk {chunk_num}: {e}")
                    return False
            
            # Export chunks in parallel
            failed_chunks = []
            with ThreadPoolExecutor(max_workers=parallel_chunks) as executor:
                futures = {executor.submit(export_chunk, i): i for i in range(total_chunks)}
                
                for future in as_completed(futures):
                    chunk_num = futures[future]
                    try:
                        success = future.result()
                        if not success:
                            failed_chunks.append(chunk_num)
                    except Exception as e:
                        logger.error(f"Chunk {chunk_num} failed with exception: {e}")
                        failed_chunks.append(chunk_num)
            
            if failed_chunks:
                logger.error(f"Failed chunks: {failed_chunks}")
                return False
            
            # Merge chunks into final file
            final_file = output_dir.parent / "data.csv"
            logger.info(f"Merging {total_chunks} chunks into {final_file}")
            
            with open(final_file, 'wb') as outfile:
                # Write header from first chunk
                first_chunk = output_dir / "chunk_0000.csv"
                if first_chunk.exists():
                    with open(first_chunk, 'rb') as f:
                        header_line = f.readline()
                        outfile.write(header_line)
                
                # Append data from all chunks (skip headers)
                for i in range(total_chunks):
                    chunk_file = output_dir / f"chunk_{i:04d}.csv"
                    if chunk_file.exists():
                        with open(chunk_file, 'rb') as f:
                            f.readline()  # Skip header
                            outfile.write(f.read())
                        
                        # Clean up chunk file
                        chunk_file.unlink()
            
            # Remove chunk directory
            if output_dir.exists() and not any(output_dir.iterdir()):
                output_dir.rmdir()
            
            logger.info(f"Chunked export completed: {final_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error in chunked export: {e}")
            return False

