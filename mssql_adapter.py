"""
SQL Server adapter implementing Importer interface
Wraps existing schema_extractor and bcp_exporter
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from base_interfaces import Importer, TableSchema
from schema_extractor import SchemaExtractor
from bcp_exporter import BCPExporter

logger = logging.getLogger(__name__)


class MSSQLImporter(Importer):
    """SQL Server importer using existing components"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize MSSQL importer
        
        Args:
            config: Configuration dictionary with source settings
        """
        super().__init__(config)
        self.extractor = None
        self.bcp_exporter = None
        
        # Build connection string
        src = config.get('source', config)
        if src.get('windows_auth', False):
            self.conn_str = (
                f"DRIVER={{{src.get('driver', 'ODBC Driver 17 for SQL Server')}}};"
                f"SERVER={src['host']},{src['port']};"
                f"DATABASE={src['database']};"
                f"Trusted_Connection=yes;"
            )
        else:
            self.conn_str = (
                f"DRIVER={{{src.get('driver', 'ODBC Driver 17 for SQL Server')}}};"
                f"SERVER={src['host']},{src['port']};"
                f"DATABASE={src['database']};"
                f"UID={src.get('username', '')};"
                f"PWD={src.get('password', '')};"
            )
        
        # BCP settings
        self.server = f"{src['host']},{src['port']}"
        self.database = src['database']
        self.username = src.get('username')
        self.password = src.get('password')
        self.trusted_connection = src.get('windows_auth', False)
        
    def connect(self) -> bool:
        """Establish connection to SQL Server"""
        try:
            self.extractor = SchemaExtractor(self.conn_str)
            self.extractor.connect()
            
            self.bcp_exporter = BCPExporter(
                server=self.server,
                database=self.database,
                username=self.username,
                password=self.password,
                trusted_connection=self.trusted_connection
            )
            
            self.connected = True
            logger.info("Connected to SQL Server")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {e}")
            return False
            
    def disconnect(self):
        """Close connection to SQL Server"""
        if self.extractor:
            self.extractor.disconnect()
        self.connected = False
        logger.info("Disconnected from SQL Server")
        
    def get_tables(self) -> List[str]:
        """Get list of tables"""
        try:
            return self.extractor.get_all_tables()
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return []
            
    def get_schema(self, table_name: str, schema: str = 'dbo') -> TableSchema:
        """
        Get schema information for a table
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: dbo)
            
        Returns:
            TableSchema object
        """
        table_schema = TableSchema(self.database, schema, table_name)
        
        try:
            # Get column information
            columns = self.extractor.get_columns(table_name, schema)
            table_schema.columns = columns
            
            # Get primary keys
            pks = self.extractor.get_primary_keys(table_name, schema)
            table_schema.primary_keys = pks
            
            # Get indexes
            indexes = self.extractor.get_indexes(table_name, schema)
            table_schema.indexes = indexes
            
            # Get foreign keys
            fks = self.extractor.get_foreign_keys(table_name, schema)
            table_schema.foreign_keys = fks
            
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {e}")
            
        return table_schema
        
    def get_row_count(self, table_name: str, schema: str = 'dbo') -> int:
        """Get row count for a table"""
        try:
            return self.extractor.get_row_count(table_name, schema)
        except Exception as e:
            logger.error(f"Error getting row count: {e}")
            return 0
            
    def export_data(self, table_name: str, output_path: Path,
                   schema: str = 'dbo', **kwargs) -> bool:
        """
        Export table data using BCP
        
        Args:
            table_name: Name of the table
            output_path: Path to output file
            schema: Schema name
            **kwargs: BCP options (field_delimiter, batch_size, etc.)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get column list for header
            columns = self.extractor.get_column_list(table_name, schema)
            
            # Export with BCP
            success = self.bcp_exporter.export_table_with_header(
                table_name=table_name,
                output_file=str(output_path),
                column_list=columns,
                schema=schema,
                field_delimiter=kwargs.get('field_delimiter', ','),
                row_delimiter=kwargs.get('row_delimiter', r'\n'),
                text_qualifier=kwargs.get('text_qualifier', '"'),
                code_page=kwargs.get('code_page', '65001'),
                batch_size=kwargs.get('batch_size', 100000),
                timeout=kwargs.get('timeout', 3600)
            )
            
            if success:
                logger.info(f"Data exported to: {output_path}")
            return success
            
        except Exception as e:
            logger.error(f"Error exporting data: {e}")
            return False
            
    def export_table_chunked(self, table_name: str, output_dir: Path,
                           schema: str = 'dbo', **kwargs) -> bool:
        """
        Export large table using chunked approach
        
        Args:
            table_name: Name of the table
            output_dir: Directory for chunk files 
            schema: Schema name
            **kwargs: Chunking and BCP options (including total_rows)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get column list to ensure consistent export structure
            column_list = self.extractor.get_column_list(table_name, schema)
            
            return self.bcp_exporter.export_table_chunked(
                table_name=table_name,
                output_dir=output_dir,
                schema=schema,
                column_list=column_list,
                **kwargs
            )
        except Exception as e:
            logger.error(f"Error in chunked export: {e}")
            return False
            
    def export_schema(self, table_name: str, output_path: Path,
                     schema: str = 'dbo', target_type: str = 'postgres', target_schema: str = 'public') -> bool:
        """
        Export table schema as DDL
        
        Args:
            table_name: Name of the table
            output_path: Path to output SQL file
            schema: Schema name
            target_type: Target database type
            target_schema: Target schema for DDL
            
        Returns:
            True if successful, False otherwise
        """
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Generate CREATE TABLE DDL with target schema
            create_ddl = self.extractor.generate_create_table_ddl(
                table_name, schema, target_schema
            )
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(create_ddl)
                
            logger.info(f"Schema DDL written to: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting schema: {e}")
            return False

