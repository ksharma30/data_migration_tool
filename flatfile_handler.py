"""
Flatfile importer and exporter
Supports CSV, TSV, pipe-delimited, and custom separators
"""

import csv
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from base_interfaces import Importer, Exporter, TableSchema, FlatFileFormat

logger = logging.getLogger(__name__)


class FlatFileImporter(Importer):
    """Imports data from flatfiles"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize flatfile importer
        
        Args:
            config: Configuration dictionary with 'file_path' and 'format'
        """
        super().__init__(config)
        self.file_path = Path(config.get('file_path', ''))
        
        # Parse format configuration
        format_config = config.get('format', {})
        self.format = FlatFileFormat(
            format_type=format_config.get('type', FlatFileFormat.CSV),
            delimiter=format_config.get('delimiter'),
            quote=format_config.get('quote', '"'),
            escape=format_config.get('escape', '"'),
            header=format_config.get('header', True),
            encoding=format_config.get('encoding', 'utf-8')
        )
        
    def connect(self) -> bool:
        """Check if file exists"""
        if self.file_path.exists():
            self.connected = True
            logger.info(f"Flatfile found: {self.file_path}")
            return True
        else:
            logger.error(f"Flatfile not found: {self.file_path}")
            return False
            
    def disconnect(self):
        """No-op for flatfiles"""
        self.connected = False
        
    def get_tables(self) -> List[str]:
        """Return single table name based on filename"""
        return [self.file_path.stem]
        
    def get_schema(self, table_name: str, schema: str = None) -> TableSchema:
        """
        Get schema from flatfile header
        
        Args:
            table_name: Name of the table (ignored, uses filename)
            schema: Schema name (default: 'public')
            
        Returns:
            TableSchema object
        """
        schema_name = schema or 'public'
        table_schema = TableSchema('flatfile', schema_name, table_name)
        
        try:
            with open(self.file_path, 'r', encoding=self.format.encoding) as f:
                reader = csv.reader(
                    f,
                    delimiter=self.format.delimiter,
                    quotechar=self.format.quote,
                    escapechar=self.format.escape
                )
                
                if self.format.header:
                    headers = next(reader)
                    # Create column definitions (all as TEXT for now)
                    for col in headers:
                        table_schema.columns.append({
                            'name': col,
                            'type': 'TEXT',
                            'nullable': True
                        })
                else:
                    # If no header, peek at first row to count columns
                    first_row = next(reader)
                    for i in range(len(first_row)):
                        table_schema.columns.append({
                            'name': f'column_{i+1}',
                            'type': 'TEXT',
                            'nullable': True
                        })
                        
        except Exception as e:
            logger.error(f"Error reading flatfile schema: {e}")
            
        return table_schema
        
    def get_row_count(self, table_name: str, schema: str = None) -> int:
        """
        Get row count from flatfile
        
        Args:
            table_name: Name of the table (ignored)
            schema: Schema name (ignored)
            
        Returns:
            Number of rows (excluding header if present)
        """
        try:
            with open(self.file_path, 'r', encoding=self.format.encoding) as f:
                reader = csv.reader(
                    f,
                    delimiter=self.format.delimiter,
                    quotechar=self.format.quote,
                    escapechar=self.format.escape
                )
                
                count = sum(1 for _ in reader)
                
                # Subtract header row if present
                if self.format.header and count > 0:
                    count -= 1
                    
                return count
        except Exception as e:
            logger.error(f"Error counting rows in flatfile: {e}")
            return 0
            
    def export_data(self, table_name: str, output_path: Path,
                   schema: str = None, **kwargs) -> bool:
        """
        Copy flatfile to output location
        
        Args:
            table_name: Name of the table (ignored)
            output_path: Destination path
            schema: Schema name (ignored)
            **kwargs: Additional options
            
        Returns:
            True if successful, False otherwise
        """
        try:
            import shutil
            output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.file_path, output_path)
            logger.info(f"Flatfile copied to: {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error copying flatfile: {e}")
            return False
            
    def export_schema(self, table_name: str, output_path: Path,
                     schema: str = None, target_type: str = 'postgres', target_schema: str = 'public') -> bool:
        """
        Generate CREATE TABLE DDL from flatfile
        
        Args:
            table_name: Name of the table
            output_path: Path to output SQL file
            schema: Schema name
            target_type: Target database type
            
        Returns:
            True if successful, False otherwise
        """
        try:
            table_schema = self.get_schema(table_name, schema)
            schema_name = schema or 'public'
            
            # Generate DDL
            ddl = f"-- Generated from flatfile: {self.file_path.name}\n"
            ddl += f"CREATE TABLE {schema_name}.{table_name} (\n"
            
            column_defs = []
            for col in table_schema.columns:
                col_def = f"    {col['name']} {col['type']}"
                if not col['nullable']:
                    col_def += " NOT NULL"
                column_defs.append(col_def)
                
            ddl += ",\n".join(column_defs)
            ddl += "\n);\n"
            
            # Write to file
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(ddl)
                
            logger.info(f"Schema DDL written to: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error generating schema DDL: {e}")
            return False


class FlatFileExporter(Exporter):
    """Exports data to flatfiles"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize flatfile exporter
        
        Args:
            config: Configuration dictionary with 'output_dir' and 'format'
        """
        super().__init__(config)
        self.output_dir = Path(config.get('output_dir', 'output'))
        
        # Parse format configuration
        format_config = config.get('format', {})
        self.format = FlatFileFormat(
            format_type=format_config.get('type', FlatFileFormat.CSV),
            delimiter=format_config.get('delimiter'),
            quote=format_config.get('quote', '"'),
            escape=format_config.get('escape', '"'),
            header=format_config.get('header', True),
            encoding=format_config.get('encoding', 'utf-8')
        )
        
    def connect(self) -> bool:
        """Create output directory"""
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.connected = True
            logger.info(f"Flatfile output directory ready: {self.output_dir}")
            return True
        except Exception as e:
            logger.error(f"Failed to create output directory: {e}")
            return False
            
    def disconnect(self):
        """No-op for flatfiles"""
        self.connected = False
        
    def create_schema(self, schema_file: Path, **kwargs) -> bool:
        """
        No-op for flatfiles (schema is implicit in CSV structure)
        
        Args:
            schema_file: Path to DDL file (ignored)
            **kwargs: Additional options
            
        Returns:
            Always True
        """
        logger.info("Flatfile exporter: schema creation not required")
        return True
        
    def import_data(self, table_name: str, data_file: Path,
                   schema: str = None, **kwargs) -> bool:
        """
        Copy data file to output directory
        
        Args:
            table_name: Name of the table
            data_file: Path to source data file
            schema: Schema name (used in filename)
            **kwargs: Additional options
            
        Returns:
            True if successful, False otherwise
        """
        try:
            import shutil
            
            # Determine output filename
            if schema:
                output_file = self.output_dir / f"{schema}.{table_name}.{self.format.format_type}"
            else:
                output_file = self.output_dir / f"{table_name}.{self.format.format_type}"
                
            shutil.copy2(data_file, output_file)
            logger.info(f"Data exported to flatfile: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting to flatfile: {e}")
            return False
            
    def get_row_count(self, table_name: str, schema: str = None) -> int:
        """
        Get row count from exported flatfile
        
        Args:
            table_name: Name of the table
            schema: Schema name
            
        Returns:
            Number of rows
        """
        try:
            # Determine filename
            if schema:
                file_path = self.output_dir / f"{schema}.{table_name}.{self.format.format_type}"
            else:
                file_path = self.output_dir / f"{table_name}.{self.format.format_type}"
                
            if not file_path.exists():
                return 0
                
            with open(file_path, 'r', encoding=self.format.encoding) as f:
                reader = csv.reader(
                    f,
                    delimiter=self.format.delimiter,
                    quotechar=self.format.quote,
                    escapechar=self.format.escape
                )
                
                count = sum(1 for _ in reader)
                
                # Subtract header row if present
                if self.format.header and count > 0:
                    count -= 1
                    
                return count
                
        except Exception as e:
            logger.error(f"Error counting rows in flatfile: {e}")
            return 0
            
    def table_exists(self, table_name: str, schema: str = None) -> bool:
        """
        Check if flatfile exists
        
        Args:
            table_name: Name of the table
            schema: Schema name
            
        Returns:
            True if file exists, False otherwise
        """
        if schema:
            file_path = self.output_dir / f"{schema}.{table_name}.{self.format.format_type}"
        else:
            file_path = self.output_dir / f"{table_name}.{self.format.format_type}"
            
        return file_path.exists()
        
    def drop_table(self, table_name: str, schema: str = None) -> bool:
        """
        Delete flatfile
        
        Args:
            table_name: Name of the table
            schema: Schema name
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if schema:
                file_path = self.output_dir / f"{schema}.{table_name}.{self.format.format_type}"
            else:
                file_path = self.output_dir / f"{table_name}.{self.format.format_type}"
                
            if file_path.exists():
                file_path.unlink()
                logger.info(f"Flatfile deleted: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting flatfile: {e}")
            return False

