"""
CSV Importer - reads data from CSV files
Implements Importer interface for unified migration processor
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from base_interfaces import Importer, TableSchema

logger = logging.getLogger(__name__)


class CSVImporter(Importer):
    """CSV file importer"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize CSV importer
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(config)
        self.csv_file = None
        self.csv_files = []  # List of (file_path, table_name) tuples
        self.csv_config = {}
        
    def connect(self) -> bool:
        """
        Establish connection to CSV source
        
        Returns:
            True if CSV file(s) are accessible
        """
        try:
            source_config = self.config.get('source', {})
            
            # Check if multiple CSV files specified
            if 'csv_files' in source_config:
                self.csv_files = []
                for csv_entry in source_config['csv_files']:
                    csv_path = Path(csv_entry['file'])
                    table_name = csv_entry.get('table', csv_path.stem)
                    
                    if not csv_path.exists():
                        logger.error(f"CSV file not found: {csv_path}")
                        return False
                    
                    self.csv_files.append((csv_path, table_name))
                    logger.info(f"  ✓ CSV file ready: {csv_path.name} → Table: {table_name}")
                
                if not self.csv_files:
                    logger.error("No csv_files specified in config")
                    return False
                
                logger.info(f"Connected to {len(self.csv_files)} CSV file(s)")
            
            # Single CSV file (legacy format)
            elif 'csv_file' in source_config:
                self.csv_file = Path(source_config['csv_file'])
                if not self.csv_file.exists():
                    logger.error(f"CSV file not found: {self.csv_file}")
                    return False
                logger.info(f"Connected to CSV file: {self.csv_file}")
                self.csv_files = [(self.csv_file, self.csv_file.stem)]
            
            elif 'csv_file' in self.config:
                self.csv_file = Path(self.config['csv_file'])
                if not self.csv_file.exists():
                    logger.error(f"CSV file not found: {self.csv_file}")
                    return False
                logger.info(f"Connected to CSV file: {self.csv_file}")
                self.csv_files = [(self.csv_file, self.csv_file.stem)]
            
            else:
                logger.error("No csv_file or csv_files specified in config")
                return False
            
            self.csv_config = source_config
            self.connected = True
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to CSV: {e}")
            return False
    
    def disconnect(self):
        """Close CSV connection"""
        self.connected = False
        logger.info("Disconnected from CSV")
    
    def get_tables(self) -> List[str]:
        """
        Get list of available tables (CSV files with their table names)
        
        Returns:
            List of table names
        """
        if not self.connected or not self.csv_files:
            return []
        
        try:
            # Return the mapped table names from csv_files list
            return [table_name for csv_path, table_name in self.csv_files]
            
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return []
    
    def get_csv_file_for_table(self, table_name: str) -> Optional[Path]:
        """
        Get CSV file path for a specific table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Path to CSV file or None
        """
        try:
            for csv_path, mapped_table_name in self.csv_files:
                if mapped_table_name == table_name:
                    return csv_path
            return None
        except Exception as e:
            logger.error(f"Error getting CSV file for table {table_name}: {e}")
            return None
    
    def get_schema(self, table_name: str, schema: str = None) -> TableSchema:
        """
        Get schema from CSV headers
        
        Args:
            table_name: Name of the table/CSV
            schema: Schema name (ignored for CSV)
            
        Returns:
            TableSchema object
        """
        table_schema = TableSchema('csv', 'public', table_name)
        
        try:
            import pandas as pd
            
            # Get the CSV file for this table
            csv_path = self.get_csv_file_for_table(table_name)
            if not csv_path:
                # Fallback to self.csv_file for single-file scenarios
                csv_path = self.csv_file
            
            if not csv_path or not csv_path.exists():
                logger.error(f"CSV file not found for table {table_name}")
                return table_schema
            
            # Read CSV to get column info
            delimiter = self.csv_config.get('delimiter', ',')
            quotechar = self.csv_config.get('quote_char', '"')
            
            df = pd.read_csv(csv_path, nrows=0, delimiter=delimiter, quotechar=quotechar)
            
            # Create column info from DataFrame
            for col_name in df.columns:
                table_schema.columns.append({
                    'name': col_name,
                    'type': 'VARCHAR(MAX)',  # Default to VARCHAR for CSV imports
                    'nullable': True
                })
            
            logger.info(f"Found {len(table_schema.columns)} columns in CSV: {csv_path.name}")
            return table_schema
            
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {e}")
            return table_schema
    
    def get_row_count(self, table_name: str, schema: str = None) -> int:
        """
        Get row count from CSV (excluding header)
        
        Args:
            table_name: Name of the table/CSV
            schema: Schema name (ignored)
            
        Returns:
            Number of rows
        """
        try:
            # Get the CSV file for this table
            csv_path = self.get_csv_file_for_table(table_name)
            if not csv_path:
                csv_path = self.csv_file
            
            if not csv_path or not csv_path.exists():
                logger.error(f"CSV file not found for table {table_name}")
                return 0
            
            # Count rows efficiently
            with open(csv_path, 'rb') as f:
                count = sum(1 for line in f if line.strip())
            
            # Subtract header row
            has_header = self.csv_config.get('header', True)
            if has_header and count > 0:
                count -= 1
            
            logger.info(f"CSV {csv_path.name} has {count} rows")
            return count
            
        except Exception as e:
            logger.error(f"Error counting rows for {table_name}: {e}")
            return 0
    
    def export_schema(self, table_name: str, output_file: Path, source_schema: str = None,
                     target_schema: str = None) -> bool:
        """
        Generate CREATE TABLE statement from CSV headers
        
        Args:
            table_name: Name of the table
            output_file: Output SQL file path
            source_schema: Source schema (ignored for CSV)
            target_schema: Target schema
            
        Returns:
            True if successful
        """
        try:
            # Get the CSV file for this table
            csv_path = self.get_csv_file_for_table(table_name)
            if not csv_path:
                csv_path = self.csv_file
            
            if not csv_path or not csv_path.exists():
                logger.error(f"CSV file not found for table {table_name}")
                return False
            
            import pandas as pd
            
            delimiter = self.csv_config.get('delimiter', ',')
            quotechar = self.csv_config.get('quote_char', '"')
            
            # Read CSV to get column info
            df = pd.read_csv(csv_path, nrows=0, delimiter=delimiter, quotechar=quotechar, dtype=str)
            
            # Generate CREATE TABLE script
            schema = target_schema or 'dbo'
            sql_lines = [
                f"-- Created from CSV import: {csv_path.name}",
                f"IF OBJECT_ID('[{schema}].[{table_name}]', 'U') IS NOT NULL",
                f"    DROP TABLE [{schema}].[{table_name}];",
                f"",
                f"CREATE TABLE [{schema}].[{table_name}] (",
            ]
            
            # Add columns
            for i, col_name in enumerate(df.columns):
                col_def = f"    [{col_name}] VARCHAR(MAX) NULL"
                if i < len(df.columns) - 1:
                    col_def += ","
                sql_lines.append(col_def)
            
            sql_lines.append(");")
            
            # Write to file
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(sql_lines))
            
            logger.info(f"Schema for {table_name} exported to: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting schema for {table_name}: {e}")
            return False
    
    def export_data(self, table_name: str, output_file: Path, source_schema: str = None,
                   **kwargs) -> bool:
        """
        Copy CSV data to output file (or just reference the CSV)
        
        Args:
            table_name: Name of the table
            output_file: Output file path
            source_schema: Source schema (ignored)
            **kwargs: Additional options
            
        Returns:
            True if successful
        """
        try:
            import shutil
            
            # Get the CSV file for this table
            csv_path = self.get_csv_file_for_table(table_name)
            if not csv_path:
                csv_path = self.csv_file
            
            if not csv_path or not csv_path.exists():
                logger.error(f"CSV file not found for table {table_name}")
                return False
            
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Copy CSV to output location
            shutil.copy2(csv_path, output_file)
            logger.info(f"Data for {table_name} copied to: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting data for {table_name}: {e}")
            return False
    
    def export_table_chunked(self, table_name: str, output_dir: Path, schema: str = None,
                           **kwargs) -> bool:
        """
        Export table in chunks (not implemented for CSV as source)
        
        Args:
            table_name: Name of the table
            output_dir: Output directory
            schema: Schema name
            **kwargs: Additional options
            
        Returns:
            False - chunking not supported for CSV source
        """
        logger.warning("Chunked export not supported for CSV source")
        return False
    
    def get_table_columns(self, table_name: str, schema: str = None) -> List[Dict]:
        """
        Get column information
        
        Args:
            table_name: Name of the table
            schema: Schema name (ignored)
            
        Returns:
            List of column dictionaries
        """
        try:
            table_schema = self.get_schema(table_name, schema)
            return table_schema.columns
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            return []
