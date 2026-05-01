"""
Base interfaces for importers and exporters
Supports: sqlserver, postgres, flatfile, gpkg
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class TableSchema:
    """Represents table schema information"""
    
    def __init__(self, database: str, schema: str, table: str):
        self.database = database
        self.schema = schema
        self.table = table
        self.columns = []
        self.primary_keys = []
        self.indexes = []
        self.foreign_keys = []
        
    @property
    def full_name(self) -> str:
        """Get fully qualified table name"""
        return f"{self.database}.{self.schema}.{self.table}"
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'database': self.database,
            'schema': self.schema,
            'table': self.table,
            'columns': self.columns,
            'primary_keys': self.primary_keys,
            'indexes': self.indexes,
            'foreign_keys': self.foreign_keys
        }


class Importer(ABC):
    """Base class for data importers"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize importer
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.connected = False
        
    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to data source
        
        Returns:
            True if successful, False otherwise
        """
        pass
        
    @abstractmethod
    def disconnect(self):
        """Close connection to data source"""
        pass
        
    @abstractmethod
    def get_tables(self) -> List[str]:
        """
        Get list of available tables
        
        Returns:
            List of table names
        """
        pass
        
    @abstractmethod
    def get_schema(self, table_name: str, schema: str = None) -> TableSchema:
        """
        Get schema information for a table
        
        Args:
            table_name: Name of the table
            schema: Schema name (optional)
            
        Returns:
            TableSchema object
        """
        pass
        
    @abstractmethod
    def get_row_count(self, table_name: str, schema: str = None) -> int:
        """
        Get row count for a table
        
        Args:
            table_name: Name of the table
            schema: Schema name (optional)
            
        Returns:
            Number of rows
        """
        pass
        
    @abstractmethod
    def export_data(self, table_name: str, output_path: Path, 
                   schema: str = None, **kwargs) -> bool:
        """
        Export table data to file
        
        Args:
            table_name: Name of the table
            output_path: Path to output file
            schema: Schema name (optional)
            **kwargs: Additional export options
            
        Returns:
            True if successful, False otherwise
        """
        pass
        
    @abstractmethod
    def export_schema(self, table_name: str, output_path: Path,
                     schema: str = None, target_type: str = 'postgres', target_schema: str = 'public') -> bool:
        """
        Export table schema as DDL
        
        Args:
            table_name: Name of the table
            output_path: Path to output SQL file
            schema: Schema name (optional)
            target_type: Target database type for DDL conversion
            target_schema: Target schema for DDL generation
            
        Returns:
            True if successful, False otherwise
        """
        pass


class Exporter(ABC):
    """Base class for data exporters"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize exporter
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.connected = False
        
    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to data destination
        
        Returns:
            True if successful, False otherwise
        """
        pass
        
    @abstractmethod
    def disconnect(self):
        """Close connection to data destination"""
        pass
        
    @abstractmethod
    def create_schema(self, schema_file: Path, **kwargs) -> bool:
        """
        Create table from schema DDL file
        
        Args:
            schema_file: Path to DDL file
            **kwargs: Additional options
            
        Returns:
            True if successful, False otherwise
        """
        pass
        
    @abstractmethod
    def import_data(self, table_name: str, data_file: Path,
                   schema: str = None, **kwargs) -> bool:
        """
        Import data from file into table
        
        Args:
            table_name: Name of the table
            data_file: Path to data file
            schema: Schema name (optional)
            **kwargs: Additional import options
            
        Returns:
            True if successful, False otherwise
        """
        pass
        
    @abstractmethod
    def get_row_count(self, table_name: str, schema: str = None) -> int:
        """
        Get row count for a table
        
        Args:
            table_name: Name of the table
            schema: Schema name (optional)
            
        Returns:
            Number of rows
        """
        pass
        
    @abstractmethod
    def table_exists(self, table_name: str, schema: str = None) -> bool:
        """
        Check if table exists
        
        Args:
            table_name: Name of the table
            schema: Schema name (optional)
            
        Returns:
            True if exists, False otherwise
        """
        pass
        
    @abstractmethod
    def drop_table(self, table_name: str, schema: str = None) -> bool:
        """
        Drop a table
        
        Args:
            table_name: Name of the table
            schema: Schema name (optional)
            
        Returns:
            True if successful, False otherwise
        """
        pass


class FlatFileFormat:
    """Flatfile format configuration"""
    
    CSV = 'csv'
    TSV = 'tsv'
    PIPE = 'pipe'
    CUSTOM = 'custom'
    
    def __init__(self, format_type: str = CSV, delimiter: str = None,
                 quote: str = '"', escape: str = '"', header: bool = True,
                 encoding: str = 'utf-8'):
        """
        Initialize flatfile format
        
        Args:
            format_type: Format type (csv, tsv, pipe, custom)
            delimiter: Field delimiter (auto-detected from format_type if None)
            quote: Quote character
            escape: Escape character
            header: Whether file has header row
            encoding: File encoding
        """
        self.format_type = format_type
        
        # Auto-detect delimiter
        if delimiter is None:
            if format_type == self.CSV:
                delimiter = ','
            elif format_type == self.TSV:
                delimiter = '\t'
            elif format_type == self.PIPE:
                delimiter = '|'
            else:
                delimiter = ','
                
        self.delimiter = delimiter
        self.quote = quote
        self.escape = escape
        self.header = header
        self.encoding = encoding
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'format_type': self.format_type,
            'delimiter': self.delimiter,
            'quote': self.quote,
            'escape': self.escape,
            'header': self.header,
            'encoding': self.encoding
        }

