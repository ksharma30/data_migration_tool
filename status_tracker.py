"""
Status tracking with YAML output
Tracks: input_count, flatfile_count, output_count per schema.table
"""

import yaml
import logging
from pathlib import Path
from typing import Dict, Any
from collections import defaultdict

logger = logging.getLogger(__name__)


class StatusTracker:
    """Tracks migration status and row counts"""
    
    def __init__(self, status_file: Path):
        """
        Initialize status tracker
        
        Args:
            status_file: Path to status.yaml file
        """
        self.status_file = Path(status_file)
        self.data = defaultdict(lambda: defaultdict(dict))
        
        # Load existing status if file exists
        if self.status_file.exists():
            self.load()
            
    def load(self):
        """Load status from YAML file"""
        try:
            with open(self.status_file, 'r', encoding='utf-8') as f:
                loaded = yaml.safe_load(f)
                if loaded:
                    self.data = defaultdict(lambda: defaultdict(dict), loaded)
            logger.info(f"Status loaded from: {self.status_file}")
        except Exception as e:
            logger.warning(f"Could not load status file: {e}")
            
    def save(self):
        """Save status to YAML file"""
        try:
            self.status_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert defaultdict to regular dict for YAML
            output = {}
            for schema, tables in self.data.items():
                output[schema] = {}
                for table, counts in tables.items():
                    output[schema][table] = dict(counts)
                    
            with open(self.status_file, 'w', encoding='utf-8') as f:
                yaml.dump(output, f, default_flow_style=False, sort_keys=False)
                
            logger.info(f"Status saved to: {self.status_file}")
        except Exception as e:
            logger.error(f"Failed to save status file: {e}")
            
    def get_input_count(self, schema: str, table: str) -> int:
        """Get input count for a table"""
        table_key = f"{schema}.{table}"
        table_status = self.status.get('tables', {}).get(table_key, {})
        return table_status.get('input_count', 0)
        
    def set_input_count(self, schema: str, table: str, count: int):
        """
        Set input row count
        
        Args:
            schema: Schema name
            table: Table name
            count: Row count
        """
        if schema not in self.data:
            self.data[schema] = defaultdict(dict)
        if table not in self.data[schema]:
            self.data[schema][table] = {}
            
        self.data[schema][table]['input_count'] = count
        logger.debug(f"Set input_count for {schema}.{table}: {count}")
        
    def set_flatfile_count(self, schema: str, table: str, count: int):
        """
        Set flatfile row count
        
        Args:
            schema: Schema name
            table: Table name
            count: Row count
        """
        if schema not in self.data:
            self.data[schema] = defaultdict(dict)
        if table not in self.data[schema]:
            self.data[schema][table] = {}
            
        self.data[schema][table]['flatfile_count'] = count
        logger.debug(f"Set flatfile_count for {schema}.{table}: {count}")
        
    def set_output_count(self, schema: str, table: str, count: int):
        """
        Set output row count
        
        Args:
            schema: Schema name
            table: Table name
            count: Row count
        """
        if schema not in self.data:
            self.data[schema] = defaultdict(dict)
        if table not in self.data[schema]:
            self.data[schema][table] = {}
            
        self.data[schema][table]['output_count'] = count
        logger.debug(f"Set output_count for {schema}.{table}: {count}")
        
    def get_counts(self, schema: str, table: str) -> Dict[str, int]:
        """
        Get all counts for a table
        
        Args:
            schema: Schema name
            table: Table name
            
        Returns:
            Dictionary with counts
        """
        if schema in self.data and table in self.data[schema]:
            return dict(self.data[schema][table])
        return {'input_count': 0, 'flatfile_count': 0, 'output_count': 0}
        
    def get_input_count(self, schema: str, table: str) -> int:
        """Get input count for table"""
        counts = self.get_counts(schema, table)
        return counts.get('input_count', 0)
        
    def get_flatfile_count(self, schema: str, table: str) -> int:
        """Get flatfile count for table"""
        counts = self.get_counts(schema, table)
        return counts.get('flatfile_count', 0)
        
    def get_output_count(self, schema: str, table: str) -> int:
        """Get output count for table"""
        counts = self.get_counts(schema, table)
        return counts.get('output_count', 0)
        
    def get_all_schemas(self) -> list:
        """Get list of all schemas"""
        return list(self.data.keys())
        
    def get_tables_for_schema(self, schema: str) -> list:
        """Get list of tables for a schema"""
        if schema in self.data:
            return list(self.data[schema].keys())
        return []
        
    def clear(self):
        """Clear all status data"""
        self.data = defaultdict(lambda: defaultdict(dict))
        logger.info("Status data cleared")
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        output = {}
        for schema, tables in self.data.items():
            output[schema] = {}
            for table, counts in tables.items():
                output[schema][table] = dict(counts)
        return output

