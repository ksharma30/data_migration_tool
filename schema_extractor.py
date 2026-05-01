"""
SQL Server Schema Extractor
Extracts table schemas from SQL Server and generates PostgreSQL-compatible DDL
"""

import pyodbc
import logging
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)


class SchemaExtractor:
    """Extracts schema information from SQL Server"""
    
    # SQL Server to PostgreSQL data type mapping
    TYPE_MAPPING = {
        'bigint': 'BIGINT',
        'int': 'INTEGER',
        'smallint': 'SMALLINT',
        'tinyint': 'SMALLINT',
        'bit': 'BOOLEAN',
        'decimal': 'DECIMAL',
        'numeric': 'NUMERIC',
        'money': 'DECIMAL(19,4)',
        'smallmoney': 'DECIMAL(10,4)',
        'float': 'DOUBLE PRECISION',
        'real': 'REAL',
        'datetime': 'TIMESTAMP',
        'datetime2': 'TIMESTAMP',
        'smalldatetime': 'TIMESTAMP',
        'date': 'DATE',
        'time': 'TIME',
        'datetimeoffset': 'TIMESTAMP WITH TIME ZONE',
        'char': 'CHAR',
        'varchar': 'VARCHAR',
        'text': 'TEXT',
        'nchar': 'CHAR',
        'nvarchar': 'VARCHAR',
        'ntext': 'TEXT',
        'binary': 'BYTEA',
        'varbinary': 'BYTEA',
        'image': 'BYTEA',
        'uniqueidentifier': 'UUID',
        'xml': 'XML',
    }
    
    def __init__(self, connection_string: str):
        """
        Initialize schema extractor
        
        Args:
            connection_string: SQL Server connection string
        """
        self.connection_string = connection_string
        self.conn = None
        
    def connect(self):
        """Establish connection to SQL Server"""
        try:
            self.conn = pyodbc.connect(self.connection_string, timeout=30)
            logger.info("Connected to SQL Server successfully")
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {e}")
            raise
            
    def disconnect(self):
        """Close connection to SQL Server"""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from SQL Server")
            
    def get_all_tables(self, schema: str = 'dbo') -> List[str]:
        """
        Get list of all tables in the database
        
        Args:
            schema: Schema name (default: dbo)
            
        Returns:
            List of table names
        """
        query = """
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE' 
            AND TABLE_SCHEMA = ?
            ORDER BY TABLE_NAME
        """
        cursor = self.conn.cursor()
        cursor.execute(query, schema)
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables
        
    def table_exists(self, table_name: str, schema: str = 'dbo') -> bool:
        """
        Check if a table exists in the database
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: dbo)
            
        Returns:
            True if table exists, False otherwise
        """
        query = """
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = ? 
            AND TABLE_SCHEMA = ?
        """
        cursor = self.conn.cursor()
        cursor.execute(query, table_name, schema)
        count = cursor.fetchone()[0]
        cursor.close()
        return count > 0
        
    def get_table_columns(self, table_name: str, schema: str = 'dbo') -> List[Dict]:
        """
        Get column information for a table
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: dbo)
            
        Returns:
            List of column dictionaries
        """
        query = """
            SELECT 
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                c.IS_NULLABLE,
                c.COLUMN_DEFAULT,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY,
                COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN (
                SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                    ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                    AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                    AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
                    AND tc.TABLE_NAME = ku.TABLE_NAME
            ) pk ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA 
                AND c.TABLE_NAME = pk.TABLE_NAME 
                AND c.COLUMN_NAME = pk.COLUMN_NAME
            WHERE c.TABLE_NAME = ? 
            AND c.TABLE_SCHEMA = ?
            ORDER BY c.ORDINAL_POSITION
        """
        cursor = self.conn.cursor()
        cursor.execute(query, table_name, schema)
        
        columns = []
        for row in cursor.fetchall():
            col = {
                'name': row[0],
                'data_type': row[1].lower(),
                'max_length': row[2],
                'precision': row[3],
                'scale': row[4],
                'is_nullable': row[5] == 'YES',
                'default': row[6],
                'is_primary_key': row[7] == 1,
                'is_identity': row[8] == 1
            }
            columns.append(col)
            
        cursor.close()
        return columns
        
    def get_primary_key(self, table_name: str, schema: str = 'dbo') -> List[str]:
        """
        Get primary key columns for a table
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: dbo)
            
        Returns:
            List of primary key column names
        """
        query = """
            SELECT ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
                AND tc.TABLE_NAME = ku.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            AND tc.TABLE_NAME = ?
            AND tc.TABLE_SCHEMA = ?
            ORDER BY ku.ORDINAL_POSITION
        """
        cursor = self.conn.cursor()
        cursor.execute(query, table_name, schema)
        pk_columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return pk_columns
        
    def get_indexes(self, table_name: str, schema: str = 'dbo') -> List[Dict]:
        """
        Get index information for a table
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: dbo)
            
        Returns:
            List of index dictionaries
        """
        query = """
            SELECT 
                i.name AS index_name,
                i.is_unique,
                i.type_desc,
                STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN sys.tables t ON i.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = ?
            AND s.name = ?
            AND i.is_primary_key = 0
            AND i.is_unique_constraint = 0
            GROUP BY i.name, i.is_unique, i.type_desc
        """
        cursor = self.conn.cursor()
        cursor.execute(query, table_name, schema)
        
        indexes = []
        for row in cursor.fetchall():
            idx = {
                'name': row[0],
                'is_unique': row[1],
                'type': row[2],
                'columns': row[3]
            }
            indexes.append(idx)
            
        cursor.close()
        return indexes
        
    def get_foreign_keys(self, table_name: str, schema: str = 'dbo') -> List[Dict]:
        """
        Get foreign key information for a table
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: dbo)
            
        Returns:
            List of foreign key dictionaries
        """
        query = """
            SELECT 
                fk.name AS fk_name,
                STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS columns,
                OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS ref_schema,
                OBJECT_NAME(fk.referenced_object_id) AS ref_table,
                STRING_AGG(rc.name, ', ') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS ref_columns
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            JOIN sys.columns c ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
            JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
            JOIN sys.tables t ON fk.parent_object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name = ?
            AND s.name = ?
            GROUP BY fk.name, fk.referenced_object_id
        """
        cursor = self.conn.cursor()
        cursor.execute(query, table_name, schema)
        
        fks = []
        for row in cursor.fetchall():
            fk = {
                'name': row[0],
                'columns': row[1],
                'ref_schema': row[2],
                'ref_table': row[3],
                'ref_columns': row[4]
            }
            fks.append(fk)
            
        cursor.close()
        return fks
        
    def map_data_type(self, column: Dict) -> str:
        """
        Map SQL Server data type to PostgreSQL data type
        
        Args:
            column: Column dictionary
            
        Returns:
            PostgreSQL data type string
        """
        data_type = column['data_type']
        base_type = self.TYPE_MAPPING.get(data_type, 'TEXT')
        
        # Handle types with length/precision
        if data_type in ['char', 'varchar', 'nchar', 'nvarchar']:
            if column['max_length'] and column['max_length'] != -1:
                return f"{base_type}({column['max_length']})"
            else:
                return 'TEXT'
        elif data_type in ['decimal', 'numeric']:
            if column['precision'] and column['scale']:
                return f"{base_type}({column['precision']},{column['scale']})"
            else:
                return base_type
        elif data_type in ['binary', 'varbinary']:
            if column['max_length'] and column['max_length'] != -1:
                return base_type
            else:
                return 'BYTEA'
                
        return base_type
        
    def generate_create_table_ddl(self, table_name: str, schema: str = 'dbo', 
                                   pg_schema: str = 'public') -> str:
        """
        Generate PostgreSQL CREATE TABLE DDL
        
        Args:
            table_name: Name of the table
            schema: SQL Server schema name (default: dbo)
            pg_schema: PostgreSQL schema name (default: public)
            
        Returns:
            CREATE TABLE DDL string
        """
        columns = self.get_table_columns(table_name, schema)
        pk_columns = self.get_primary_key(table_name, schema)
        
        ddl = f"CREATE TABLE {pg_schema}.{table_name} (\n"
        
        column_defs = []
        for col in columns:
            pg_type = self.map_data_type(col)
            col_def = f"    {col['name']} {pg_type}"
            
            # Add NOT NULL constraint
            if not col['is_nullable']:
                col_def += " NOT NULL"
                
            # Add default value (simplified, may need more work)
            if col['default'] and not col['is_identity']:
                default_val = col['default'].strip('()')
                # Clean up SQL Server specific defaults
                if default_val.lower() not in ['getdate()', 'newid()']:
                    col_def += f" DEFAULT {default_val}"
                elif default_val.lower() == 'getdate()':
                    col_def += " DEFAULT CURRENT_TIMESTAMP"
                elif default_val.lower() == 'newid()':
                    col_def += " DEFAULT gen_random_uuid()"
                    
            column_defs.append(col_def)
            
        ddl += ",\n".join(column_defs)
        
        # Add primary key constraint
        if pk_columns:
            pk_cols = ", ".join(pk_columns)
            ddl += f",\n    CONSTRAINT {table_name}_pkey PRIMARY KEY ({pk_cols})"
            
        ddl += "\n);\n"
        
        return ddl
        
    def generate_indexes_ddl(self, table_name: str, schema: str = 'dbo',
                            pg_schema: str = 'public') -> str:
        """
        Generate PostgreSQL CREATE INDEX DDL
        
        Args:
            table_name: Name of the table
            schema: SQL Server schema name (default: dbo)
            pg_schema: PostgreSQL schema name (default: public)
            
        Returns:
            CREATE INDEX DDL string
        """
        indexes = self.get_indexes(table_name, schema)
        
        ddl = ""
        for idx in indexes:
            unique = "UNIQUE " if idx['is_unique'] else ""
            idx_name = f"{table_name}_{idx['name']}"
            columns = idx['columns']
            ddl += f"CREATE {unique}INDEX {idx_name} ON {pg_schema}.{table_name} ({columns});\n"
            
        return ddl
        
    def generate_foreign_keys_ddl(self, table_name: str, schema: str = 'dbo',
                                  pg_schema: str = 'public') -> str:
        """
        Generate PostgreSQL ALTER TABLE ADD FOREIGN KEY DDL
        
        Args:
            table_name: Name of the table
            schema: SQL Server schema name (default: dbo)
            pg_schema: PostgreSQL schema name (default: public)
            
        Returns:
            ALTER TABLE DDL string
        """
        fks = self.get_foreign_keys(table_name, schema)
        
        ddl = ""
        for fk in fks:
            fk_name = f"{table_name}_{fk['name']}"
            ddl += f"ALTER TABLE {pg_schema}.{table_name} ADD CONSTRAINT {fk_name} "
            ddl += f"FOREIGN KEY ({fk['columns']}) "
            ddl += f"REFERENCES {pg_schema}.{fk['ref_table']} ({fk['ref_columns']});\n"
            
        return ddl
        
    def get_row_count(self, table_name: str, schema: str = 'dbo') -> int:
        """
        Get approximate row count for a table
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: dbo)
            
        Returns:
            Row count
        """
        query = f"SELECT COUNT(*) FROM [{schema}].[{table_name}]"
        cursor = self.conn.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count
        
    def get_column_list(self, table_name: str, schema: str = 'dbo') -> List[str]:
        """
        Get list of column names for a table
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: dbo)
            
        Returns:
            List of column names
        """
        columns = self.get_table_columns(table_name, schema)
        return [col['name'] for col in columns]

