"""
GeoPackage (GPKG) importer and exporter
Uses SQLite backend with spatial extensions
"""

import sqlite3
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from base_interfaces import Importer, Exporter, TableSchema
import subprocess
import shlex

logger = logging.getLogger(__name__)


class GPKGImporter(Importer):
    """Imports data from GeoPackage files"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize GPKG importer
        
        Args:
            config: Configuration dictionary with 'file_path'
        """
        super().__init__(config)
        self.file_path = Path(config.get('file_path', ''))
        self.conn = None
        
    def connect(self) -> bool:
        """Connect to GeoPackage file"""
        try:
            if not self.file_path.exists():
                logger.error(f"GeoPackage file not found: {self.file_path}")
                return False
                
            self.conn = sqlite3.connect(str(self.file_path))
            self.connected = True
            logger.info(f"Connected to GeoPackage: {self.file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to GeoPackage: {e}")
            return False
            
    def disconnect(self):
        """Close connection to GeoPackage"""
        if self.conn:
            self.conn.close()
            self.connected = False
            logger.info("Disconnected from GeoPackage")
            
    def get_tables(self) -> List[str]:
        """
        Get list of tables in GeoPackage
        
        Returns:
            List of table names
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT table_name 
                FROM gpkg_contents 
                WHERE data_type IN ('features', 'attributes')
            """)
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return tables
            
        except Exception as e:
            logger.error(f"Error getting tables from GeoPackage: {e}")
            return []
            
    def get_schema(self, table_name: str, schema: str = None) -> TableSchema:
        """
        Get schema information for a table
        
        Args:
            table_name: Name of the table
            schema: Schema name (ignored for GPKG)
            
        Returns:
            TableSchema object
        """
        table_schema = TableSchema('gpkg', 'main', table_name)
        
        try:
            cursor = self.conn.cursor()
            
            # Get column information
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            for col in columns:
                col_id, col_name, col_type, not_null, default_val, is_pk = col
                table_schema.columns.append({
                    'name': col_name,
                    'type': col_type,
                    'nullable': not not_null == 0,
                    'default': default_val,
                    'is_primary_key': is_pk == 1
                })
                
                if is_pk:
                    table_schema.primary_keys.append(col_name)
                    
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {e}")
            
        return table_schema
        
    def get_row_count(self, table_name: str, schema: str = None) -> int:
        """
        Get row count for a table
        
        Args:
            table_name: Name of the table
            schema: Schema name (ignored)
            
        Returns:
            Number of rows
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
            
        except Exception as e:
            logger.error(f"Error getting row count for {table_name}: {e}")
            return 0
            
    def export_data(self, table_name: str, output_path: Path,
                   schema: str = None, **kwargs) -> bool:
        """
        Export table data to CSV
        
        Args:
            table_name: Name of the table
            output_path: Path to output file
            schema: Schema name (ignored)
            **kwargs: Additional options
            
        Returns:
            True if successful, False otherwise
        """
        try:
            import csv
            
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT * FROM {table_name}")
            
            # Get column names
            column_names = [desc[0] for desc in cursor.description]
            
            # Write to CSV
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(column_names)
                writer.writerows(cursor)
                
            cursor.close()
            logger.info(f"Data exported from GPKG to: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting data from GPKG: {e}")
            return False
            
    def export_schema(self, table_name: str, output_path: Path,
                     schema: str = None, target_type: str = 'postgres', target_schema: str = 'public') -> bool:
        """
        Export table schema as DDL
        
        Args:
            table_name: Name of the table
            output_path: Path to output SQL file
            schema: Schema name (for target)
            target_type: Target database type
            
        Returns:
            True if successful, False otherwise
        """
        try:
            table_schema = self.get_schema(table_name, schema)
            target_schema = schema or 'public'
            
            # Type mapping SQLite -> PostgreSQL
            type_map = {
                'INTEGER': 'INTEGER',
                'REAL': 'DOUBLE PRECISION',
                'TEXT': 'TEXT',
                'BLOB': 'BYTEA',
                'NUMERIC': 'NUMERIC',
                'BOOLEAN': 'BOOLEAN',
                'DATE': 'DATE',
                'DATETIME': 'TIMESTAMP',
                'GEOMETRY': 'GEOMETRY',
                'POINT': 'GEOMETRY(POINT)',
                'LINESTRING': 'GEOMETRY(LINESTRING)',
                'POLYGON': 'GEOMETRY(POLYGON)',
                'MULTIPOINT': 'GEOMETRY(MULTIPOINT)',
                'MULTILINESTRING': 'GEOMETRY(MULTILINESTRING)',
                'MULTIPOLYGON': 'GEOMETRY(MULTIPOLYGON)'
            }
            
            # Generate DDL
            ddl = f"-- Generated from GeoPackage: {self.file_path.name}\n"
            ddl += f"CREATE TABLE {target_schema}.{table_name} (\n"
            
            column_defs = []
            for col in table_schema.columns:
                col_type = type_map.get(col['type'].upper(), col['type'])
                col_def = f"    {col['name']} {col_type}"
                
                if not col['nullable']:
                    col_def += " NOT NULL"
                    
                if col.get('default'):
                    col_def += f" DEFAULT {col['default']}"
                    
                column_defs.append(col_def)
                
            # Add primary key constraint
            if table_schema.primary_keys:
                pk_cols = ', '.join(table_schema.primary_keys)
                column_defs.append(f"    PRIMARY KEY ({pk_cols})")
                
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

    #################################################################
    # New: Bulk GPKG -> PostGIS loader using ogr2ogr
    #################################################################
    def build_pg_connection_string(self, pg_config: Dict[str, Any]) -> str:
        """
        Build a libpq style connection string for ogr2ogr/PG: PG:"host=... dbname=... user=... password=..."
        """
        parts = []
        host = pg_config.get('host')
        port = pg_config.get('port')
        database = pg_config.get('database')
        user = pg_config.get('username')
        password = pg_config.get('password')
        if host:
            parts.append(f"host={host}")
        if port:
            parts.append(f"port={port}")
        if database:
            parts.append(f"dbname={database}")
        if user:
            parts.append(f"user={user}")
        if password:
            parts.append(f"password={password}")
        return ' '.join(parts)

    def load_all_gpkg_to_postgres(self, directory: Path, pg_config: Dict[str, Any], target_schema: str, ogr_options: str = "") -> bool:
        """
        Load all .gpkg files from a directory into PostgreSQL using ogr2ogr.

        Args:
            directory: Path to folder containing .gpkg files
            pg_config: PostgreSQL connection config dictionary
            target_schema: Target schema name in Postgres
            ogr_options: Additional ogr2ogr CLI options (string)

        Returns:
            True if all imports succeeded, False if any failed
        """
        try:
            directory = Path(directory)
            if not directory.exists() or not directory.is_dir():
                logger.error(f"GPKG directory not found: {directory}")
                return False

            gpkg_files = list(directory.glob('*.gpkg'))
            if not gpkg_files:
                logger.info(f"No .gpkg files found in {directory}")
                return True

            pg_conn = self.build_pg_connection_string(pg_config)

            # Prepare execution options
            use_docker_force = False
            dry_run = False
            parallel_jobs = 1
            try:
                # The caller may pass pg_config with nested 'gpkg' config; try to read overrides
                if isinstance(pg_config, dict):
                    gpkg_opts = pg_config.get('gpkg_options', {}) or {}
                    use_docker_force = gpkg_opts.get('use_docker_for_gdal', False)
                    dry_run = gpkg_opts.get('dry_run', False)
                    parallel_jobs = int(gpkg_opts.get('parallel_jobs', 1))
            except Exception:
                pass

            # If caller didn't pass options, try to read from config-like keys in pg_config
            if not use_docker_force and isinstance(pg_config, dict):
                use_docker_force = pg_config.get('use_docker_for_gdal', use_docker_force)

            # Allow directory-level config to provide defaults
            try:
                # default: read from environment or leave as-is
                pass
            except Exception:
                pass

            all_ok = True

            # Worker function for one file
            def import_one(gpkg: Path) -> bool:
                layer_name = gpkg.stem

                base_cmd = [
                    'ogr2ogr',
                    '-f', 'PostgreSQL',
                    f'PG:"{pg_conn}"',
                    str(gpkg),
                    '-nln', f"{target_schema}.{layer_name}",
                    '-lco', f"SCHEMA={target_schema}",
                ]

                if ogr_options:
                    base_cmd.extend(shlex.split(ogr_options))

                # Dockerized command (mounting the directory)
                docker_cmd = [
                    'docker', 'run', '--rm',
                    '-v', f"{directory.resolve()}:/data",
                    'osgeo/gdal',
                    'ogr2ogr',
                    '-f', 'PostgreSQL',
                    f'PG:"{pg_conn}"',
                    f'/data/{gpkg.name}',
                    '-nln', f"{target_schema}.{layer_name}",
                    '-lco', f"SCHEMA={target_schema}"
                ]
                if ogr_options:
                    docker_cmd.extend(shlex.split(ogr_options))

                # Choose which command to run based on flags and availability
                commands_to_try = []
                if use_docker_force:
                    commands_to_try = [(docker_cmd, True)]
                else:
                    commands_to_try = [(base_cmd, False), (docker_cmd, True)]

                logger.info(f"Importing {gpkg.name} into {target_schema}.{layer_name}")

                for cmd, is_docker in commands_to_try:
                    cmd_str = ' '.join(cmd)
                    if dry_run:
                        logger.info(f"[dry-run] Would run: {cmd_str}")
                        return True

                    try:
                        logger.debug(f"Running: {cmd_str}")
                        res = subprocess.run(cmd, capture_output=True, text=True)
                    except FileNotFoundError:
                        # Command not found (ogr2ogr or docker)
                        if is_docker:
                            logger.debug("Docker not available for fallback")
                            continue
                        else:
                            logger.debug("Local ogr2ogr not available")
                            continue

                    if res.returncode == 0:
                        logger.info(f"Imported {gpkg.name} -> {target_schema}.{layer_name}{' (via Docker)' if is_docker else ''}")
                        if res.stdout:
                            logger.debug(res.stdout)
                        return True
                    else:
                        stderr = (res.stderr or '').strip()
                        logger.error(f"ogr2ogr failed for {gpkg.name} (docker={is_docker}): returncode={res.returncode}")
                        if stderr:
                            logger.error(stderr)
                        # If the failure indicates missing Postgres driver and we haven't tried docker, continue to docker
                        if "Unable to find driver `PostgreSQL'" in stderr or "driver `PostgreSQL'" in stderr:
                            logger.warning("Missing Postgres driver in this ogr2ogr build; will try Docker fallback if available")
                            continue
                        # Otherwise, try next candidate (docker) if available
                        continue

                # All commands exhausted
                logger.error(f"All import attempts failed for {gpkg.name}")
                return False

            # Run imports (parallel or sequential)
            if parallel_jobs and parallel_jobs > 1:
                from concurrent.futures import ThreadPoolExecutor, as_completed

                with ThreadPoolExecutor(max_workers=parallel_jobs) as ex:
                    futures = {ex.submit(import_one, f): f for f in gpkg_files}
                    for fut in as_completed(futures):
                        gpkg = futures[fut]
                        try:
                            ok = fut.result()
                        except Exception as e:
                            logger.error(f"Exception importing {gpkg.name}: {e}")
                            ok = False
                        if not ok:
                            all_ok = False
            else:
                for gpkg in gpkg_files:
                    ok = import_one(gpkg)
                    if not ok:
                        all_ok = False

            return all_ok
        except Exception as e:
            logger.error(f"Error loading GPKG files to Postgres: {e}")
            return False


class GPKGExporter(Exporter):
    """Exports data to GeoPackage files"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize GPKG exporter
        
        Args:
            config: Configuration dictionary with 'file_path'
        """
        super().__init__(config)
        self.file_path = Path(config.get('file_path', 'output.gpkg'))
        self.conn = None
        
    def connect(self) -> bool:
        """Connect to or create GeoPackage file"""
        try:
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Create or open GeoPackage
            self.conn = sqlite3.connect(str(self.file_path))
            
            # Initialize GeoPackage structure if new
            self._initialize_gpkg()
            
            self.connected = True
            logger.info(f"Connected to GeoPackage: {self.file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to GeoPackage: {e}")
            return False
            
    def _initialize_gpkg(self):
        """Initialize GeoPackage metadata tables"""
        cursor = self.conn.cursor()
        
        # Check if already initialized
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='gpkg_contents'
        """)
        
        if cursor.fetchone():
            cursor.close()
            return
            
        # Create gpkg_contents table
        cursor.execute("""
            CREATE TABLE gpkg_contents (
                table_name TEXT NOT NULL PRIMARY KEY,
                data_type TEXT NOT NULL,
                identifier TEXT UNIQUE,
                description TEXT DEFAULT '',
                last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                min_x DOUBLE,
                min_y DOUBLE,
                max_x DOUBLE,
                max_y DOUBLE,
                srs_id INTEGER
            )
        """)
        
        self.conn.commit()
        cursor.close()
        logger.info("Initialized GeoPackage structure")
        
    def disconnect(self):
        """Close connection to GeoPackage"""
        if self.conn:
            self.conn.close()
            self.connected = False
            logger.info("Disconnected from GeoPackage")
            
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
            with open(schema_file, 'r', encoding='utf-8') as f:
                ddl = f.read()
                
            # Execute DDL (may need adaptation for SQLite)
            cursor = self.conn.cursor()
            cursor.executescript(ddl)
            self.conn.commit()
            cursor.close()
            
            logger.info(f"Schema created from: {schema_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating schema: {e}")
            return False
            
    def import_data(self, table_name: str, data_file: Path,
                   schema: str = None, **kwargs) -> bool:
        """
        Import data from CSV into GeoPackage table
        
        Args:
            table_name: Name of the table
            data_file: Path to CSV file
            schema: Schema name (ignored)
            **kwargs: Additional options
            
        Returns:
            True if successful, False otherwise
        """
        try:
            import csv
            
            with open(data_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                headers = next(reader)
                
                # Build INSERT statement
                placeholders = ','.join(['?' for _ in headers])
                insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
                
                cursor = self.conn.cursor()
                cursor.executemany(insert_sql, reader)
                self.conn.commit()
                cursor.close()
                
            # Register in gpkg_contents
            self._register_table(table_name)
            
            logger.info(f"Data imported into GPKG table: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error importing data to GPKG: {e}")
            return False
            
    def _register_table(self, table_name: str):
        """Register table in gpkg_contents"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO gpkg_contents 
                (table_name, data_type, identifier, last_change)
                VALUES (?, 'attributes', ?, datetime('now'))
            """, (table_name, table_name))
            self.conn.commit()
            cursor.close()
        except Exception as e:
            logger.warning(f"Could not register table in gpkg_contents: {e}")
            
    def get_row_count(self, table_name: str, schema: str = None) -> int:
        """
        Get row count for a table
        
        Args:
            table_name: Name of the table
            schema: Schema name (ignored)
            
        Returns:
            Number of rows
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
            
        except Exception as e:
            logger.error(f"Error getting row count: {e}")
            return 0
            
    def table_exists(self, table_name: str, schema: str = None) -> bool:
        """
        Check if table exists in GeoPackage
        
        Args:
            table_name: Name of the table
            schema: Schema name (ignored)
            
        Returns:
            True if exists, False otherwise
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?
            """, (table_name,))
            exists = cursor.fetchone() is not None
            cursor.close()
            return exists
            
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False
            
    def drop_table(self, table_name: str, schema: str = None) -> bool:
        """
        Drop a table from GeoPackage
        
        Args:
            table_name: Name of the table
            schema: Schema name (ignored)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            cursor.execute("DELETE FROM gpkg_contents WHERE table_name=?", (table_name,))
            self.conn.commit()
            cursor.close()
            
            logger.info(f"Table dropped from GPKG: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error dropping table: {e}")
            return False

