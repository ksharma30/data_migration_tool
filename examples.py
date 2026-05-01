"""
Example usage of the SQL Server to PostgreSQL migration tool
This script demonstrates how to use the migration tool programmatically
"""

# Example 1: Basic migration using config file
# ----------------------------------------------
def example_basic_migration():
    """Run migration using default config file"""
    from config_loader import load_config, setup_logging
    from migration_processor import MigrationProcessor
    
    # Load configuration
    config = load_config('config.yaml')
    
    # Setup logging
    setup_logging(config)
    
    # Create and run processor
    processor = MigrationProcessor(config)
    processor.run()
    
    print(f"Migration status: {processor.overall_status}")


# Example 2: Migrate specific tables
# -----------------------------------
def example_specific_tables():
    """Migrate only specific tables"""
    from config_loader import load_config, setup_logging
    from migration_processor import MigrationProcessor
    
    config = load_config('config.yaml')
    
    # Override tables in config
    config['migration']['tables'] = ['Customers', 'Orders', 'Products']
    
    setup_logging(config)
    processor = MigrationProcessor(config)
    processor.run()


# Example 3: Custom migration with schema extraction
# ---------------------------------------------------
def example_schema_extraction():
    """Extract schema for a single table"""
    from schema_extractor import SchemaExtractor
    
    # Connection string
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost,1433;"
        "DATABASE=MyDatabase;"
        "UID=sa;"
        "PWD=MyPassword;"
    )
    
    # Create extractor
    extractor = SchemaExtractor(conn_str)
    extractor.connect()
    
    # Get all tables
    tables = extractor.get_all_tables()
    print(f"Found {len(tables)} tables: {tables}")
    
    # Get schema for specific table
    table_name = 'Customers'
    columns = extractor.get_table_columns(table_name)
    
    print(f"\nColumns in {table_name}:")
    for col in columns:
        print(f"  {col['name']}: {col['data_type']}")
    
    # Generate DDL
    ddl = extractor.generate_create_table_ddl(table_name)
    print(f"\nGenerated DDL:\n{ddl}")
    
    extractor.disconnect()


# Example 4: Manual BCP export
# -----------------------------
def example_bcp_export():
    """Export a table using BCP"""
    from bcp_exporter import BCPExporter
    
    # Create exporter
    exporter = BCPExporter(
        server='localhost,1433',
        database='MyDatabase',
        username='sa',
        password='MyPassword'
    )
    
    # Export table
    success = exporter.export_table_with_header(
        table_name='Customers',
        output_file='customers.csv',
        column_list=['CustomerID', 'CustomerName', 'Email'],
        field_delimiter=',',
        batch_size=100000
    )
    
    print(f"Export {'successful' if success else 'failed'}")


# Example 5: Manual PostgreSQL load
# ----------------------------------
def example_postgres_load():
    """Load CSV into PostgreSQL"""
    from postgres_loader import PostgreSQLLoader
    
    # Create loader
    loader = PostgreSQLLoader(
        host='localhost',
        port=5432,
        database='mydb',
        username='postgres',
        password='postgres',
        ssl=False
    )
    
    loader.connect()
    
    # Create table
    ddl = """
    CREATE TABLE customers (
        customer_id INTEGER PRIMARY KEY,
        customer_name VARCHAR(100),
        email VARCHAR(100)
    );
    """
    loader.execute_sql(ddl)
    
    # Load CSV
    success = loader.load_csv_with_copy(
        table_name='customers',
        csv_file='customers.csv',
        delimiter=',',
        header=True
    )
    
    # Verify
    if success:
        count = loader.get_row_count('customers')
        print(f"Loaded {count} rows")
    
    loader.disconnect()


# Example 6: Connection testing
# ------------------------------
def example_test_connections():
    """Test database connections"""
    from config_loader import load_config
    from schema_extractor import SchemaExtractor
    from postgres_loader import PostgreSQLLoader
    
    config = load_config('config.yaml')
    
    # Test SQL Server
    try:
        src = config['source']
        conn_str = (
            f"DRIVER={{{src['driver']}}};"
            f"SERVER={src['host']},{src['port']};"
            f"DATABASE={src['database']};"
            f"UID={src['username']};"
            f"PWD={src['password']};"
        )
        
        extractor = SchemaExtractor(conn_str)
        extractor.connect()
        tables = extractor.get_all_tables()
        print(f"✓ SQL Server connected: {len(tables)} tables")
        extractor.disconnect()
        
    except Exception as e:
        print(f"✗ SQL Server connection failed: {e}")
    
    # Test PostgreSQL
    try:
        dst = config['destination']
        loader = PostgreSQLLoader(
            host=dst['host'],
            port=dst['port'],
            database=dst['database'],
            username=dst['username'],
            password=dst['password'],
            ssl=dst.get('ssl', False)
        )
        loader.connect()
        print(f"✓ PostgreSQL connected")
        loader.disconnect()
        
    except Exception as e:
        print(f"✗ PostgreSQL connection failed: {e}")


# Example 7: Migrate single table programmatically
# -------------------------------------------------
def example_migrate_single_table():
    """Migrate a single table with custom settings"""
    from config_loader import load_config, setup_logging
    from migration_processor import MigrationProcessor
    
    config = load_config('config.yaml')
    setup_logging(config)
    
    # Create processor
    processor = MigrationProcessor(config)
    processor.initialize_connections()
    
    # Migrate single table
    job = processor.migrate_table('Customers', schema='dbo', pg_schema='public')
    
    print(f"Table: {job.table_name}")
    print(f"Status: {job.status}")
    print(f"Source rows: {job.row_count_source}")
    print(f"Target rows: {job.row_count_target}")
    print(f"Duration: {job.duration:.2f}s")
    
    processor.close_connections()


# Example 8: Get migration report
# --------------------------------
def example_get_report():
    """Get detailed migration report"""
    from config_loader import load_config, setup_logging
    from migration_processor import MigrationProcessor
    
    config = load_config('config.yaml')
    setup_logging(config)
    
    processor = MigrationProcessor(config)
    processor.run()
    
    # Print summary
    print("\nMigration Results:")
    for job in processor.jobs:
        status_icon = "✓" if job.status == "SUCCESS" else "✗"
        print(f"{status_icon} {job.table_name}: {job.row_count_source} rows, {job.duration:.2f}s")


# Example 9: Custom data type mapping
# ------------------------------------
def example_custom_type_mapping():
    """Customize data type mapping"""
    from schema_extractor import SchemaExtractor
    
    # Modify type mapping
    SchemaExtractor.TYPE_MAPPING['datetime'] = 'TIMESTAMPTZ'  # Use timezone-aware
    SchemaExtractor.TYPE_MAPPING['money'] = 'NUMERIC(19,4)'   # Different precision
    
    # Now run extraction with custom mapping
    conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;..."
    extractor = SchemaExtractor(conn_str)
    extractor.connect()
    
    # Your extraction code here
    
    extractor.disconnect()


# Example 10: Batch migration with error handling
# ------------------------------------------------
def example_batch_migration_with_errors():
    """Migrate multiple databases with error handling"""
    from config_loader import load_config, setup_logging
    from migration_processor import MigrationProcessor
    import logging
    
    databases = ['DB1', 'DB2', 'DB3']
    results = {}
    
    for db_name in databases:
        try:
            print(f"\n{'='*80}")
            print(f"Migrating database: {db_name}")
            print(f"{'='*80}")
            
            # Load config and update database name
            config = load_config('config.yaml')
            config['source']['database'] = db_name
            config['destination']['database'] = f"{db_name}_pg"
            
            setup_logging(config)
            
            # Run migration
            processor = MigrationProcessor(config)
            processor.run()
            
            results[db_name] = processor.overall_status
            
        except Exception as e:
            logging.error(f"Failed to migrate {db_name}: {e}")
            results[db_name] = "ERROR"
    
    # Print summary
    print(f"\n{'='*80}")
    print("Overall Migration Summary")
    print(f"{'='*80}")
    for db_name, status in results.items():
        print(f"{db_name}: {status}")


if __name__ == '__main__':
    # Run examples (comment/uncomment as needed)
    
    # Basic usage
    # example_basic_migration()
    
    # Advanced usage
    # example_specific_tables()
    # example_schema_extraction()
    # example_bcp_export()
    # example_postgres_load()
    
    # Testing
    example_test_connections()
    
    # Custom scenarios
    # example_migrate_single_table()
    # example_get_report()
    # example_batch_migration_with_errors()

