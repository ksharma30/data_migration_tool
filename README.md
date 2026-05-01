# SQL Server to PostgreSQL Migration Tool

A high-performance migration tool that uses BCP (Bulk Copy Program) to export data from SQL Server and PostgreSQL COPY command to import data, optimized for handling millions of rows efficiently.

## Features

- **Fast Data Export**: Uses BCP utility for high-speed data extraction from SQL Server
- **Fast Data Import**: Uses PostgreSQL COPY command for efficient bulk loading
- **Schema Migration**: Automatically converts SQL Server schema to PostgreSQL-compatible DDL
- **Data Type Mapping**: Intelligent mapping of SQL Server data types to PostgreSQL equivalents
- **Validation**: Checks table existence, row counts, and data integrity
- **Comprehensive Logging**: Detailed logs and migration reports
- **Configurable**: YAML-based configuration for easy customization
- **Resume Capability**: Directory structure preserves intermediate files for troubleshooting

## Project Structure

```
sql2pg_copy/
├── config.yaml              # Configuration file (YAML)
├── requirements.txt         # Python dependencies
├── migrate.py              # Main entry point
├── config_loader.py        # Configuration loading and validation
├── schema_extractor.py     # SQL Server schema extraction
├── bcp_exporter.py         # BCP export handler
├── postgres_loader.py      # PostgreSQL COPY loader
├── migration_processor.py  # Main migration orchestrator
├── intermediate/           # Intermediate files directory
│   └── [database_name]/
│       └── [table_name]/
│           ├── data.csv                    # Exported CSV data
│           ├── 1_schema_creation.sql       # Table creation DDL
│           └── 2_post_creation.sql         # Indexes and foreign keys
└── output/                 # Output directory
    ├── migration.log       # Migration log file
    └── migration_report_*.txt  # Migration reports
```

## Prerequisites

### Software Requirements

1. **Python 3.8+**
2. **SQL Server BCP Utility** (comes with SQL Server or can be installed separately)
3. **SQL Server ODBC Driver** (ODBC Driver 17 or later)
4. **Access to both databases** (SQL Server source and PostgreSQL target)

### Installing BCP on Windows

BCP is typically installed with SQL Server. To check if it's available:

```bash
bcp -v
```

If not available, install SQL Server Command Line Utilities from:
https://learn.microsoft.com/en-us/sql/tools/bcp-utility

### Installing ODBC Driver

Download and install the ODBC Driver for SQL Server from:
https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

## Installation

1. **Clone or download this project**

2. **Install Python dependencies**:

```bash
cd G:\sql2pg_copy
pip install -r requirements.txt
```

3. **Configure the migration** by editing `config.yaml` (see Configuration section)

## Configuration

Edit `config.yaml` to match your environment:

### Source Database (SQL Server)

```yaml
source:
  type: "mssql"
  host: "localhost"          # SQL Server host
  port: 1433                 # SQL Server port
  database: "YourSourceDB"   # Source database name
  username: "sa"             # SQL Server username (if not using Windows auth)
  password: "YourPassword"   # SQL Server password (if not using Windows auth)
  windows_auth: false        # Set to true for Windows Authentication
  driver: "ODBC Driver 17 for SQL Server"  # ODBC driver name
```

### Destination Database (PostgreSQL)

```yaml
destination:
  type: "postgres"
  host: "localhost"          # PostgreSQL host
  port: 5432                 # PostgreSQL port
  database: "YourTargetDB"   # Target database name
  username: "postgres"       # PostgreSQL username
  password: "YourPassword"   # PostgreSQL password
  ssl: false                 # Set to true if SSL is required
```

### Tables to Migrate

```yaml
migration:
  # List specific tables (or leave empty to migrate all tables)
  tables:
    - "table1"
    - "table2"
  
  # Tables to exclude from migration
  exclude_tables: []
  
  # Drop and recreate tables if they exist in PostgreSQL
  drop_if_exists: true
```

### Directory Configuration

```yaml
directories:
  input: "sql2pg_copy/input"              # Input directory (future use)
  intermediate: "sql2pg_copy/intermediate" # CSV and SQL files
  output: "sql2pg_copy/output"            # Logs and reports
```

## Usage

### Basic Usage

```bash
python migrate.py
```

This will use the default `config.yaml` in the current directory.

### Specify Custom Config File

```bash
python migrate.py path/to/your/config.yaml
```

## Migration Process

The tool performs the following steps for each table:

1. **Validation**: Checks if table exists in source database
2. **Schema Extraction**: Extracts table schema from SQL Server
3. **Schema Conversion**: Converts SQL Server DDL to PostgreSQL DDL
4. **Table Creation**: Creates table in PostgreSQL (drops if exists and configured)
5. **Data Export**: Uses BCP to export data to CSV file
6. **Data Import**: Uses PostgreSQL COPY to load CSV into table
7. **Verification**: Compares row counts between source and target
8. **Post-Processing**: Creates indexes (foreign keys added last to avoid conflicts)
9. **Optimization**: Runs VACUUM ANALYZE on the table

## Data Type Mapping

The tool automatically maps SQL Server data types to PostgreSQL equivalents:

| SQL Server | PostgreSQL |
|-----------|-----------|
| bigint | BIGINT |
| int | INTEGER |
| smallint | SMALLINT |
| tinyint | SMALLINT |
| bit | BOOLEAN |
| decimal(p,s) | DECIMAL(p,s) |
| money | DECIMAL(19,4) |
| float | DOUBLE PRECISION |
| datetime/datetime2 | TIMESTAMP |
| date | DATE |
| varchar(n) | VARCHAR(n) |
| nvarchar(n) | VARCHAR(n) |
| text/ntext | TEXT |
| binary/varbinary | BYTEA |
| uniqueidentifier | UUID |

## Output Files

### Intermediate Files (per table)

```
intermediate/[database_name]/[table_name]/
├── data.csv                    # Exported data in CSV format
├── 1_schema_creation.sql       # CREATE TABLE statement
└── 2_post_creation.sql         # Indexes and foreign key constraints
```

### Output Files

```
output/
├── migration.log                      # Detailed migration log
└── migration_report_YYYYMMDD_HHMMSS.txt  # Summary report
```

## Troubleshooting

### BCP Not Found

**Error**: `BCP utility not found`

**Solution**: 
- Install SQL Server Command Line Utilities
- Add BCP to your system PATH
- Verify with: `bcp -v`

### Connection Failed

**Error**: `Failed to connect to SQL Server/PostgreSQL`

**Solution**:
- Verify host, port, and credentials in config.yaml
- Check firewall rules
- For PostgreSQL, ensure `pg_hba.conf` allows connections
- For SSL connections to PostgreSQL, set `ssl: true` in config

### ODBC Driver Not Found

**Error**: `[ODBC Driver 17 for SQL Server] not found`

**Solution**:
- Install ODBC Driver for SQL Server
- Update `driver` in config.yaml to match your installed driver
- Check available drivers: `odbcinst -q -d` (Linux) or ODBC Data Source Administrator (Windows)

### Row Count Mismatch

**Warning**: `Row count mismatch`

**Possible causes**:
- Data was modified during migration
- Character encoding issues
- NULL handling differences
- Check migration logs for details

### Table Already Exists

**Error**: `Table already exists`

**Solution**:
- Set `drop_if_exists: true` in config.yaml to automatically drop existing tables
- Or manually drop tables in PostgreSQL before migration

## Performance Tuning

### For Large Tables (millions of rows)

1. **Increase batch size**:
```yaml
bcp:
  batch_size: 500000  # Increase for better performance
```

2. **Disable constraints during load**:
```yaml
schema:
  create_indexes: false        # Create indexes manually after all data is loaded
  create_foreign_keys: false   # Add foreign keys after all tables are migrated
```

3. **Run in parallel** (future enhancement):
```yaml
performance:
  parallel_tables: 4  # Number of tables to migrate in parallel
```

4. **Adjust timeouts**:
```yaml
performance:
  bcp_timeout: 7200     # 2 hours for very large tables
  copy_timeout: 7200
```

## Best Practices

1. **Test on a small subset first**: Start with a few tables to validate the configuration
2. **Backup your PostgreSQL database**: Before running migration with `drop_if_exists: true`
3. **Monitor disk space**: Ensure sufficient space in intermediate directory (CSV files can be large)
4. **Run during maintenance window**: For production databases with millions of rows
5. **Add foreign keys last**: Migrate all tables first, then add foreign key constraints manually
6. **Review logs**: Check migration.log and reports for any warnings or errors

## Example Workflow

```bash
# 1. Configure your databases
nano config.yaml

# 2. Test with a single table
# Edit config.yaml to specify just one table
python migrate.py

# 3. Review the results
cat output/migration.log
cat output/migration_report_*.txt

# 4. If successful, migrate all tables
# Edit config.yaml to include all tables or leave empty
python migrate.py

# 5. Verify data
# Connect to PostgreSQL and verify row counts, data integrity
```

## Limitations

- Foreign keys are attempted but may fail if referenced tables don't exist yet
- Triggers, stored procedures, and views are not migrated (schema only)
- Identity columns are converted to normal columns (no auto-increment)
- Complex constraints may need manual adjustment
- Partitioned tables are migrated as regular tables

## Support

For issues, questions, or contributions, please refer to the project documentation or contact your database administrator.

## License

This tool is provided as-is for database migration purposes.

