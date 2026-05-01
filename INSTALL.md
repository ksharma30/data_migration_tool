# Installation and Setup Guide

## Step 1: Install Dependencies

Open PowerShell or Command Prompt and navigate to the project directory:

```bash
cd G:\sql2pg_copy
```

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Expected output:
```
Successfully installed pyodbc-4.0.39 psycopg2-binary-2.9.9 PyYAML-6.0.1 ...
```

## Step 2: Configure Database Connections

1. **Create your configuration file**:

```bash
copy config.example.yaml config.yaml
```

2. **Edit config.yaml** with your database details:

```bash
notepad config.yaml
```

Update these sections:

### SQL Server (Source)
```yaml
source:
  host: "your-sql-server-host"      # e.g., "localhost" or "192.168.1.100"
  port: 1433
  database: "YourSourceDatabase"    # Database to migrate FROM
  username: "sa"                    # SQL Server username
  password: "YourPassword"          # SQL Server password
  windows_auth: false               # Set to true for Windows Authentication
  driver: "ODBC Driver 17 for SQL Server"
```

### PostgreSQL (Destination)
```yaml
destination:
  host: "your-postgres-host"        # e.g., "localhost" or "192.168.1.101"
  port: 5432
  database: "YourTargetDatabase"    # Database to migrate TO
  username: "postgres"              # PostgreSQL username
  password: "YourPassword"          # PostgreSQL password
  ssl: false                        # Set to true if required by your server
```

### Tables to Migrate
```yaml
migration:
  tables:
    - "table1"    # List specific tables
    - "table2"
    # Or leave empty [] to migrate ALL tables
  
  drop_if_exists: true   # WARNING: Will drop existing tables in PostgreSQL!
```

## Step 3: Test Database Connections

Before running the full migration, test your connections:

```bash
python test_connections.py
```

Expected output:
```
================================================================================
Testing SQL Server Connection
================================================================================
Host: localhost
Port: 1433
Database: MyDatabase

✓ Connection successful!
✓ Found 25 tables in database

First 10 tables:
  - Customers
  - Orders
  - Products
  ...

================================================================================
Testing PostgreSQL Connection
================================================================================
Host: localhost
Port: 5432
Database: MyDatabase_PG
SSL: False

✓ Connection successful!
✓ PostgreSQL version: PostgreSQL 15.2 on x86_64-pc-linux-gnu...

================================================================================
Connection Test Summary
================================================================================
SQL Server: ✓ OK
PostgreSQL: ✓ OK

✓ All connections successful! You can now run the migration.

Run: python migrate.py
```

If you see any errors, check:
- Database host and port are correct
- Credentials are correct
- Firewall allows connections
- PostgreSQL pg_hba.conf allows connections (if remote)
- ODBC Driver is installed for SQL Server

## Step 4: Run Migration

### Option A: Interactive Script (Recommended for first time)

```bash
run_migration.bat
```

This will:
- Check if config.yaml exists
- Optionally test connections
- Ask for confirmation
- Run the migration
- Show results

### Option B: Direct Command

```bash
python migrate.py
```

### Option C: Custom Config File

```bash
python migrate.py path\to\custom_config.yaml
```

## Step 5: Monitor Progress

During migration, you'll see output like:

```
================================================================================
SQL Server to PostgreSQL Migration Starting
================================================================================

[INFO] Connected to SQL Server successfully
[INFO] Connected to PostgreSQL successfully
[INFO] Found 5 tables to migrate: Customers, Orders, Products, OrderDetails, Categories

Migrating table 1/5: Customers
================================================================================
Starting migration for table: Customers
================================================================================
[INFO] Source table has 1250000 rows
[INFO] Migrating schema for table: Customers
[INFO] Schema DDL saved to: sql2pg_copy/intermediate/MyDB/Customers/1_schema_creation.sql
[INFO] Table Customers created successfully in PostgreSQL
[INFO] Exporting data for table: Customers
[INFO] Data exported successfully: ...intermediate/MyDB/Customers/data.csv (45.23 MB)
[INFO] Loading data into table: Customers
[INFO] Successfully loaded 1250000 rows into Customers
[INFO] Target table has 1250000 rows
[INFO] Vacuumed and analyzed Customers
✓ Successfully migrated table Customers

Migrating table 2/5: Orders
...
```

## Step 6: Check Results

### View Migration Log

```bash
type output\migration.log
```

Or open in notepad:
```bash
notepad output\migration.log
```

### View Migration Report

```bash
type output\migration_report_20250105_143022.txt
```

Example report:
```
SQL Server to PostgreSQL Migration Report
================================================================================

Date: 2025-01-05 14:30:22
Source: MyDatabase
Target: MyDatabase_PG

Total tables: 5
  Successful: 5
  Warnings: 0
  Failed: 0

Detailed Results:
--------------------------------------------------------------------------------

Table: Customers
  Status: SUCCESS
  Source rows: 1250000
  Target rows: 1250000
  Duration: 45.23s

Table: Orders
  Status: SUCCESS
  Source rows: 3500000
  Target rows: 3500000
  Duration: 125.67s

...
```

### Check Intermediate Files

For each table, you'll find:

```
intermediate/
└── MyDatabase/
    └── Customers/
        ├── data.csv                 # Exported data
        ├── 1_schema_creation.sql    # CREATE TABLE statement
        └── 2_post_creation.sql      # Indexes and foreign keys
```

You can inspect these files to:
- Review the DDL that was generated
- Check the CSV data format
- Manually run scripts if needed

## Step 7: Verify Data in PostgreSQL

Connect to PostgreSQL and verify:

```sql
-- Check table exists
\dt

-- Check row count
SELECT COUNT(*) FROM customers;

-- Check data sample
SELECT * FROM customers LIMIT 10;

-- Check indexes
\d customers
```

## Troubleshooting

### Error: BCP not found

**Solution**: Install SQL Server Command Line Utilities
- Download from: https://learn.microsoft.com/en-us/sql/tools/bcp-utility
- Add to system PATH
- Verify with: `bcp -v`

### Error: ODBC Driver not found

**Solution**: Install ODBC Driver for SQL Server
- Download from: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
- Update `driver` in config.yaml to match installed version

### Error: Connection failed to PostgreSQL

**Solution**: Check SSL settings
- If you see "no pg_hba.conf entry" error
- Try setting `ssl: true` in config.yaml
- Or update PostgreSQL pg_hba.conf to allow connections

### Warning: Row count mismatch

**Possible causes**:
- Data was modified during migration
- Check migration.log for details
- Verify NULL handling in data

### Error: Table already exists

**Solution**: Set `drop_if_exists: true` in config.yaml

## Best Practices

### Before Migration
1. ✓ Test connections with `test_connections.py`
2. ✓ Start with a small subset of tables
3. ✓ Backup PostgreSQL database
4. ✓ Ensure sufficient disk space (CSV files can be large)
5. ✓ Schedule during maintenance window for large databases

### During Migration
1. ✓ Monitor disk space in intermediate directory
2. ✓ Watch migration.log for errors
3. ✓ Don't interrupt the process (each table is transactional)

### After Migration
1. ✓ Review migration report
2. ✓ Verify row counts in PostgreSQL
3. ✓ Test application with new database
4. ✓ Add foreign keys if not done during migration
5. ✓ Run ANALYZE on all tables
6. ✓ Update sequences if needed (for identity columns)

### For Large Tables (Millions of Rows)
1. ✓ Increase batch_size in config.yaml (e.g., 500000)
2. ✓ Set `create_indexes: false` during initial load
3. ✓ Create indexes manually after all data is loaded
4. ✓ Set `create_foreign_keys: false`
5. ✓ Add foreign keys manually after all tables are migrated
6. ✓ Monitor PostgreSQL WAL and temp space

## Configuration Tips

### Migrate All Tables
```yaml
migration:
  tables: []  # Empty list = all tables
```

### Migrate Specific Tables
```yaml
migration:
  tables:
    - "Customers"
    - "Orders"
    - "Products"
```

### Exclude Certain Tables
```yaml
migration:
  exclude_tables:
    - "sysdiagrams"
    - "temp_table"
```

### Performance Tuning
```yaml
migration:
  bcp:
    batch_size: 500000  # Increase for large tables

performance:
  bcp_timeout: 7200     # 2 hours for very large tables
  copy_timeout: 7200
```

### Defer Indexes and FKs
```yaml
migration:
  schema:
    create_indexes: false        # Create manually later
    create_foreign_keys: false   # Add after all tables migrated
```

## Next Steps

After successful migration:

1. **Update application connection strings** to use PostgreSQL
2. **Test application functionality** thoroughly
3. **Migrate stored procedures** (manual process)
4. **Migrate views** (manual process)
5. **Update sequences** for auto-increment columns if needed
6. **Set up regular backups** for PostgreSQL
7. **Monitor performance** and optimize as needed

## Getting Help

If you encounter issues:

1. Check `output/migration.log` for detailed error messages
2. Review README.md for comprehensive documentation
3. Check examples.py for code samples
4. Verify your configuration against config.example.yaml

## Quick Command Reference

```bash
# Install dependencies
pip install -r requirements.txt

# Create config from example
copy config.example.yaml config.yaml

# Test connections
python test_connections.py

# Run migration (interactive)
run_migration.bat

# Run migration (direct)
python migrate.py

# Run with custom config
python migrate.py my_config.yaml

# Check BCP version
bcp -v

# View migration log
type output\migration.log

# View latest report
dir output\migration_report_*.txt /od
```

## Complete Example

Here's a complete example from start to finish:

```powershell
# 1. Navigate to project
cd G:\sql2pg_copy

# 2. Install dependencies
pip install -r requirements.txt

# 3. Create config
copy config.example.yaml config.yaml
notepad config.yaml
# Edit: Update source and destination database settings

# 4. Test connections
python test_connections.py
# Output: ✓ All connections successful!

# 5. Run migration
python migrate.py
# Wait for completion...

# 6. Check results
type output\migration_report_20250105_143022.txt
# Output shows: 5 tables migrated successfully

# 7. Verify in PostgreSQL
psql -h localhost -U postgres -d MyDatabase_PG
# \dt                           -- List tables
# SELECT COUNT(*) FROM customers;  -- Verify data

# Done! 🎉
```

That's it! You're now ready to migrate your databases from SQL Server to PostgreSQL.

