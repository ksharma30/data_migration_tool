# SQL Server to PostgreSQL Migration Tool - Quick Reference

## Quick Start

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Copy and edit configuration**:
   ```bash
   copy config.example.yaml config.yaml
   notepad config.yaml
   ```

3. **Test connections**:
   ```bash
   python test_connections.py
   ```

4. **Run migration**:
   ```bash
   python migrate.py
   ```

## Key Files

| File | Purpose |
|------|---------|
| `migrate.py` | Main entry point - run this to start migration |
| `config.yaml` | Configuration file (create from config.example.yaml) |
| `test_connections.py` | Test database connections before migration |
| `quickstart.bat` | Windows setup script |
| `README.md` | Full documentation |

## Configuration Checklist

In `config.yaml`, update:

- [x] SQL Server connection (host, database, credentials)
- [x] PostgreSQL connection (host, database, credentials)  
- [x] List of tables to migrate (or leave empty for all)
- [x] `drop_if_exists` flag (WARNING: drops existing tables)
- [x] Directory paths if needed

## Directory Structure After Migration

```
sql2pg_copy/
├── intermediate/              # CSV files and SQL scripts
│   └── [database]/
│       └── [table]/
│           ├── data.csv
│           ├── 1_schema_creation.sql
│           └── 2_post_creation.sql
└── output/                    # Logs and reports
    ├── migration.log
    └── migration_report_*.txt
```

## Common Issues

| Issue | Solution |
|-------|----------|
| BCP not found | Install SQL Server Command Line Utilities |
| ODBC driver error | Install ODBC Driver 17 for SQL Server |
| Connection failed | Check host, port, credentials, firewall |
| Row count mismatch | Check logs for encoding/NULL issues |
| Table exists error | Set `drop_if_exists: true` in config |

## Performance Tips

- For large tables (millions of rows):
  - Increase `bcp.batch_size` to 500000+
  - Set `schema.create_indexes: false`
  - Add indexes manually after all data is loaded
  - Set `schema.create_foreign_keys: false`
  - Add foreign keys manually after all tables are migrated

## Migration Steps (per table)

1. ✓ Validate table exists in source
2. ✓ Extract and convert schema
3. ✓ Drop table in target (if configured)
4. ✓ Create table in target
5. ✓ Export data with BCP → CSV
6. ✓ Import data with COPY
7. ✓ Verify row counts
8. ✓ Create indexes
9. ✓ Run VACUUM ANALYZE

## Output Reports

Check these files after migration:
- `output/migration.log` - Detailed logs
- `output/migration_report_*.txt` - Summary report

## Support Commands

```bash
# Test connections only
python test_connections.py

# Run with custom config
python migrate.py my_config.yaml

# Check BCP version
bcp -v

# Check Python packages
pip list | findstr "pyodbc psycopg2 yaml"
```

