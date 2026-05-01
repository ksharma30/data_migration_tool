@echo off
REM SQL Server to PostgreSQL Migration Tool
REM Main execution script

setlocal EnableDelayedExpansion

echo.
echo ╔═══════════════════════════════════════════════════════════════════════════╗
echo ║                                                                           ║
echo ║          SQL Server to PostgreSQL Migration Tool                         ║
echo ║                                                                           ║
echo ║          Fast BCP export + PostgreSQL COPY import                        ║
echo ║                                                                           ║
echo ╚═══════════════════════════════════════════════════════════════════════════╝
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8 or later from https://www.python.org/
    pause
    exit /b 1
)

REM Check if config.yaml exists
if not exist "config.yaml" (
    echo ERROR: config.yaml not found!
    echo.
    echo Please create config.yaml from config.example.yaml:
    echo   1. copy config.example.yaml config.yaml
    echo   2. Edit config.yaml with your database connection details
    echo   3. Run this script again
    echo.
    pause
    exit /b 1
)

echo [INFO] Configuration file found: config.yaml
echo.

REM Ask user if they want to test connections first
set /p TEST_CONN="Test database connections first? (Y/N): "
if /i "%TEST_CONN%"=="Y" (
    echo.
    echo Testing connections...
    python test_connections.py
    if errorlevel 1 (
        echo.
        echo [WARNING] Connection test failed!
        set /p CONTINUE="Continue with migration anyway? (Y/N): "
        if /i not "!CONTINUE!"=="Y" (
            echo Migration cancelled.
            pause
            exit /b 1
        )
    )
    echo.
)

REM Confirm migration
echo.
echo ╔═══════════════════════════════════════════════════════════════════════════╗
echo ║                         READY TO START MIGRATION                          ║
echo ╚═══════════════════════════════════════════════════════════════════════════╝
echo.
echo This will:
echo   1. Connect to SQL Server and PostgreSQL
echo   2. Extract schema from SQL Server tables
echo   3. Create tables in PostgreSQL
echo   4. Export data using BCP to CSV files
echo   5. Import data using PostgreSQL COPY command
echo   6. Create indexes and constraints
echo   7. Verify row counts
echo.
set /p CONFIRM="Start migration now? (Y/N): "
if /i not "%CONFIRM%"=="Y" (
    echo Migration cancelled.
    pause
    exit /b 0
)

echo.
echo ╔═══════════════════════════════════════════════════════════════════════════╗
echo ║                         MIGRATION IN PROGRESS                             ║
echo ╚═══════════════════════════════════════════════════════════════════════════╝
echo.

REM Run migration with custom config if provided as argument
if "%~1"=="" (
    python migrate.py
) else (
    python migrate.py "%~1"
)

set MIGRATION_EXIT_CODE=%ERRORLEVEL%

echo.
echo ╔═══════════════════════════════════════════════════════════════════════════╗
echo ║                         MIGRATION COMPLETED                               ║
echo ╚═══════════════════════════════════════════════════════════════════════════╝
echo.

if %MIGRATION_EXIT_CODE% EQU 0 (
    echo [SUCCESS] Migration completed successfully!
    echo.
    echo Check the following files for details:
    echo   - output\migration.log - Detailed log file
    echo   - output\migration_report_*.txt - Summary report
    echo   - intermediate\ - CSV and SQL files
) else (
    echo [ERROR] Migration failed with exit code: %MIGRATION_EXIT_CODE%
    echo.
    echo Please check:
    echo   - output\migration.log for error details
    echo   - Database connection settings in config.yaml
    echo   - SQL Server and PostgreSQL server status
)

echo.
pause
exit /b %MIGRATION_EXIT_CODE%

