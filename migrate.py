#!/usr/bin/env python
"""
SQL Server to PostgreSQL Migration Tool
Main entry point with Textual TUI support

Usage:
    python migrate.py [config_file]
    python migrate.py --interactive
    
    config_file: Path to configuration file (default: config.yaml)
    --interactive: Force interactive TUI mode
"""

import sys
import logging
from pathlib import Path
import io

# Fix encoding issues on Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from config_loader import load_config, validate_config, setup_logging
from unified_processor import UnifiedMigrationProcessor
from tui_app import check_or_create_config

logger = logging.getLogger(__name__)


def print_banner():
    """Print application banner"""
    banner = """
╔═══════════════════════════════════════════════════════════════════════════╗
║                                                                           ║
║          SQL2PG Copy - Unified Migration Tool                            ║
║                                                                           ║
║          BCP | PostgreSQL COPY | Flatfile | GeoPackage                   ║
║                                                                           ║
╚═══════════════════════════════════════════════════════════════════════════╝
"""
    print(banner)


def main():
    """Main entry point"""
    try:
        print_banner()
        
        # Parse arguments
        interactive = '--interactive' in sys.argv
        config_file = 'config.yaml'
        
        for arg in sys.argv[1:]:
            if arg != '--interactive' and not arg.startswith('-'):
                config_file = arg
                break
        
        # Check if config exists or show TUI
        config_path = Path(config_file)
        config = None
        
        if not config_path.exists() or interactive:
            print("Configuration file not found or interactive mode requested.")
            print("Starting interactive configuration builder...\n")
            
            result = check_or_create_config(config_file, interactive)
            
            if result['action'] == 'cancel':
                print("Configuration cancelled.")
                sys.exit(0)
            elif result['action'] == 'error':
                print(f"Error: {result.get('error', 'Unknown error')}")
                sys.exit(1)
            elif result['action'] in ('save', 'run', 'use_existing'):
                config = result['config']
                if result['action'] == 'save':
                    print(f"\nConfiguration saved to: {config_file}")
                    print("Run 'python migrate.py' to start migration.")
                    sys.exit(0)
        else:
            print(f"Loading configuration from: {config_file}\n")
            config = load_config(config_file)
        
        # Setup logging
        setup_logging(config)
        
        logger.info("=" * 80)
        logger.info("SQL2PG Copy - Unified Migration Tool")
        logger.info("=" * 80)
        
        # Validate configuration
        if not validate_config(config):
            logger.error("Configuration validation failed")
            sys.exit(1)
            
        # Create and run unified migration processor
        processor = UnifiedMigrationProcessor(config)
        processor.run()
        
        # Exit with appropriate code
        if processor.overall_status == "SUCCESS":
            logger.info("Migration completed successfully!")
            sys.exit(0)
        elif processor.overall_status == "WARNING":
            logger.warning("Migration completed with warnings!")
            sys.exit(0)
        else:
            logger.error("Migration failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nMigration interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()

