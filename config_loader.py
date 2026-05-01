"""
Configuration Loader
Handles loading and validating configuration from YAML file
"""

import yaml
import logging
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)


def load_config(config_file: str = 'config.yaml') -> Dict[str, Any]:
    """
    Load configuration from YAML file
    
    Args:
        config_file: Path to configuration file
        
    Returns:
        Configuration dictionary
    """
    try:
        config_path = Path(config_file)
        
        if not config_path.exists():
            logger.error(f"Configuration file not found: {config_file}")
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
            
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            
        logger.info(f"Configuration loaded from: {config_file}")
        return config
        
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML configuration: {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise


def validate_config(config: Dict[str, Any]) -> bool:
    """
    Validate configuration
    
    Args:
        config: Configuration dictionary
        
    Returns:
        True if valid, False otherwise
    """
    required_sections = ['source', 'destination', 'directories', 'migration']
    
    for section in required_sections:
        if section not in config:
            logger.error(f"Missing required configuration section: {section}")
            return False
            
    # Validate source configuration
    source = config['source']
    source_type = source.get('type', 'mssql')
    
    if source_type == 'csv':
        # CSV source requires either 'csv_file' (single file) or 'csv_files' (multiple files)
        if 'csv_file' not in source and 'csv_files' not in source:
            logger.error("CSV source requires 'csv_file' or 'csv_files' configuration")
            return False
        
        # If using csv_files (new format), validate it's a list
        if 'csv_files' in source:
            csv_files = source['csv_files']
            if not isinstance(csv_files, list) or len(csv_files) == 0:
                logger.error("CSV source 'csv_files' must be a non-empty list")
                return False
            
            # Validate each file entry
            for entry in csv_files:
                if not isinstance(entry, dict):
                    logger.error("Each csv_files entry must be a dictionary with 'file' and 'table' keys")
                    return False
                if 'file' not in entry or 'table' not in entry:
                    logger.error("Each csv_files entry must have 'file' and 'table' keys")
                    return False
    else:
        # Database sources require host, port, database
        required_source = ['host', 'port', 'database']
        for key in required_source:
            if key not in source:
                logger.error(f"Missing required source configuration: {key}")
                return False
            
    # Validate destination configuration
    destination = config['destination']
    dest_type = destination.get('type', 'postgres')
    
    if dest_type == 'mssql':
        # MSSQL destination requires host, port, database, but not username/password if using Windows auth
        required_dest = ['host', 'port', 'database']
        for key in required_dest:
            if key not in destination:
                logger.error(f"Missing required destination configuration: {key}")
                return False
        
        # Check authentication
        if not destination.get('windows_auth', False):
            if 'username' not in destination or 'password' not in destination:
                logger.error("SQL Server authentication requires username and password")
                return False
    else:
        # Other destinations need host, port, database, username, password
        required_destination = ['host', 'port', 'database', 'username', 'password']
        for key in required_destination:
            if key not in destination:
                logger.error(f"Missing required destination configuration: {key}")
                return False
            
    # Validate directories
    directories = config['directories']
    required_dirs = ['intermediate', 'output']
    for key in required_dirs:
        if key not in directories:
            logger.error(f"Missing required directory configuration: {key}")
            return False
            
    logger.info("Configuration validation passed")
    return True


def setup_logging(config: Dict[str, Any]) -> logging.Logger:
    """
    Setup logging based on configuration
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Configured logger
    """
    log_config = config.get('logging', {})
    
    # Get log level
    log_level_str = log_config.get('level', 'INFO')
    log_level = getattr(logging, log_level_str, logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add console handler
    if log_config.get('log_to_console', True):
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # Add file handler
    if log_config.get('log_to_file', True):
        output_dir = Path(config['directories']['output'])
        output_dir.mkdir(parents=True, exist_ok=True)
        
        log_file = output_dir / log_config.get('log_file', 'migration.log')
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    logger.info("Logging configured successfully")
    return root_logger

