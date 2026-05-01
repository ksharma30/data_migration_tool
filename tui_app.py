"""
Textual TUI for interactive configuration
Shown when config.yaml is missing or --interactive flag is used
"""

import yaml
import logging
from pathlib import Path
from typing import Dict, Any, List
from textual.app import App, ComposeResult
from textual.containers import Container, Vertical, Horizontal, ScrollableContainer
from textual.widgets import Header, Footer, Static, Button, Input, Select, Checkbox, Label, RadioButton, RadioSet
from textual.binding import Binding
from textual.screen import Screen

logger = logging.getLogger(__name__)


class ConfigBuilderScreen(Screen):
    """Main configuration builder screen"""
    
    BINDINGS = [
        Binding("ctrl+s", "save", "Save Config"),
        Binding("ctrl+q", "quit", "Quit"),
    ]
    
    def __init__(self):
        super().__init__()
        self.config = {
            'source': {},
            'destination': {},
            'migration': {
                'tables': [],
                'exclude_tables': [],
                'bcp': {},
                'copy': {},
                'schema': {}
            },
            'directories': {
                'intermediate': 'sql2pg_copy/intermediate',
                'output': 'sql2pg_copy/output'
            },
            'logging': {'level': 'INFO'},
            'performance': {}
        }
        
    def compose(self) -> ComposeResult:
        """Create child widgets"""
        yield Header()
        
        with ScrollableContainer():
            yield Static("SQL2PG Copy - Configuration Builder", classes="title")
            yield Static("", classes="spacer")
            
            # Source configuration
            yield Static("Source Database", classes="section-title")
            with Container(classes="config-section"):
                yield Label("Type:")
                yield Select(
                    [("SQL Server", "mssql"), ("PostgreSQL", "postgres"), 
                     ("Flatfile", "flatfile"), ("GeoPackage", "gpkg")],
                    id="source_type",
                    value="mssql"
                )
                
                yield Label("Host:")
                yield Input(placeholder="localhost", id="source_host")
                
                yield Label("Port:")
                yield Input(placeholder="1433", id="source_port")
                
                yield Label("Database:")
                yield Input(placeholder="YourSourceDB", id="source_database")
                
                yield Label("Username:")
                yield Input(placeholder="sa", id="source_username")
                
                yield Label("Password:")
                yield Input(placeholder="", password=True, id="source_password")
            
            yield Static("", classes="spacer")
            
            # Destination configuration
            yield Static("Destination Database", classes="section-title")
            with Container(classes="config-section"):
                yield Label("Type:")
                yield Select(
                    [("PostgreSQL", "postgres"), ("Flatfile", "flatfile"), 
                     ("GeoPackage", "gpkg")],
                    id="dest_type",
                    value="postgres"
                )
                
                yield Label("Host:")
                yield Input(placeholder="localhost", id="dest_host")
                
                yield Label("Port:")
                yield Input(placeholder="5432", id="dest_port")
                
                yield Label("Database:")
                yield Input(placeholder="YourTargetDB", id="dest_database")
                
                yield Label("Username:")
                yield Input(placeholder="postgres", id="dest_username")
                
                yield Label("Password:")
                yield Input(placeholder="", password=True, id="dest_password")
            
            yield Static("", classes="spacer")
            
            # Migration options
            yield Static("Migration Options", classes="section-title")
            with Container(classes="config-section"):
                yield Label("Tables (comma-separated, empty for all):")
                yield Input(placeholder="table1,table2", id="tables")
                
                yield Label("Flatfile Separator:")
                yield Select(
                    [("Comma (CSV)", ","), ("Tab (TSV)", "\t"), 
                     ("Pipe", "|"), ("Semicolon", ";")],
                    id="separator",
                    value=","
                )
                
                yield Checkbox("Drop tables if exist", id="drop_if_exists", value=True)
                yield Checkbox("Create indexes", id="create_indexes", value=True)
                yield Checkbox("Create foreign keys", id="create_foreign_keys", value=False)
            
            yield Static("", classes="spacer")
            
            # Action buttons
            with Horizontal(classes="button-row"):
                yield Button("Save Config", variant="primary", id="save_btn")
                yield Button("Save & Run", variant="success", id="save_run_btn")
                yield Button("Cancel", variant="error", id="cancel_btn")
        
        yield Footer()
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses"""
        if event.button.id == "save_btn":
            self.action_save()
        elif event.button.id == "save_run_btn":
            self.action_save_and_run()
        elif event.button.id == "cancel_btn":
            self.app.exit()
    
    def action_save(self):
        """Save configuration to file"""
        self._build_config()
        config_path = Path("config.yaml")
        
        try:
            with open(config_path, 'w', encoding='utf-8') as f:
                yaml.dump(self.config, f, default_flow_style=False, sort_keys=False)
            
            self.app.exit(result={'action': 'save', 'config': self.config})
            
        except Exception as e:
            logger.error(f"Failed to save config: {e}")
            self.app.exit(result={'action': 'error', 'error': str(e)})
    
    def action_save_and_run(self):
        """Save configuration and start migration"""
        self._build_config()
        config_path = Path("config.yaml")
        
        try:
            with open(config_path, 'w', encoding='utf-8') as f:
                yaml.dump(self.config, f, default_flow_style=False, sort_keys=False)
            
            self.app.exit(result={'action': 'run', 'config': self.config})
            
        except Exception as e:
            logger.error(f"Failed to save config: {e}")
            self.app.exit(result={'action': 'error', 'error': str(e)})
    
    def action_quit(self):
        """Quit without saving"""
        self.app.exit(result={'action': 'cancel'})
    
    def _build_config(self):
        """Build configuration from form inputs"""
        # Source
        source_type = self.query_one("#source_type", Select).value
        self.config['source'] = {
            'type': source_type,
            'host': self.query_one("#source_host", Input).value or "localhost",
            'port': int(self.query_one("#source_port", Input).value or "1433"),
            'database': self.query_one("#source_database", Input).value or "YourSourceDB",
            'username': self.query_one("#source_username", Input).value or "sa",
            'password': self.query_one("#source_password", Input).value or "",
        }
        
        if source_type == 'mssql':
            self.config['source']['windows_auth'] = False
            self.config['source']['driver'] = "ODBC Driver 17 for SQL Server"
        
        # Destination
        dest_type = self.query_one("#dest_type", Select).value
        self.config['destination'] = {
            'type': dest_type,
            'host': self.query_one("#dest_host", Input).value or "localhost",
            'port': int(self.query_one("#dest_port", Input).value or "5432"),
            'database': self.query_one("#dest_database", Input).value or "YourTargetDB",
            'username': self.query_one("#dest_username", Input).value or "postgres",
            'password': self.query_one("#dest_password", Input).value or "",
        }
        
        if dest_type == 'postgres':
            self.config['destination']['ssl'] = False
        
        # Migration options
        tables_input = self.query_one("#tables", Input).value
        if tables_input:
            self.config['migration']['tables'] = [t.strip() for t in tables_input.split(',')]
        else:
            self.config['migration']['tables'] = []
        
        separator = self.query_one("#separator", Select).value
        self.config['migration']['drop_if_exists'] = self.query_one("#drop_if_exists", Checkbox).value
        
        # BCP settings
        self.config['migration']['bcp'] = {
            'field_delimiter': separator,
            'row_delimiter': "\\n",
            'text_qualifier': '"',
            'batch_size': 100000,
            'code_page': "65001"
        }
        
        # COPY settings
        self.config['migration']['copy'] = {
            'format': 'csv',
            'header': True,
            'delimiter': separator,
            'quote': '"',
            'escape': '"',
            'null': ""
        }
        
        # Schema settings
        self.config['migration']['schema'] = {
            'create_indexes': self.query_one("#create_indexes", Checkbox).value,
            'create_foreign_keys': self.query_one("#create_foreign_keys", Checkbox).value,
            'create_primary_keys': True,
            'create_unique_constraints': True,
            'create_check_constraints': True
        }
        
        # Performance
        self.config['performance'] = {
            'parallel_tables': 1,
            'bcp_timeout': 3600,
            'copy_timeout': 3600
        }
        
        # Logging
        self.config['logging'] = {
            'level': 'INFO',
            'log_to_file': True,
            'log_file': 'migration.log',
            'log_to_console': True
        }


class ConfigBuilderApp(App):
    """Textual app for building configuration"""
    
    CSS = """
    .title {
        text-align: center;
        text-style: bold;
        color: $accent;
        padding: 1;
    }
    
    .section-title {
        text-style: bold;
        color: $secondary;
        padding: 1 0;
    }
    
    .config-section {
        border: solid $primary;
        padding: 1;
        margin: 1 2;
    }
    
    .spacer {
        height: 1;
    }
    
    .button-row {
        align: center middle;
        padding: 2;
    }
    
    Button {
        margin: 0 1;
    }
    
    Label {
        padding: 0 0 0 1;
    }
    
    Input, Select {
        margin: 0 1 1 1;
    }
    """
    
    def on_mount(self) -> None:
        """Set up the app"""
        self.title = "SQL2PG Copy - Configuration Builder"
        self.sub_title = "Interactive Setup"
    
    def compose(self) -> ComposeResult:
        """Create the screen"""
        yield ConfigBuilderScreen()


def run_tui() -> Dict[str, Any]:
    """
    Run the TUI and return the result
    
    Returns:
        Dictionary with 'action' and optionally 'config'
    """
    app = ConfigBuilderApp()
    result = app.run()
    return result or {'action': 'cancel'}


def check_or_create_config(config_path: str = 'config.yaml', 
                           interactive: bool = False) -> Dict[str, Any]:
    """
    Check if config exists, if not show TUI
    
    Args:
        config_path: Path to config file
        interactive: Force interactive mode
        
    Returns:
        Configuration dictionary and action
    """
    config_file = Path(config_path)
    
    # If config exists and not forcing interactive, load it
    if config_file.exists() and not interactive:
        logger.info(f"Loading existing config: {config_path}")
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return {'action': 'use_existing', 'config': config}
    
    # Show TUI
    logger.info("Starting interactive configuration...")
    result = run_tui()
    
    return result

