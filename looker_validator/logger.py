"""
Enhanced logging configuration for looker-validator.
"""

import logging
import os
import sys
from pathlib import Path
from typing import Optional

import colorama

# Initialize colorama for cross-platform color support
colorama.init()

# Define color constants
COLORS = {
    "red": colorama.Fore.RED,
    "green": colorama.Fore.GREEN,
    "yellow": colorama.Fore.YELLOW,
    "cyan": colorama.Fore.CYAN,
    "blue": colorama.Fore.BLUE,
    "magenta": colorama.Fore.MAGENTA,
    "white": colorama.Fore.WHITE,
    "bold": colorama.Style.BRIGHT,
    "dim": colorama.Style.DIM,
    "reset": colorama.Style.RESET_ALL,
}

LOG_FILENAME = "looker_validator.log"


class ColoredFormatter(logging.Formatter):
    """Formatter that adds colors to log messages."""
    
    COLORS = {
        "DEBUG": COLORS["blue"],
        "INFO": COLORS["green"],
        "WARNING": COLORS["yellow"],
        "ERROR": COLORS["red"],
        "CRITICAL": COLORS["red"] + COLORS["bold"]
    }
    
    def format(self, record: logging.LogRecord) -> str:
        """Format the record with colors."""
        # Skip colors if NO_COLOR is set
        if os.environ.get('NO_COLOR') or os.environ.get('TERM') == 'dumb':
            return super().format(record)
        
        # Add color based on log level
        levelname = record.levelname
        color = self.COLORS.get(levelname, '')
        
        # Format the message
        message = super().format(record)
        
        # Add color to the message
        if color:
            reset = COLORS["reset"]
            message = f"{color}{message}{reset}"
        
        return message


class FileFormatter(logging.Formatter):
    """Formatter for file logs without color codes."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format the record without colors."""
        message = super().format(record)
        return self._strip_color_codes(message)
    
    def _strip_color_codes(self, text: str) -> str:
        """Remove ANSI color codes from text."""
        for escape_sequence in COLORS.values():
            text = text.replace(escape_sequence, "")
        return text


def setup_logger(
    name: str = "looker_validator",
    level: int = logging.INFO,
    log_dir: Optional[str] = None
) -> logging.Logger:
    """Set up a logger with console and file handlers.
    
    Args:
        name: Logger name
        level: Logging level
        log_dir: Directory for log files
        
    Returns:
        Configured logger
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ColoredFormatter("%(message)s"))
    console_handler.setLevel(level)
    logger.addHandler(console_handler)
    
    # Create file handler if log_dir is provided
    if log_dir:
        log_dir_path = Path(log_dir)
        log_dir_path.mkdir(exist_ok=True)
        
        # Create subfolder for SQL query logs
        (log_dir_path / "queries").mkdir(exist_ok=True)
        
        file_handler = logging.FileHandler(
            log_dir_path / LOG_FILENAME,
            encoding="utf-8"
        )
        file_handler.setFormatter(
            FileFormatter("%(asctime)s %(levelname)s | %(message)s")
        )
        file_handler.setLevel(logging.DEBUG)  # Always log DEBUG to file
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str = "looker_validator") -> logging.Logger:
    """Get an existing logger.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def log_sql_error(
    model: str,
    explore: str,
    sql: str,
    log_dir: str,
    dimension: Optional[str] = None
) -> Path:
    """Save failing SQL to a file.
    
    Args:
        model: Model name
        explore: Explore name
        sql: SQL to save
        log_dir: Directory for logs
        dimension: Optional dimension name
        
    Returns:
        Path to the SQL file
    """
    # Create a filename based on the model/explore/dimension
    file_name = f"{model}__{explore}"
    if dimension:
        file_name += f"__{dimension}"
    file_name = file_name.replace(".", "_") + ".sql"
    
    # Create the file path
    file_path = Path(log_dir) / "queries" / file_name
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    # Write SQL to file
    with open(file_path, "w") as f:
        f.write(sql)
    
    return file_path