"""
Logging configuration for Looker Validator.
"""

import os
import logging
import logging.handlers
from typing import Optional


def setup_logger(
    name: str = "looker_validator",
    log_dir: str = "logs",
    verbose: bool = False
) -> logging.Logger:
    """Set up a logger with console and file handlers.

    Args:
        name: Name of the logger
        log_dir: Directory for log files
        verbose: Whether to enable verbose logging

    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    
    # Skip if already configured
    if logger.handlers:
        return logger
    
    # Set level based on verbose flag
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    
    # Create formatters
    console_formatter = logging.Formatter(
        "%(message)s"
    )
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)  # Always INFO for console
    
    # File handler
    try:
        os.makedirs(log_dir, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            os.path.join(log_dir, f"{name}.log"),
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5
        )
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)  # Always DEBUG for file
        
        # Add handlers to logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
    except Exception as e:
        # If file logging fails, just use console
        console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
        logger.addHandler(console_handler)
        logger.warning(f"Failed to set up file logging: {str(e)}")
    
    return logger


def get_colored_logger(log_dir: str = "logs", verbose: bool = False) -> logging.Logger:
    """Get a logger with colored output for the console.

    Args:
        log_dir: Directory for log files
        verbose: Whether to enable verbose logging

    Returns:
        Configured logger instance with colored console output
    """
    try:
        # Try to import colorama for cross-platform color support
        from colorama import init, Fore, Style
        init()
        
        class ColoredFormatter(logging.Formatter):
            """Custom formatter for colored console output."""
            
            COLORS = {
                'DEBUG': Fore.BLUE,
                'INFO': Fore.GREEN,
                'WARNING': Fore.YELLOW,
                'ERROR': Fore.RED,
                'CRITICAL': Fore.RED + Style.BRIGHT
            }
            
            def format(self, record):
                """Format the record with colors."""
                # Check if NO_COLOR environment variable is set
                if os.environ.get('NO_COLOR'):
                    return super().format(record)
                
                # Add color based on log level
                levelname = record.levelname
                color = self.COLORS.get(levelname, '')
                
                # Format the message
                message = super().format(record)
                
                # Add color to the message
                if color:
                    reset = Style.RESET_ALL
                    message = f"{color}{message}{reset}"
                
                return message
        
        # Create logger
        logger = logging.getLogger("looker_validator")
        
        # Skip if already configured
        if logger.handlers:
            return logger
        
        # Set level based on verbose flag
        logger.setLevel(logging.DEBUG if verbose else logging.INFO)
        
        # Create formatters
        console_formatter = ColoredFormatter("%(message)s")
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)  # Always INFO for console
        
        # File handler
        try:
            os.makedirs(log_dir, exist_ok=True)
            file_handler = logging.handlers.RotatingFileHandler(
                os.path.join(log_dir, "looker_validator.log"),
                maxBytes=10 * 1024 * 1024,  # 10 MB
                backupCount=5
            )
            file_handler.setFormatter(file_formatter)
            file_handler.setLevel(logging.DEBUG)  # Always DEBUG for file
            
            # Add handlers to logger
            logger.addHandler(console_handler)
            logger.addHandler(file_handler)
        except Exception as e:
            # If file logging fails, just use console
            console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
            logger.addHandler(console_handler)
            logger.warning(f"Failed to set up file logging: {str(e)}")
        
        return logger
        
    except ImportError:
        # Fall back to regular logger if colorama isn't available
        return setup_logger(log_dir=log_dir, verbose=verbose)