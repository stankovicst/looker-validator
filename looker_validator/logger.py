# looker_validator/logger.py
"""
Logging configuration for Looker Validator.
Simplified to focus *only* on file logging setup.
Console level and output formatting are handled by cli.py and printer.py (Rich).
"""

import os
import logging
import logging.handlers
from typing import Optional
from pathlib import Path

# Default log file name
LOG_FILENAME = "looker_validator.log"

def setup_file_logging(
    log_dir: str,
    log_level: int = logging.DEBUG, # Default file logging level to DEBUG
    logger_name: str = "looker_validator" # Root logger name for the app
) -> bool:
    """
    Sets up a rotating file handler for the application's logger.

    Assumes the root logger's level has already been set (e.g., in cli.py).
    This function *adds* the file handler to the specified logger.

    Args:
        log_dir: Directory where the log file should be stored.
        log_level: The logging level for the file handler (e.g., logging.DEBUG).
        logger_name: The name of the logger to attach the handler to (usually the root app logger).

    Returns:
        True if file logging was set up successfully, False otherwise.
    """
    # Get the specified logger instance (or root if name matches default)
    # Using getLogger without args gets the root logger if name is root logger name
    logger = logging.getLogger(logger_name if logger_name != logging.getLogger().name else None)

    # Define log file path
    try:
        log_path = Path(log_dir).resolve()
        log_file = log_path / LOG_FILENAME
    except Exception as e:
        # Use root logger temporarily if app logger failed init? Or just print.
        logging.error(f"Invalid log directory path '{log_dir}': {e}. File logging disabled.")
        return False


    # --- Check if a handler for this exact file already exists ---
    handler_exists = False
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
             # Resolve paths to handle potential relative paths consistently
            try:
                 if Path(handler.baseFilename).resolve() == log_file.resolve():
                     handler_exists = True
                     logger.debug(f"File handler for '{log_file}' already exists.")
                     # Ensure level is set correctly if handler already exists
                     handler.setLevel(log_level)
                     break
            except Exception as e:
                 logger.warning(f"Could not compare handler path '{getattr(handler, 'baseFilename', 'N/A')}' with target '{log_file}': {e}")

    if handler_exists:
        return True # Already configured

    # --- Create log directory if it doesn't exist ---
    try:
        log_path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.error(f"Failed to create log directory '{log_path}': {e}. File logging disabled.", exc_info=True)
        return False

    # --- Create and configure the file handler ---
    try:
        # Use RotatingFileHandler to prevent excessively large log files
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB per file
            backupCount=5,             # Keep 5 backup logs
            encoding='utf-8'           # Explicitly set encoding
        )

        # Set formatter for the file handler (more detailed than console)
        file_formatter = logging.Formatter(
            # Include timestamp, level, logger name, line number, and message
            "%(asctime)s [%(levelname)-7s] %(name)s:%(lineno)d - %(message)s"
        )
        file_handler.setFormatter(file_formatter)

        # Set the logging level for this specific handler
        file_handler.setLevel(log_level)

        # Add the handler to the logger
        logger.addHandler(file_handler)
        # Use logger.info here, assuming root logger level is set appropriately in cli.py
        logger.info(f"File logging configured: Level={logging.getLevelName(log_level)}, Path='{log_file}'")
        return True

    except Exception as e:
        # Catch any other errors during handler setup
        logger.error(f"Failed to set up file logging to '{log_file}': {e}", exc_info=True)
        return False


# Removed setup_logger (functionality split between cli.py and setup_file_logging here)
# Removed get_colored_logger and ColoredFormatter (console handled by rich via printer.py)

