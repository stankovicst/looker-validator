# looker_validator/utils/helpers.py
"""
Helper utilities for Looker Validator.
Simplified version: Removed functions made redundant by BaseValidator or printer.
Improved error handling for file operations.
"""

import os
import re
import json
import time
import logging
from typing import Dict, List, Any, Optional, Tuple, Set, Union
from pathlib import Path # Use pathlib for path operations

logger = logging.getLogger(__name__)

# --- Potentially Useful Helper Functions ---

def format_duration_for_display(seconds: float) -> str:
    """Format a duration (< 0.1s -> ms, < 60s -> s, < 3600s -> Xm Ys, >= 3600s -> Xh Ym)."""
    if seconds < 0.1:
        # Show 0 decimal places for < 100ms
        return f"{seconds * 1000:.0f}ms"
    elif seconds < 1:
        # Show 0 decimal places for >= 100ms and < 1s
        return f"{seconds * 1000:.0f}ms"
    elif seconds < 60:
        # Show 1 decimal place for seconds
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"

def check_spectacles_ignore(sql: Optional[str], tags: Optional[List[str]]) -> bool:
    """Check if a dimension/measure has Spectacles ignore comment/tag.

    Args:
        sql: SQL string for the dimension/measure.
        tags: List of LookML tags for the dimension/measure.

    Returns:
        True if the field should be ignored based on Spectacles convention.
    """
    # Check for spectacles: ignore tag (case-insensitive)
    if tags and any("spectacles: ignore" in tag.lower() for tag in tags):
        logger.debug("Field ignored due to 'spectacles: ignore' tag.")
        return True

    # Check for -- spectacles: ignore comment in SQL (case-insensitive)
    if sql and re.search(r'--\s*spectacles:\s*ignore', sql, re.IGNORECASE):
        logger.debug("Field ignored due to '-- spectacles: ignore' comment.")
        return True

    return False

def save_json_file(data: Any, file_path: Union[str, Path], indent: int = 2) -> bool:
    """Save data to a JSON file with improved error handling and logging.

    Args:
        data: Data structure to save (must be JSON serializable).
        file_path: Path object or string path to save the file.
        indent: Indentation level for JSON formatting.

    Returns:
        True if saved successfully, False otherwise.
    """
    try:
        path = Path(file_path).resolve() # Resolve to absolute path for clarity
        # Create directory if it doesn't exist
        path.parent.mkdir(parents=True, exist_ok=True)

        # Save to file with utf-8 encoding
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, ensure_ascii=False, sort_keys=True) # Add sort_keys
        logger.info(f"Successfully saved JSON data to: {path}")
        return True
    except (IOError, OSError) as e:
        logger.error(f"I/O error saving JSON file to '{file_path}': {e}", exc_info=True)
        return False
    except (TypeError, ValueError) as e: # Catches JSON serialization errors
        logger.error(f"Data serialization error saving JSON file to '{file_path}': {e}", exc_info=True)
        return False
    except Exception as e: # Catch other potential errors
        logger.error(f"Unexpected error saving JSON file to '{file_path}': {e}", exc_info=True)
        return False


def load_json_file(file_path: Union[str, Path], default: Any = None) -> Any:
    """Load data from a JSON file with improved error handling and logging.

    Args:
        file_path: Path object or string path to the JSON file.
        default: Default value if file doesn't exist or can't be loaded/parsed.

    Returns:
        Loaded data or default value.
    """
    try:
        path = Path(file_path).resolve()
        if not path.is_file():
            logger.debug(f"JSON file not found: {path}, returning default.")
            return default

        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.debug(f"Successfully loaded JSON data from: {path}")
        return data
    except (IOError, OSError) as e:
        logger.warning(f"I/O error loading JSON file '{file_path}': {e}. Returning default.")
        return default
    except json.JSONDecodeError as e:
        logger.warning(f"JSON decode error in file '{file_path}': {e}. Returning default.")
        return default
    except Exception as e: # Catch other potential errors
        logger.warning(f"Unexpected error loading JSON file '{file_path}': {e}. Returning default.", exc_info=True)
        return default


def create_explore_url(base_url: str, model: str, explore: str) -> str:
    """Create a URL to an explore page in the Looker UI."""
    if not base_url: # Handle case where base_url might be None/empty
        return "#"
    # Ensure base_url doesn't end with a slash
    clean_base_url = base_url.rstrip('/')
    # Basic URL structure
    return f"{clean_base_url}/explore/{model}/{explore}"


def extract_filename_from_path(file_path: Optional[str]) -> str:
    """Extract the filename from a file path using pathlib, handling potential errors."""
    if not file_path:
        return "Unknown file"
    try:
        # Use pathlib for robust path handling
        return Path(file_path).name
    except Exception as e: # Catch potential errors with invalid path formats
        logger.warning(f"Could not extract filename from path '{file_path}': {e}")
        return "Invalid path"


def extract_looker_error(error_message: Optional[str]) -> str:
    """Attempt to extract a more concise error message from common Looker API error patterns."""
    if not error_message:
        return "Unknown error"

    # Try common patterns (add more as needed)
    # Added LookML pattern, DB patterns
    patterns = [
        r"SQL ERROR:\s*(.*?)(?:\\n|\n|$)", # SQL Error
        r"Syntax error:\s*(.*?)(?:\\n|\n|$)", # Syntax Error
        r"Invalid field reference\s*`(.*?)`", # Invalid field
        r"Unknown parameter\s*'(.*?)'", # Unknown LookML parameter
        r"Unknown view\s*\"(.*?)\"", # Unknown view
        r"Unknown model\s*\"(.*?)\"", # Unknown model
        r"Could not find relation\s*\"(.*?)\"", # Database relation not found
        r"Permission denied for relation\s*\"(.*?)\"", # DB Permission denied
        r"Invalid argument\(s\):\s*(.*?)(?:\\n|\n|$)", # Generic invalid argument
        r"Element \w+ is not allowed", # LookML structure error
    ]

    for pattern in patterns:
        match = re.search(pattern, error_message, re.IGNORECASE | re.DOTALL)
        if match:
            extracted = match.group(1).strip().strip("'\"`")
            # Add context based on pattern if helpful
            if "SQL ERROR" in error_message: return f"SQL Error: {extracted}"
            if "Syntax error" in error_message: return f"Syntax Error: {extracted}"
            # Add more context prefixes if desired
            return extracted # Return the core message part

    # If no specific pattern matches, return the first non-empty line or a truncated version
    lines = [line.strip() for line in error_message.splitlines() if line.strip()]
    first_line = lines[0] if lines else error_message # Fallback to original if split fails

    max_len = 150
    if len(first_line) > max_len:
        return first_line[:max_len] + "..."
    else:
        return first_line

# --- Functions Removed (Redundant/Replaced) ---
# - format_time (use format_duration_for_display)
# - parse_explore_selector (handled by BaseValidator.resolve_explores)
# - format_error_message (handled by rich text wrapping/overflow in printer)
# - is_model_excluded (handled by BaseValidator.matches_selector)
# - is_explore_excluded (handled by BaseValidator.matches_selector)

