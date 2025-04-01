"""
Helper utilities for Looker Validator.
"""

import os
import re
import json
import time
from typing import Dict, List, Any, Optional, Tuple, Set, Union


def format_time(seconds: float) -> str:
    """Format time duration in a human-readable format.

    Args:
        seconds: Time in seconds

    Returns:
        Formatted time string
    """
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    else:
        hours = seconds / 3600
        minutes = (seconds % 3600) / 60
        return f"{int(hours)} hours {int(minutes)} minutes"


def parse_explore_selector(selector: str) -> Tuple[str, str, bool]:
    """Parse a model/explore selector string.

    Args:
        selector: Model/explore selector (e.g., "model/explore" or "-model/explore")

    Returns:
        Tuple of (model, explore, is_exclude)
    """
    is_exclude = selector.startswith("-")
    if is_exclude:
        selector = selector[1:]
    
    parts = selector.split("/", 1)
    
    if len(parts) == 1:
        return parts[0], "*", is_exclude
    else:
        return parts[0], parts[1], is_exclude


def format_error_message(message: str, max_length: int = 100) -> str:
    """Format an error message for display.

    Args:
        message: Error message
        max_length: Maximum length for the formatted message

    Returns:
        Formatted error message
    """
    # Strip whitespace and newlines
    message = " ".join(message.split())
    
    # Truncate if too long
    if len(message) > max_length:
        return message[:max_length] + "..."
    
    return message


def check_spectacles_ignore(sql: str, tags: List[str]) -> bool:
    """Check if a dimension should be ignored by Spectacles.

    Args:
        sql: SQL string for the dimension
        tags: List of tags for the dimension

    Returns:
        True if the dimension should be ignored
    """
    # Check for spectacles: ignore tag
    if any("spectacles: ignore" in tag.lower() for tag in tags):
        return True
    
    # Check for -- spectacles: ignore comment in SQL
    if sql and re.search(r'--\s*spectacles:\s*ignore', sql, re.IGNORECASE):
        return True
    
    return False


def save_json_file(data: Any, file_path: str, indent: int = 2) -> bool:
    """Save data to a JSON file.

    Args:
        data: Data to save
        file_path: Path to save the file
        indent: Indentation level for JSON formatting

    Returns:
        True if saved successfully
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Save to file
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=indent)
        
        return True
    except Exception as e:
        print(f"Failed to save JSON file: {str(e)}")
        return False


def load_json_file(file_path: str, default: Any = None) -> Any:
    """Load data from a JSON file.

    Args:
        file_path: Path to the JSON file
        default: Default value if file doesn't exist or can't be loaded

    Returns:
        Loaded data or default value
    """
    try:
        if not os.path.exists(file_path):
            return default
        
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Failed to load JSON file: {str(e)}")
        return default


def format_duration_for_display(seconds: float) -> str:
    """Format a duration for user display.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted duration string
    """
    if seconds < 0.1:
        return f"{seconds * 1000:.0f}ms"
    elif seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def create_explore_url(base_url: str, model: str, explore: str) -> str:
    """Create a URL to an explore in Looker.

    Args:
        base_url: Looker instance URL
        model: Model name
        explore: Explore name

    Returns:
        URL to the explore
    """
    # Ensure base_url doesn't end with a slash
    if base_url.endswith('/'):
        base_url = base_url[:-1]
    
    return f"{base_url}/explore/{model}/{explore}"


def is_model_excluded(model: str, includes: List[str], excludes: List[str]) -> bool:
    """Check if a model is excluded based on include/exclude lists.

    Args:
        model: Model name
        includes: List of include patterns
        excludes: List of exclude patterns

    Returns:
        True if the model should be excluded
    """
    # Check exclude patterns first
    for exclude in excludes:
        if exclude == model or exclude == f"{model}/*":
            return True
    
    # If includes are specified but model isn't included, exclude it
    if includes:
        for include in includes:
            if include == model or include == f"{model}/*" or include == "*":
                return False
        return True
    
    # Default: not excluded
    return False


def is_explore_excluded(model: str, explore: str, includes: List[str], excludes: List[str]) -> bool:
    """Check if an explore is excluded based on include/exclude lists.

    Args:
        model: Model name
        explore: Explore name
        includes: List of include patterns
        excludes: List of exclude patterns

    Returns:
        True if the explore should be excluded
    """
    # Check if model is excluded
    if is_model_excluded(model, includes, excludes):
        return True
    
    # Check exclude patterns for the explore
    for exclude in excludes:
        if exclude == f"{model}/{explore}" or exclude == f"*/{explore}":
            return True
    
    # If includes are specified and explore isn't included, exclude it
    if includes:
        is_included = False
        for include in includes:
            if (include == f"{model}/{explore}" or 
                include == f"{model}/*" or 
                include == f"*/{explore}" or
                include == "*/*"):
                is_included = True
                break
        
        if not is_included:
            return True
    
    # Default: not excluded
    return False


def extract_filename_from_path(file_path: str) -> str:
    """Extract the filename from a file path.

    Args:
        file_path: File path

    Returns:
        Filename
    """
    return os.path.basename(file_path) if file_path else "Unknown file"


def extract_looker_error(error_message: str) -> str:
    """Extract the relevant part of a Looker error message.

    Args:
        error_message: Full error message from Looker

    Returns:
        Simplified error message
    """
    # Extract SQL error
    if "SQL ERROR" in error_message:
        match = re.search(r"SQL ERROR: (.*?)(\n|$)", error_message)
        if match:
            return match.group(1)
    
    # Extract syntax error
    if "Syntax error" in error_message:
        match = re.search(r"Syntax error: (.*?)(\n|$)", error_message)
        if match:
            return match.group(1)
    
    # Default: return the original message with whitespace cleaned up
    return " ".join(error_message.split())