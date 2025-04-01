"""
Utilities for formatted console output.
"""

import os
import logging
import textwrap
from typing import Optional, List, Tuple
from pathlib import Path

import colorama

logger = logging.getLogger(__name__)

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

LINE_WIDTH = 80


def color(text: str, color_name: str) -> str:
    """Apply color to text if colors are enabled."""
    if os.environ.get("NO_COLOR") or os.environ.get("TERM") == "dumb":
        return str(text)
    else:
        return f"{COLORS[color_name]}{text}{COLORS['reset']}"


def red(text: str) -> str:
    """Apply red color to text."""
    return color(text, "red")


def green(text: str) -> str:
    """Apply green color to text."""
    return color(text, "green")


def yellow(text: str) -> str:
    """Apply yellow color to text."""
    return color(text, "yellow")


def cyan(text: str) -> str:
    """Apply cyan color to text."""
    return color(text, "cyan")


def blue(text: str) -> str:
    """Apply blue color to text."""
    return color(text, "blue")


def bold(text: str) -> str:
    """Make text bold."""
    return color(text, "bold")


def dim(text: str) -> str:
    """Make text dim."""
    return color(text, "dim")


def print_header(text: str, char: str = "=", line_width: int = LINE_WIDTH, leading_newline: bool = True) -> None:
    """Print a formatted header with surrounding characters."""
    header = f" {text} ".center(line_width, char)
    if leading_newline:
        header = "\n" + header
    logger.info(f"{header}\n")


def print_section(text: str, line_width: int = LINE_WIDTH) -> None:
    """Print a section divider with text."""
    logger.info(f"\n{bold(text)}")
    logger.info("-" * line_width)


def print_validation_result(status: str, source: str, skip_reason: Optional[str] = None) -> None:
    """Print a validation result with appropriate formatting."""
    if status == "passed":
        bullet = "✓"
        message = green(source)
    elif status == "failed":
        bullet = "❌"
        message = red(source)
    elif status == "skipped":
        bullet = "⏭️"
        if skip_reason:
            status = f"skipped ({skip_reason.replace('_', ' ')})"
        message = dim(source)
    else:
        raise ValueError(f"Unknown status: {status}")
    
    logger.info(f"{bullet} {message} {status}")


def print_sql_error(
    model: str,
    explore: str,
    message: str,
    sql: str,
    log_dir: str,
    dimension: Optional[str] = None,
    lookml_url: Optional[str] = None,
) -> None:
    """Print a SQL error with context."""
    path = f"{model}/{dimension if dimension else explore}"
    logger.info(f"\n{red(path)}")
    logger.info("=" * (len(path) + 2))
    
    # Format and wrap the error message
    wrapped = textwrap.fill(message, LINE_WIDTH)
    logger.info(wrapped)
    
    # Log LookML link if available
    if lookml_url:
        logger.info(f"\nLookML: {lookml_url}")
    
    # Save SQL to file and log the path
    file_path = log_sql_error(model, explore, sql, log_dir, dimension)
    logger.info(f"\nTest SQL saved to: {file_path}")


def log_sql_error(
    model: str, 
    explore: str, 
    sql: str, 
    log_dir: str, 
    dimension: Optional[str] = None
) -> Path:
    """Save SQL that caused an error to a file."""
    log_path = Path(log_dir) / "queries"
    log_path.mkdir(parents=True, exist_ok=True)
    
    # Create a filename based on the model/explore/dimension
    file_name = f"{model}__{explore}"
    if dimension:
        file_name += f"__{dimension}"
    file_name = file_name.replace(".", "_") + ".sql"
    
    file_path = log_path / file_name
    
    logger.debug(f"Logging failing SQL query to '{file_path}'")
    
    with open(file_path, "w") as f:
        f.write(sql)
    
    return file_path


def print_lookml_error(
    file_path: str, 
    line_number: int, 
    severity: str, 
    message: str, 
    lookml_url: Optional[str] = None
) -> None:
    """Print a LookML error with context."""
    if not file_path:
        file_path = "[File name not given by Looker]"
        
    header_text = f"{file_path}:{line_number}"
    logger.info(f"\n{yellow(header_text) if severity == 'warning' else red(header_text)}")
    logger.info("=" * (len(header_text) + 2))
    
    # Format and wrap the error message
    wrapped = textwrap.fill(f"[{severity.title()}] {message}", LINE_WIDTH)
    logger.info(wrapped)
    
    # Log LookML link if available
    if lookml_url:
        logger.info(f"\nLookML: {lookml_url}")


def print_content_error(
    model: str,
    explore: str,
    message: str,
    content_type: str,
    space: str,
    title: str,
    url: str,
    tile_type: Optional[str] = None,
    tile_title: Optional[str] = None
) -> None:
    """Print a content validation error with context."""
    path = f"{title} [{space}]"
    logger.info(f"\n{red(path)}")
    logger.info("=" * (len(path) + 2))
    
    # Print tile information for dashboards
    if content_type == "dashboard" and tile_type and tile_title:
        if tile_type == "dashboard_filter":
            tile_type = "Filter"
        else:
            tile_type = "Tile"
        line = f"{tile_type} '{tile_title}' failed validation."
        logger.info(textwrap.fill(line, LINE_WIDTH) + "\n")
    
    # Format and wrap the error message
    line = f"Error in {model}/{explore}: {message}"
    logger.info(textwrap.fill(line, LINE_WIDTH))
    
    # Log content URL
    logger.info(f"\n{content_type.title()}: {url}")


def format_table(
    headers: List[str], 
    rows: List[Tuple],
    padding: int = 2,
    header_color: str = "bold"
) -> str:
    """Format data as a table with aligned columns."""
    if not rows:
        return "No data"
    
    # Convert all values to strings
    str_rows = [[str(cell) for cell in row] for row in rows]
    
    # Determine the width of each column
    col_widths = [len(h) for h in headers]
    for row in str_rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))
    
    # Add padding
    col_widths = [w + padding for w in col_widths]
    
    # Format the headers
    header_row = "".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    separator = "".join("-" * w for w in col_widths)
    
    # Format the rows
    formatted_rows = ["".join(cell.ljust(col_widths[i]) for i, cell in enumerate(row)) for row in str_rows]
    
    # Combine everything
    result = f"{color(header_row, header_color)}\n{separator}\n" + "\n".join(formatted_rows)
    
    return result