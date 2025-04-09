# looker_validator/printer.py

"""
Utilities for formatted console output.
Based on the user's original code's functionality, but reimplemented using 'rich'
for better structured output suitable for CI environments.
"""

import sys
import os
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Import rich safely
try:
    from rich.console import Console
    from rich.theme import Theme
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
    from rich.syntax import Syntax
    from rich import print as rich_print # Use rich's print for auto-detection
except ImportError:
    print("Error: 'rich' library is required for enhanced output. Please install it (`pip install rich`).", file=sys.stderr)
    # Provide a basic fallback if rich is not installed
    # Note: Fallback won't have colors, tables, panels etc.
    class ConsoleFallback:
        def print(self, *args, **kwargs):
            # Basic print, ignoring style arguments
            new_kwargs = {k: v for k, v in kwargs.items() if k not in ['style', 'justify', 'overflow']}
            is_stderr = kwargs.get('stderr', False) # Crude check
            print(*args, **new_kwargs, file=sys.stderr if is_stderr else sys.stdout)

    console_stderr = ConsoleFallback()
    console_stdout = ConsoleFallback()

    # --- Fallback Print Functions ---
    def print_header(text: str, **kwargs): print(f"\n=== {text} ===")
    def print_section(text: str, **kwargs): print(f"\n--- {text} ---")
    def print_success(text: str): print(f"[OK] {text}")
    def print_fail(text: str): print(f"[FAIL] {text}", file=sys.stderr)
    def print_warning(text: str): print(f"[WARN] {text}", file=sys.stderr)
    def print_info(text: str): print(f"[INFO] {text}")
    def print_debug(text: str): print(f"[DEBUG] {text}")
    def print_validation_result(status: str, source: str, skip_reason: Optional[str] = None):
        if status == "passed": print(f"[PASS] {source}")
        elif status == "failed": print(f"[FAIL] {source}", file=sys.stderr)
        elif status == "skipped": print(f"[SKIP] {source}" + (f" ({skip_reason})" if skip_reason else ""))
        else: print(f"[????] {status} - {source}")

    def log_sql_error(model: str, explore: str, sql: str, log_dir: str, dimension: Optional[str] = None) -> Path:
        # Keep the original file logging logic
        log_path = Path(log_dir) / "queries"
        log_path.mkdir(parents=True, exist_ok=True)
        file_name = f"{model}__{explore}"
        if dimension: file_name += f"__{dimension}"
        file_name = file_name.replace(".", "_") + ".sql"
        file_path = log_path / file_name
        print(f"[DEBUG] Logging failing SQL query to '{file_path}'") # Basic debug
        try:
            with open(file_path, "w", encoding='utf-8') as f: f.write(sql)
        except Exception as e:
            print(f"[ERROR] Failed to write SQL log to {file_path}: {e}", file=sys.stderr)
        return file_path

    def print_sql_error(model: str, explore: str, message: str, sql: str, log_dir: str, dimension: Optional[str] = None, lookml_url: Optional[str] = None):
        path = f"{model}/{dimension if dimension else explore}"
        print(f"\n[SQL ERROR] {path}\n{'=' * (len(path) + 2)}", file=sys.stderr)
        print(message, file=sys.stderr)
        if lookml_url: print(f"LookML: {lookml_url}", file=sys.stderr)
        file_path = log_sql_error(model, explore, sql, log_dir, dimension)
        print(f"Test SQL saved to: {file_path}", file=sys.stderr)

    def print_lookml_error(file_path: str, line_number: int, severity: str, message: str, lookml_url: Optional[str] = None):
        if not file_path: file_path = "[File name not given by Looker]"
        header_text = f"{file_path}:{line_number}"
        print(f"\n[LookML {severity.upper()}] {header_text}\n{'=' * (len(header_text) + 2)}", file=sys.stderr)
        print(f"[{severity.title()}] {message}", file=sys.stderr)
        if lookml_url: print(f"LookML: {lookml_url}", file=sys.stderr)

    def print_content_error(model: str, explore: str, message: str, content_type: str, space: str, title: str, url: str, tile_type: Optional[str] = None, tile_title: Optional[str] = None):
        path = f"{title} [{space}]"
        print(f"\n[Content ERROR] {path}\n{'=' * (len(path) + 2)}", file=sys.stderr)
        if content_type == "dashboard" and tile_type and tile_title:
            tile_label = "Filter" if tile_type == "dashboard_filter" else "Tile"
            print(f"{tile_label} '{tile_title}' failed validation.\n", file=sys.stderr)
        print(f"Error in {model}/{explore}: {message}", file=sys.stderr)
        print(f"{content_type.title()}: {url}", file=sys.stderr)

    def print_error_summary_table(errors: List[Dict[str, Any]]):
        print("\n--- Error Summary ---", file=sys.stderr)
        if not errors:
            print("No errors found.")
            return
        # Basic list format for fallback
        for i, error in enumerate(errors):
             print(f"Error {i+1}: {error}", file=sys.stderr)

else:
    # --- Rich Implementation ---
    # Check for NO_COLOR env var, respecting user's original logic
    NO_COLOR = os.environ.get("NO_COLOR") or os.environ.get("TERM") == "dumb"

    # Define a custom theme for consistent styling
    # Styles match intent of original colorama usage where possible
    custom_theme = Theme({
        "info": "dim cyan",
        "warning": "yellow",
        "error": "bold red",
        "success": "green",
        "debug": "dim magenta",
        "header": "bold blue underline",
        "section": "bold blue",
        "detail_key": "bold cyan",
        "detail_value": "none", # Default text color
        "skip": "dim",
        "path": "cyan",
        "url": "underline blue",
        "severity_error": "bold red",
        "severity_warning": "yellow",
    })

    # Separate consoles for stdout (info, success) and stderr (errors, warnings)
    # This helps separate streams in CI logs.
    # Force terminal might be needed depending on the CI system's handling of colors.
    console_stderr = Console(theme=custom_theme, stderr=True, force_terminal=False, no_color=NO_COLOR)
    console_stdout = Console(theme=custom_theme, force_terminal=False, no_color=NO_COLOR)

    # Get logger (assuming configured elsewhere, e.g., logger.py)
    # Printer functions will use Rich consoles, but logging can still happen
    logger = logging.getLogger(__name__)

    # --- Rich Printer Functions ---

    def print_header(text: str, char: str = "=", line_width: int = 80, leading_newline: bool = True):
        """Prints a formatted header using Rich Panel."""
        # Rich handles width and centering better than manual padding
        if leading_newline:
             console_stdout.print() # Print a blank line
        console_stdout.print(Panel(Text(text, justify="center"), style="header", expand=True))
        # Log the header as well if needed for file logs
        logger.info(f" {text} ".center(line_width, char)) # Keep original log format

    def print_section(text: str, line_width: int = 80):
        """Prints a section header using Rich style."""
        console_stdout.print(f"\n[{custom_theme.styles['section']}]{text}[/]")
        console_stdout.print("-" * line_width)
        logger.info(f"\n{text}\n{'-' * line_width}") # Keep original log format

    def print_success(text: str):
        """Prints a success message (stdout)."""
        console_stdout.print(f"[success]✔[/success] {text}")
        logger.info(f"[SUCCESS] {text}")

    def print_fail(text: str):
        """Prints a failure/error message (stderr)."""
        console_stderr.print(f"[error]✖[/error] {text}")
        logger.error(f"[FAIL] {text}")

    def print_warning(text: str):
        """Prints a warning message (stderr)."""
        console_stderr.print(f"[warning]⚠[/warning] {text}")
        logger.warning(f"[WARN] {text}")

    def print_info(text: str):
        """Prints an informational message (stdout)."""
        console_stdout.print(f"[{custom_theme.styles['info']}]ℹ[/] {text}")
        logger.info(f"[INFO] {text}")

    def print_debug(text: str):
        """Prints a debug message (stdout)."""
        # Optionally check an env var or config flag to enable/disable
        # if DEBUG_ENABLED:
        console_stdout.print(f"[debug]⚙[/debug] {text}")
        logger.debug(f"[DEBUG] {text}")

    def print_validation_result(status: str, source: str, skip_reason: Optional[str] = None):
        """Print a validation result with appropriate formatting using Rich."""
        status_lower = status.lower()
        log_msg = f"{source} {status}"

        if status_lower == "passed":
            bullet = "[success]✓[/success]"
            message = f"[success]{source}[/success]"
            status_text = "[success]passed[/success]"
            console_to_use = console_stdout
        elif status_lower == "failed":
            bullet = "[error]❌[/error]"
            message = f"[error]{source}[/error]"
            status_text = "[error]failed[/error]"
            console_to_use = console_stderr
        elif status_lower == "skipped":
            bullet = "[skip]⏭️[/skip]"
            message = f"[skip]{source}[/skip]"
            status_text = "[skip]skipped[/skip]"
            if skip_reason:
                reason_formatted = skip_reason.replace('_', ' ')
                status_text += f" [skip]({reason_formatted})[/]"
                log_msg += f" ({reason_formatted})"
            console_to_use = console_stdout # Skipped usually isn't an error
        else:
            bullet = "[warning]?[/warning]" # Unknown status
            message = f"[warning]{source}[/warning]"
            status_text = f"[warning]{status}[/warning]"
            console_to_use = console_stderr

        console_to_use.print(f"{bullet} {message} {status_text}")
        logger.info(log_msg) # Log the plain status

    def log_sql_error(model: str, explore: str, sql: str, log_dir: str, dimension: Optional[str] = None) -> Path:
        """Save SQL that caused an error to a file. (Retained from original)"""
        log_path = Path(log_dir) / "queries"
        log_path.mkdir(parents=True, exist_ok=True)

        # Create a filename based on the model/explore/dimension
        file_name = f"{model}__{explore}"
        if dimension:
            file_name += f"__{dimension}"
        # Sanitize filename slightly more robustly
        file_name = "".join(c if c.isalnum() or c in ('_', '-') else '_' for c in file_name) + ".sql"

        file_path = log_path / file_name

        print_debug(f"Logging failing SQL query to '{file_path}'")
        logger.debug(f"Logging failing SQL query to '{file_path}'")

        try:
            with open(file_path, "w", encoding='utf-8') as f:
                f.write(sql)
            print_debug(f"Successfully wrote SQL log: {file_path}")
        except IOError as e:
            print_fail(f"Failed to write SQL log to {file_path}: {e}")
            logger.error(f"Failed to write SQL log to {file_path}: {e}", exc_info=True)

        return file_path

    def print_sql_error(
        model: str,
        explore: str,
        message: str,
        sql: str,
        log_dir: str,
        dimension: Optional[str] = None,
        lookml_url: Optional[str] = None,
    ) -> None:
        """Print a SQL error with context using Rich."""
        path = f"{model}/{dimension if dimension else explore}"
        title = f"[error]SQL Error[/error]: [path]{path}[/path]"

        content = Text()
        content.append(message, style="error")
        if lookml_url:
            content.append(f"\nLookML: ", style="bold")
            content.append(lookml_url, style="url")

        # Save SQL to file and add path to output
        file_path = log_sql_error(model, explore, sql, log_dir, dimension)
        content.append(f"\nTest SQL saved to: ", style="info")
        content.append(str(file_path), style="info") # Make path less prominent

        console_stderr.print(Panel(content, title=title, border_style="error", expand=False))
        logger.error(f"SQL Error in {path}: {message}. SQL logged to {file_path}") # Log summary

    def print_lookml_error(
        file_path: str,
        line_number: int,
        severity: str,
        message: str,
        lookml_url: Optional[str] = None
    ) -> None:
        """Print a LookML error with context using Rich."""
        if not file_path:
            file_path = "[File name not given by Looker]"

        severity_lower = severity.lower()
        style_prefix = "severity_warning" if severity_lower == "warning" else "severity_error"
        title = f"[{style_prefix}]LookML {severity.title()}[/]: [path]{file_path}[/]:[bold]{line_number}[/]"
        border_style = "warning" if severity_lower == "warning" else "error"
        console_to_use = console_stderr # Both warnings and errors go to stderr

        content = Text()
        content.append(f"[{style_prefix}]{message}[/]") # Apply style based on severity
        if lookml_url:
            content.append(f"\nLookML: ", style="bold")
            content.append(lookml_url, style="url")

        console_to_use.print(Panel(content, title=title, border_style=border_style, expand=False))
        logger.log(logging.WARNING if severity_lower == 'warning' else logging.ERROR,
                   f"LookML {severity} in {file_path}:{line_number}: {message}") # Log summary

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
        """Print a content validation error with context using Rich."""
        panel_title = f"[error]Content Error[/error]: [bold]{title}[/] [[dim]{space}[/]]"

        content = Text()
        # Add tile information for dashboards
        if content_type == "dashboard" and tile_type and tile_title:
            tile_label = "Filter" if tile_type == "dashboard_filter" else "Tile"
            content.append(f"{tile_label} '{tile_title}' failed validation.\n\n", style="bold")

        # Format and wrap the error message
        content.append(f"Error in [path]{model}/{explore}[/path]: ", style="error")
        content.append(message, style="error") # Keep error message style

        # Add content URL
        content.append(f"\n{content_type.title()}: ", style="bold")
        content.append(url, style="url")

        console_stderr.print(Panel(content, title=panel_title, border_style="error", expand=False))
        log_msg = f"Content Error in {content_type} '{title}' ({space}) - {model}/{explore}: {message}"
        if tile_title: log_msg += f" (Tile: '{tile_title}')"
        logger.error(log_msg) # Log summary


    def print_error_summary_table(errors: List[Dict[str, Any]]):
        """Prints a formatted summary table of validation errors using Rich (stderr)."""
        if not errors:
            print_info("No errors found during validation.")
            return

        console_stderr.print("\n[error]--- Error Summary ---[/error]")

        table = Table(show_header=True, header_style="bold magenta", border_style="dim", title=f"{len(errors)} Errors Found")

        # Define columns based on common keys in error dictionaries
        # Try to find common keys across all errors provided
        common_keys = set()
        for error in errors:
            if isinstance(error, dict): # Ensure it's a dictionary
                common_keys.update(error.keys())

        # Prioritize certain columns if they exist, matching previous example
        cols = ['validator', 'type', 'severity', 'model', 'explore', 'view', 'field', 'test', 'dashboard', 'look', 'title', 'space', 'file_path', 'line', 'message']
        ordered_cols = [k for k in cols if k in common_keys]
        # Add remaining keys alphabetically
        ordered_cols.extend(sorted(list(common_keys - set(ordered_cols))))

        # Add columns to table
        for key in ordered_cols:
             justify = "left"
             style = "none"
             overflow = "fold" # Default overflow handling
             max_width = None
             if key == 'message': style = "dim"; max_width = 80 # Make long messages less prominent
             elif key == 'validator': style = "bold blue"
             elif key == 'type': style = "yellow"
             elif key == 'severity': style = "warning" # Default style, adjust based on value later if needed
             elif key == 'file_path': style = "cyan"
             elif key == 'line': justify = "right"; style="bold"

             table.add_column(key.replace("_", " ").title(), justify=justify, style=style, overflow=overflow, max_width=max_width)

        # Add rows to table
        for error in errors:
            if not isinstance(error, dict): continue # Skip non-dict items

            row_values = []
            for key in ordered_cols:
                 value = error.get(key)
                 # Format value for display (handle None, lists, apply severity style)
                 style_override = None
                 if value is None:
                     display_value = "[dim]-[/]"
                 elif key == 'severity' and isinstance(value, str):
                      display_value = value.title()
                      if value.lower() == 'error': style_override = "error"
                      elif value.lower() == 'warning': style_override = "warning"
                 elif isinstance(value, list):
                     display_value = ", ".join(map(str, value))
                 else:
                     display_value = str(value)

                 # Apply style override if needed (e.g., for severity)
                 text = Text(display_value, style=style_override) if style_override else Text(display_value)
                 row_values.append(text)

            table.add_row(*row_values)

        console_stderr.print(table)

# Note: The original format_table function was removed as Rich handles table formatting.
# The logger usage within printer functions is kept for compatibility with file logging,
# but the primary console output is now handled by Rich consoles.

