# looker_validator/cli.py
"""
Command-line interface for Looker Validator.
Uses standard click options and relies on Config class for file loading.
Uses --include-personal flag to control personal folder validation (default is exclude).
"""

import os
import sys
import logging
import traceback
from typing import List, Optional, Any, Dict, Tuple

import yaml # Keep yaml import for main() pre-check
import click

# Import core components
from .config import Config, DEFAULT_CONCURRENCY # Import the constant
from .connection import LookerConnection
# Import central exceptions
from .exceptions import (
    ValidatorError, LookerApiError, LookerBranchError, LookerConnectionError,
    LookerAuthenticationError, ConfigError, ContentValidationError, LookMLValidationError,
    SQLValidationError, AssertValidationError
)
# Import validators
from .validators.sql_validator import SQLValidator
from .validators.content_validator import ContentValidator
from .validators.assert_validator import AssertValidator
from .validators.lookml_validator import LookMLValidator
# Import the updated printer - use specific functions
from .printer import (
    print_header, print_section, print_info, print_success, print_fail,
    print_warning, print_debug, print_error_summary_table
)
# Import file logging setup function
from .logger import setup_file_logging

# Get root logger for the application
logger = logging.getLogger("looker_validator")


# --- Custom Click Option Classes REMOVED ---
# Removed ConfigFileOption class as it caused the TypeError and added complexity.
# Removed env_var_option decorator; use standard @click.option with envvar=...


# --- CLI Group ---
@click.group()
@click.version_option(package_name='looker-validator')
def cli():
    """Looker Validator: A CI tool for Looker / LookML validation."""
    pass


# --- Helper Function for Setup ---
def setup_validation(kwargs: Dict[str, Any]) -> Tuple[Config, LookerConnection]:
    """Initializes Config and LookerConnection, handling errors."""
    try:
        # Config init handles precedence (CLI > Env > File > Default)
        # and validation of required fields.
        # The config_file path is passed directly if provided via CLI.
        config = Config(**kwargs)

        # Configure logging based on final config values (verbose, log_dir)
        if config.log_dir:
            file_log_level = logging.DEBUG # Always log DEBUG to file
            if not setup_file_logging(log_dir=config.log_dir, log_level=file_log_level):
                print_warning(f"File logging setup failed. Check logs/permissions for directory: {config.log_dir}")
        else:
            logger.info("Log directory not specified, file logging disabled.")

        # Now initialize connection
        connection = LookerConnection(
            base_url=config.base_url,
            client_id=config.client_id,
            client_secret=config.client_secret,
            port=config.port,
            api_version=config.api_version,
            timeout=config.timeout,
        )
        return config, connection
    except ConfigError as e:
        print_fail(f"Configuration Error: {e}")
        logger.critical(f"Configuration Error: {e}", exc_info=False) # No need for traceback here
        sys.exit(2)
    except LookerAuthenticationError as e:
        print_fail(f"Looker Authentication Failed: {e}")
        logger.critical(f"Looker Authentication Failed: {e}", exc_info=True)
        sys.exit(3)
    except Exception as e:
        print_fail(f"Initialization Error: {e}")
        logger.critical(f"Initialization Error: {e}", exc_info=True)
        sys.exit(4)


# --- CLI Commands ---

@cli.command()
# Use standard click.option
@click.option("--config-file", "-c", help="Path to YAML config file.", type=click.Path(exists=True, dir_okay=False)) # Removed config_file_param
@click.option("--base-url", help="Looker instance URL. [env: LOOKER_BASE_URL]", envvar="LOOKER_BASE_URL")
@click.option("--client-id", help="Looker API client ID. [env: LOOKER_CLIENT_ID]", envvar="LOOKER_CLIENT_ID")
@click.option("--client-secret", help="Looker API client secret. [env: LOOKER_CLIENT_SECRET]", envvar="LOOKER_CLIENT_SECRET", prompt=False, hide_input=True)
@click.option("--port", help="Looker API port. [env: LOOKER_PORT]", type=int, envvar="LOOKER_PORT")
@click.option("--api-version", help="Looker API version. [env: LOOKER_API_VERSION]", default="4.0", show_default=True, envvar="LOOKER_API_VERSION")
@click.option("--timeout", help="API request timeout. [env: LOOKER_TIMEOUT]", type=int, default=600, show_default=True, envvar="LOOKER_TIMEOUT")
@click.option("--verbose", "-v", help="Enable verbose (DEBUG) logging. [env: LOOKER_VERBOSE]", is_flag=True, default=False, envvar="LOOKER_VERBOSE")
def connect(**kwargs):
    """Tests the connection to the Looker API."""
    print_header("Testing Looker Connection")
    config, connection = setup_validation(kwargs) # Handles setup errors including missing required config

    try:
        success_message = connection.test_connection() # Raises error on failure
        print_success(success_message)
        sys.exit(0)
    except LookerConnectionError as e:
        print_fail(f"Connection Test Failed: {e}")
        logger.error(f"Connection Test Failed: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        print_fail(f"Unexpected Error during connection test: {e}")
        logger.error(f"Unexpected Error during connection test: {e}", exc_info=True)
        sys.exit(1)


# --- Common Validator Options Decorator ---
def validator_options(f):
    """Decorator for common options needed by most validator commands."""
    # Use standard click.option
    f = click.option("--config-file", "-c", help="Path to YAML config file.", type=click.Path(exists=True, dir_okay=False))(f) # Removed config_file_param
    f = click.option("--base-url", help="Looker instance URL. [env: LOOKER_BASE_URL]", envvar="LOOKER_BASE_URL")(f)
    f = click.option("--client-id", help="Looker API client ID. [env: LOOKER_CLIENT_ID]", envvar="LOOKER_CLIENT_ID")(f)
    f = click.option("--client-secret", help="Looker API client secret. [env: LOOKER_CLIENT_SECRET]", envvar="LOOKER_CLIENT_SECRET", prompt=False, hide_input=True)(f)
    f = click.option("--port", help="Looker API port. [env: LOOKER_PORT]", type=int, envvar="LOOKER_PORT")(f)
    f = click.option("--api-version", help="Looker API version. [env: LOOKER_API_VERSION]", default="4.0", show_default=True, envvar="LOOKER_API_VERSION")(f)
    f = click.option("--timeout", help="API request timeout. [env: LOOKER_TIMEOUT]", type=int, default=600, show_default=True, envvar="LOOKER_TIMEOUT")(f)
    # Project IS required for validators
    f = click.option("--project", help="Looker project name. [env: LOOKER_PROJECT]", required=True, envvar="LOOKER_PROJECT")(f)
    # Git state options
    f = click.option("--branch", help="Git branch name (default: production). [env: LOOKER_GIT_BRANCH]", envvar="LOOKER_GIT_BRANCH")(f)
    f = click.option("--commit-ref", help="Git commit reference (overrides branch). [env: LOOKER_COMMIT_REF]", envvar="LOOKER_COMMIT_REF")(f)
    f = click.option("--remote-reset", help="Reset branch to remote state before validating. [env: LOOKER_REMOTE_RESET]", is_flag=True, default=False, envvar="LOOKER_REMOTE_RESET")(f)
    f = click.option("--pin-imports", help="Pin imported projects (format: 'proj:ref,proj2:ref2'). [env: LOOKER_PIN_IMPORTS]", envvar="LOOKER_PIN_IMPORTS")(f)
    # General options
    f = click.option("--log-dir", help="Directory for logs and artifacts. [env: LOOKER_LOG_DIR]", default="logs", show_default=True, envvar="LOOKER_LOG_DIR")(f)
    f = click.option("--verbose", "-v", help="Enable verbose (DEBUG) logging. [env: LOOKER_VERBOSE]", is_flag=True, default=False, envvar="LOOKER_VERBOSE")(f)
    return f

# --- Helper to run a validator and handle results/errors ---
def run_validator(validator_class, command_name: str, config: Config, connection: LookerConnection, **kwargs):
    """Instantiates and runs a validator, handling common errors and reporting."""
    print_header(f"{command_name} Validator")
    # Pass all config options collected into the Config object
    validator_kwargs = config.as_dict()

    # Remove 'project' from validator_kwargs if it exists to avoid duplicate parameter error
    if 'project' in validator_kwargs:
        del validator_kwargs['project']

    all_errors: List[Dict[str, Any]] = []
    exit_code = 0
    try:
        # Ensure project is set for validators (redundant check, but safe)
        if not config.project:
             raise ConfigError("Looker project must be specified using --project or LOOKER_PROJECT for this command.")

        validator = validator_class(connection, config.project, **validator_kwargs)
        all_errors = validator.validate() # Returns list of error dicts

        if all_errors:
            print_error_summary_table(all_errors)
            # Determine exit code based on highest severity found
            highest_severity = "info"
            if any(e.get("severity") == "error" for e in all_errors):
                highest_severity = "error"
            elif any(e.get("severity") == "warning" for e in all_errors):
                highest_severity = "warning"

            if highest_severity == "error":
                 exit_code = 1
            else:
                 print_warning(f"Validation completed with {highest_severity} messages.")
                 exit_code = 0 # Treat warnings/info as success for CI exit code
        else:
            # Success message logged by validator now
            pass

    # Catch specific errors first
    except ConfigError as e: # Catch config errors found after initial setup
        print_fail(f"Configuration Error: {e}")
        logger.critical(f"Configuration Error: {e}", exc_info=False)
        exit_code = 2
    except (LookerConnectionError, LookerBranchError, LookerApiError, ValidatorError) as e:
        print_fail(f"{command_name} Validation Failed: {e}")
        logger.error(f"{command_name} Validation Failed: {e}", exc_info=True)
        exit_code = 1
    except Exception as e:
        print_fail(f"Unexpected Error during {command_name} validation: {e}")
        logger.exception(f"Unexpected Error during {command_name} validation:")
        exit_code = 1

    sys.exit(exit_code)


# --- Validator Commands ---

@cli.command()
@validator_options
@click.option("--explores", help="Filter explores (e.g., 'model/*', '-model/explore'). [env: LOOKER_EXPLORES]", multiple=True, envvar="LOOKER_EXPLORES")
@click.option("--concurrency", help="Number of concurrent queries. [env: LOOKER_CONCURRENCY]", type=int, default=DEFAULT_CONCURRENCY, show_default=True, envvar="LOOKER_CONCURRENCY")
# Add SQL specific options if needed, e.g., --dry-run
# @click.option("--dry-run", help="Skip query execution (validate SQL generation only). [env: LOOKER_DRY_RUN]", is_flag=True, default=False, envvar="LOOKER_DRY_RUN")
def sql(**kwargs):
    """Runs SQL validation: checks if explores generate valid SQL."""
    config, connection = setup_validation(kwargs)
    run_validator(SQLValidator, "SQL", config, connection, **kwargs)


@cli.command()
@validator_options
@click.option("--explores", help="Filter content by model/explore. [env: LOOKER_EXPLORES]", multiple=True, envvar="LOOKER_EXPLORES")
@click.option("--folders", help="Filter content by folder ID (e.g., '123', '-456'). [env: LOOKER_FOLDERS]", multiple=True, envvar="LOOKER_FOLDERS")
# --- FLAG LOGIC REMAINS THE SAME AS PREVIOUS FIX ---
# Uses --include-personal flag, default False (meaning exclude is default)
@click.option("--include-personal", help="Include content in personal folders (default: exclude). [env: LOOKER_INCLUDE_PERSONAL]", is_flag=True, default=False, envvar="LOOKER_INCLUDE_PERSONAL")
# --- END FLAG LOGIC ---
@click.option("--incremental", help="Only report errors unique to the current branch. [env: LOOKER_INCREMENTAL]", is_flag=True, default=False, envvar="LOOKER_INCREMENTAL")
@click.option("--target", help="Target branch for incremental comparison (default: production). [env: LOOKER_TARGET]", envvar="LOOKER_TARGET")
def content(**kwargs):
    """Runs Content validation: checks Looks and Dashboards for errors."""
    # --- TRANSFORMATION REMAINS THE SAME ---
    # Map the --include-personal flag to the internal exclude_personal config value
    include_personal_flag = kwargs.pop('include_personal', False) # Get value from Click
    kwargs['exclude_personal'] = not include_personal_flag # Set the inverse for Config
    # --- END TRANSFORMATION ---

    config, connection = setup_validation(kwargs)
    run_validator(ContentValidator, "Content", config, connection, **kwargs)


@cli.command(name="assert")
@validator_options
@click.option("--explores", help="Filter tests by model/explore. [env: LOOKER_EXPLORES]", multiple=True, envvar="LOOKER_EXPLORES")
@click.option("--concurrency", help="Number of concurrent tests. [env: LOOKER_CONCURRENCY]", type=int, default=DEFAULT_CONCURRENCY, show_default=True, envvar="LOOKER_CONCURRENCY")
def assert_command(**kwargs):
    """Runs Assert validation: executes LookML data tests."""
    config, connection = setup_validation(kwargs)
    run_validator(AssertValidator, "Assert", config, connection, **kwargs)


@cli.command()
@validator_options
@click.option("--severity", help="Minimum issue severity to report as failure. [env: LOOKER_SEVERITY]", type=click.Choice(["info", "warning", "error"]), default="warning", show_default=True, envvar="LOOKER_SEVERITY")
def lookml(**kwargs):
    """Runs LookML validation: checks LookML syntax and references."""
    min_severity_arg = kwargs.get("severity", "warning")
    print_header("LookML Validator")
    config, connection = setup_validation(kwargs)

    validator_kwargs = config.as_dict()

    # Remove 'project' from validator_kwargs if it exists
    if 'project' in validator_kwargs:
        del validator_kwargs['project']

    all_issues: List[Dict[str, Any]] = []
    exit_code = 0
    try:
        # Ensure project is set
        if not config.project:
             raise ConfigError("Looker project must be specified using --project or LOOKER_PROJECT for the lookml command.")

        validator = LookMLValidator(connection, config.project, **validator_kwargs)
        all_issues = validator.validate() # Returns list of all issues found

        severity_levels = {"info": 0, "warning": 1, "error": 2}
        min_level = severity_levels.get(min_severity_arg.lower(), 1)
        failing_issues = [
            issue for issue in all_issues
            if severity_levels.get(str(issue.get("severity")).lower(), 0) >= min_level
        ]

        if failing_issues:
            print_warning(f"Displaying issues with severity '{min_severity_arg}' or higher:")
            print_error_summary_table(failing_issues)
            # Exit with error code only if there are 'error' severity issues
            if any(severity_levels.get(str(issue.get("severity")).lower(), 0) >= severity_levels["error"] for issue in failing_issues):
                 exit_code = 1
            else:
                 print_warning("Validation completed with only warnings.")
                 exit_code = 0 # Treat warnings as success for exit code
        elif all_issues:
             print_info(f"LookML validation found only issues below severity threshold '{min_severity_arg}'.")
             if config.verbose:
                 print_info("Displaying all found issues (including lower severity):")
                 print_error_summary_table(all_issues)
             exit_code = 0
        else:
            pass # Logger in validator handles success message

    except ConfigError as e: # Catch config errors found after initial setup
        print_fail(f"Configuration Error: {e}")
        logger.critical(f"Configuration Error: {e}", exc_info=False)
        exit_code = 2
    except (LookerConnectionError, LookerBranchError, LookerApiError, ValidatorError, LookMLValidationError) as e:
        print_fail(f"LookML Validation Failed: {e}")
        logger.error(f"LookML Validation Failed: {e}", exc_info=True)
        exit_code = 1
    except Exception as e:
        print_fail(f"Unexpected Error during LookML validation: {e}")
        logger.exception("Unexpected Error during LookML validation:")
        exit_code = 1

    sys.exit(exit_code)


# --- Main Entry Point ---
def configure_logging(verbose: bool, log_dir: Optional[str]):
     """Configure root logger level and add file handler."""
     log_level = logging.DEBUG if verbose else logging.INFO
     # Ensure handlers are removed before adding new ones if necessary
     root_logger = logging.getLogger()
     # for handler in root_logger.handlers[:]: # Iterate over a copy
     #      root_logger.removeHandler(handler)

     logging.basicConfig(
         level=log_level,
         format="%(asctime)s [%(levelname)-7s] %(name)s: %(message)s",
         datefmt="%Y-%m-%d %H:%M:%S",
         force=True # Override existing basicConfig if any
     )
     # Mute noisy libraries if needed
     # logging.getLogger("urllib3").setLevel(logging.WARNING)

     # Set looker_sdk logger level
     sdk_log_level = logging.DEBUG if verbose else logging.INFO # Show SDK INFO by default
     logging.getLogger("looker_sdk").setLevel(sdk_log_level)

     logger.info(f"Console logging level set to {logging.getLevelName(log_level)}")

     # Setup file logging using the function from logger.py
     if log_dir:
         file_log_level = logging.DEBUG # Always log DEBUG to file for details
         if not setup_file_logging(log_dir=log_dir, log_level=file_log_level):
             print_warning(f"File logging setup failed. Check logs/permissions for directory: {log_dir}")
     else:
          logger.info("Log directory not specified, file logging disabled.")


def main():
    """Main entry point for the CLI."""
    verbose = False
    log_dir_arg = "logs" # Default log dir
    config_file_arg = None
    temp_args = sys.argv[1:]

    # Quick parse for verbose, log_dir, config_file BEFORE Click fully parses
    # Allows logging and config file loading to influence defaults early
    if "-v" in temp_args or "--verbose" in temp_args: verbose = True
    try: # Find log_dir
        if "--log-dir" in temp_args:
            log_dir_index = temp_args.index("--log-dir") + 1
            if log_dir_index < len(temp_args): log_dir_arg = temp_args[log_dir_index]
    except ValueError: pass
    try: # Find config_file
        if "--config-file" in temp_args:
            config_file_index = temp_args.index("--config-file") + 1
            if config_file_index < len(temp_args): config_file_arg = temp_args[config_file_index]
        elif "-c" in temp_args:
             config_file_index = temp_args.index("-c") + 1
             if config_file_index < len(temp_args): config_file_arg = temp_args[config_file_index]
    except ValueError: pass

    # If config file path found early, try loading it to get log_dir if not specified otherwise
    if config_file_arg and log_dir_arg == "logs": # Only override default
         try:
             with open(config_file_arg, 'r', encoding='utf-8') as f:
                 temp_config = yaml.safe_load(f)
                 if isinstance(temp_config, dict) and temp_config.get('log_dir'):
                      log_dir_arg = temp_config['log_dir']
         except Exception:
              pass # Ignore errors here, full loading happens later

    # Setup logging BEFORE invoking Click command parsing
    configure_logging(verbose=verbose, log_dir=log_dir_arg)

    try:
        # Execute the Click CLI application
        # Pass context_settings={'auto_envvar_prefix': 'LOOKER'} if needed globally
        cli(prog_name="looker-validator")
    except SystemExit as e:
         if e.code is not None and e.code != 0: logger.info(f"Exiting with status code {e.code}")
         sys.exit(e.code)
    except Exception as e:
        print_fail(f"Unhandled Top-Level Error: {e}")
        logger.exception("Unhandled Top-Level Error:")
        sys.exit(99)


if __name__ == "__main__":
    main()
