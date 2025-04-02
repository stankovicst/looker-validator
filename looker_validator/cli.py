"""
Command-line interface for Looker Validator.
"""

import os
import sys
import logging
import yaml
import click
from typing import List, Optional, Any, Dict

from looker_validator.config import Config
from looker_validator.connection import LookerConnection
from looker_validator.validators.sql_validator import SQLValidator
from looker_validator.validators.content_validator import ContentValidator
from looker_validator.validators.assert_validator import AssertValidator
from looker_validator.validators.lookml_validator import LookMLValidator

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("looker_validator")


class ConfigFileOption(click.Option):
    """Click option that loads values from a YAML config file."""
    
    def __init__(self, *args, **kwargs):
        self.config_file_param = kwargs.pop('config_file_param', 'config_file')
        super().__init__(*args, **kwargs)
        
    def handle_parse_result(self, ctx, opts, args):
        # Check if the config file parameter is provided
        config_file = opts.get(self.config_file_param)
        if config_file:
            # Load the config file
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            
            # Update options with config file values if not already set
            for key, value in config.items():
                if key not in opts or opts[key] is None:
                    opts[key] = value
        
        return super().handle_parse_result(ctx, opts, args)


def env_var_option(*param_decls, **kwargs):
    """Decorator for options that support environment variables."""
    env_var = kwargs.pop('env_var', None)
    
    if env_var and env_var in os.environ:
        kwargs['default'] = os.environ[env_var]
        if kwargs.get('required'):
            kwargs['required'] = False
    
    # Add env var to help text
    if env_var:
        help_text = kwargs.get('help', '')
        kwargs['help'] = f"{help_text} [env: {env_var}]"
    
    # Make it work with config files too
    kwargs['cls'] = ConfigFileOption
    
    return click.option(*param_decls, **kwargs)


def common_options(f):
    """Common options for all commands."""
    f = click.option(
        "--config-file", "-c",
        help="Path to YAML config file",
        type=click.Path(exists=True, dir_okay=False)
    )(f)
    f = env_var_option(
        "--base-url",
        help="Looker instance URL (e.g., https://company.looker.com)",
        env_var="LOOKER_BASE_URL",
        required=True
    )(f)
    f = env_var_option(
        "--client-id",
        help="Looker API client ID",
        env_var="LOOKER_CLIENT_ID",
        required=True
    )(f)
    f = env_var_option(
        "--client-secret",
        help="Looker API client secret",
        env_var="LOOKER_CLIENT_SECRET",
        required=True
    )(f)
    f = env_var_option(
        "--port",
        help="Looker API port",
        type=int,
        env_var="LOOKER_PORT"
    )(f)
    f = env_var_option(
        "--api-version",
        help="Looker API version",
        default="4.0",
        show_default=True,
        env_var="LOOKER_API_VERSION"
    )(f)
    f = env_var_option(
        "--project",
        help="Looker project name",
        required=True,
        env_var="LOOKER_PROJECT"
    )(f)
    f = env_var_option(
        "--branch",
        help="Git branch name (default: production)",
        env_var="LOOKER_GIT_BRANCH"
    )(f)
    f = env_var_option(
        "--commit-ref",
        help="Git commit reference",
        env_var="LOOKER_COMMIT_REF"
    )(f)
    f = env_var_option(
        "--remote-reset",
        help="Reset branch to remote state",
        is_flag=True,
        env_var="LOOKER_REMOTE_RESET"
    )(f)
    f = env_var_option(
        "--log-dir",
        help="Directory for log files",
        default="logs",
        show_default=True,
        env_var="LOOKER_LOG_DIR"
    )(f)
    f = env_var_option(
        "--verbose", "-v",
        help="Enable verbose logging",
        is_flag=True,
        env_var="LOOKER_VERBOSE"
    )(f)
    f = env_var_option(
        "--pin-imports",
        help="Pin imported projects to specific refs (format: 'project:ref,project2:ref2')",
        env_var="LOOKER_PIN_IMPORTS"
    )(f)
    f = env_var_option(
        "--timeout",
        help="API request timeout in seconds",
        type=int,
        default=600,
        show_default=True,
        env_var="LOOKER_TIMEOUT"
    )(f)
    return f


# Create the CLI group
@click.group()
def cli():
    """Looker Validator - A continuous integration tool for Looker and LookML."""
    pass


@cli.command()
@click.option(
    "--config-file", "-c",
    help="Path to YAML config file",
    type=click.Path(exists=True, dir_okay=False)
)
@env_var_option(
    "--base-url",
    help="Looker instance URL (e.g., https://company.looker.com)",
    env_var="LOOKER_BASE_URL",
    required=True
)
@env_var_option(
    "--client-id",
    help="Looker API client ID",
    env_var="LOOKER_CLIENT_ID",
    required=True
)
@env_var_option(
    "--client-secret",
    help="Looker API client secret",
    env_var="LOOKER_CLIENT_SECRET",
    required=True
)
@env_var_option(
    "--port",
    help="Looker API port",
    type=int,
    env_var="LOOKER_PORT"
)
@env_var_option(
    "--api-version",
    help="Looker API version",
    default="4.0",
    show_default=True,
    env_var="LOOKER_API_VERSION"
)
@env_var_option(
    "--project",
    help="Looker project name",
    env_var="LOOKER_PROJECT"
)  # Note: Not required for connect
@env_var_option(
    "--timeout",
    help="API request timeout in seconds",
    type=int,
    default=600,
    show_default=True,
    env_var="LOOKER_TIMEOUT"
)
def connect(**kwargs):
    """Test connection to Looker API."""
    try:
        # Initialize config
        config = Config(**kwargs)
        
        # Initialize connection
        connection = LookerConnection(
            base_url=config.base_url,
            client_id=config.client_id,
            client_secret=config.client_secret,
            port=config.port,
            api_version=config.api_version,
            timeout=config.timeout,
        )
        
        # Test connection
        success, message = connection.test_connection()
        
        if success:
            click.echo(click.style(message, fg="green"))
            sys.exit(0)
        else:
            click.echo(click.style(message, fg="red"))
            sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"Error: {str(e)}", fg="red"))
        if kwargs.get("verbose"):
            import traceback
            click.echo(traceback.format_exc())
        sys.exit(1)


@cli.command()
@common_options
@env_var_option(
    "--explores",
    help="Model/explore selectors (e.g., 'model_a/*', '-model_b/explore_c')",
    multiple=True,
    env_var="LOOKER_EXPLORES"
)
@env_var_option(
    "--concurrency",
    help="Number of concurrent queries",
    type=int,
    default=10,
    show_default=True,
    env_var="LOOKER_CONCURRENCY"
)
@env_var_option(
    "--fail-fast",
    help="Only run explore-level queries",
    is_flag=True,
    env_var="LOOKER_FAIL_FAST"
)
@env_var_option(
    "--profile", "-p",
    help="Profile query execution time",
    is_flag=True,
    env_var="LOOKER_PROFILE"
)
@env_var_option(
    "--runtime-threshold",
    help="Runtime threshold for profiler (seconds)",
    type=int,
    default=5,
    show_default=True,
    env_var="LOOKER_RUNTIME_THRESHOLD"
)
@env_var_option(
    "--incremental",
    help="Only show errors unique to the branch",
    is_flag=True,
    env_var="LOOKER_INCREMENTAL"
)
@env_var_option(
    "--target",
    help="Target branch for incremental comparison (default: production)",
    env_var="LOOKER_TARGET"
)
@env_var_option(
    "--ignore-hidden",
    help="Ignore hidden dimensions",
    is_flag=True,
    env_var="LOOKER_IGNORE_HIDDEN"
)
@env_var_option(
    "--chunk-size",
    help="Maximum number of dimensions per query",
    type=int,
    default=500,
    show_default=True,
    env_var="LOOKER_CHUNK_SIZE"
)
def sql(**kwargs):
    """Run SQL validation on Looker project."""
    try:
        # Initialize config
        config = Config(**kwargs)
        
        # Initialize connection
        connection = LookerConnection(
            base_url=config.base_url,
            client_id=config.client_id,
            client_secret=config.client_secret,
            port=config.port,
            api_version=config.api_version,
            timeout=config.timeout,
        )
        
        # Set log level if verbose
        if kwargs.get("verbose"):
            logging.getLogger().setLevel(logging.DEBUG)
        
        # Initialize and run validator
        validator = SQLValidator(
            connection=connection,
            project=config.project,
            branch=config.branch,
            commit_ref=config.commit_ref,
            remote_reset=config.remote_reset,
            explores=config.explores,
            concurrency=config.concurrency,
            fail_fast=config.fail_fast,
            profile=config.profile,
            runtime_threshold=config.runtime_threshold,
            incremental=config.incremental,
            target=config.target,
            ignore_hidden=config.ignore_hidden,
            chunk_size=config.chunk_size,
            log_dir=config.log_dir,
            pin_imports=config.pin_imports,
        )
        
        success = validator.validate()
        
        if success:
            sys.exit(0)
        else:
            sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"Error: {str(e)}", fg="red"))
        if kwargs.get("verbose"):
            import traceback
            click.echo(traceback.format_exc())
        sys.exit(1)


@cli.command()
@common_options
@env_var_option(
    "--explores",
    help="Model/explore selectors (e.g., 'model_a/*', '-model_b/explore_c')",
    multiple=True,
    env_var="LOOKER_EXPLORES"
)
@env_var_option(
    "--folders",
    help="Folder IDs to include/exclude (e.g., '25', '-33')",
    multiple=True,
    env_var="LOOKER_FOLDERS"
)
@env_var_option(
    "--exclude-personal",
    help="Exclude content in personal folders",
    is_flag=True,
    env_var="LOOKER_EXCLUDE_PERSONAL"
)
@env_var_option(
    "--incremental",
    help="Only show errors unique to the branch",
    is_flag=True,
    env_var="LOOKER_INCREMENTAL"
)
@env_var_option(
    "--target",
    help="Target branch for incremental comparison (default: production)",
    env_var="LOOKER_TARGET"
)
def content(**kwargs):
    """Run content validation on Looker project."""
    try:
        # Initialize config
        config = Config(**kwargs)
        
        # Initialize connection
        connection = LookerConnection(
            base_url=config.base_url,
            client_id=config.client_id,
            client_secret=config.client_secret,
            port=config.port,
            api_version=config.api_version,
            timeout=config.timeout,
        )
        
        # Set log level if verbose
        if kwargs.get("verbose"):
            logging.getLogger().setLevel(logging.DEBUG)
        
        # Initialize and run validator
        validator = ContentValidator(
            connection=connection,
            project=config.project,
            branch=config.branch,
            commit_ref=config.commit_ref,
            remote_reset=config.remote_reset,
            explores=config.explores,
            folders=config.folders,
            exclude_personal=config.exclude_personal,
            incremental=config.incremental,
            target=config.target,
            log_dir=config.log_dir,
            pin_imports=config.pin_imports,
        )
        
        success = validator.validate()
        
        if success:
            sys.exit(0)
        else:
            sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"Error: {str(e)}", fg="red"))
        if kwargs.get("verbose"):
            import traceback
            click.echo(traceback.format_exc())
        sys.exit(1)


@cli.command(name="assert")  # Use name parameter to override the function name
@common_options
@env_var_option(
    "--explores",
    help="Model/explore selectors (e.g., 'model_a/*', '-model_b/explore_c')",
    multiple=True,
    env_var="LOOKER_EXPLORES"
)
def assert_command(**kwargs):
    """Run LookML data tests on Looker project."""
    try:
        # Initialize config
        config = Config(**kwargs)
        
        # Initialize connection
        connection = LookerConnection(
            base_url=config.base_url,
            client_id=config.client_id,
            client_secret=config.client_secret,
            port=config.port,
            api_version=config.api_version,
            timeout=config.timeout,
        )
        
        # Set log level if verbose
        if kwargs.get("verbose"):
            logging.getLogger().setLevel(logging.DEBUG)
        
        # Initialize and run validator
        validator = AssertValidator(
            connection=connection,
            project=config.project,
            branch=config.branch,
            commit_ref=config.commit_ref,
            remote_reset=config.remote_reset,
            explores=config.explores,
            log_dir=config.log_dir,
            pin_imports=config.pin_imports,
        )
        
        success = validator.validate()
        
        if success:
            sys.exit(0)
        else:
            sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"Error: {str(e)}", fg="red"))
        if kwargs.get("verbose"):
            import traceback
            click.echo(traceback.format_exc())
        sys.exit(1)


@cli.command()
@common_options
@env_var_option(
    "--severity",
    help="Severity threshold (info, warning, error)",
    type=click.Choice(["info", "warning", "error"]),
    default="warning",
    show_default=True,
    env_var="LOOKER_SEVERITY"
)
def lookml(**kwargs):
    """Run LookML validation on Looker project."""
    try:
        # Initialize config
        config = Config(**kwargs)
        
        # Initialize connection
        connection = LookerConnection(
            base_url=config.base_url,
            client_id=config.client_id,
            client_secret=config.client_secret,
            port=config.port,
            api_version=config.api_version,
            timeout=config.timeout,
        )
        
        # Set log level if verbose
        if kwargs.get("verbose"):
            logging.getLogger().setLevel(logging.DEBUG)
        
        # Initialize and run validator
        validator = LookMLValidator(
            connection=connection,
            project=config.project,
            branch=config.branch,
            commit_ref=config.commit_ref,
            remote_reset=config.remote_reset,
            severity=config.severity,
            log_dir=config.log_dir,
            pin_imports=config.pin_imports,
        )
        
        success = validator.validate()
        
        if success:
            sys.exit(0)
        else:
            sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"Error: {str(e)}", fg="red"))
        if kwargs.get("verbose"):
            import traceback
            click.echo(traceback.format_exc())
        sys.exit(1)


def main():
    """Main entry point for the CLI."""
    # Check for NO_COLOR environment variable
    if os.environ.get("NO_COLOR"):
        # Disable colors in click
        click.disable_colors = True
    
    try:
        cli()
    except Exception as e:
        click.echo(click.style(f"Unhandled error: {str(e)}", fg="red"))
        sys.exit(1)


if __name__ == "__main__":
    main()