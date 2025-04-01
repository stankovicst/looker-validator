"""
Command-line interface for Looker Validator with enhanced environment variable support.
"""

import os
import sys
import logging
import click
from typing import List, Optional, Any, Callable

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


# Enhanced Click Option classes for environment variables
class EnvVarOption(click.Option):
    """Click option that can be set from an environment variable.
    
    The option prioritizes: CLI argument > Environment Variable > Default
    """
    
    def __init__(self, *args, **kwargs):
        # Extract env_var from kwargs
        self.env_var = kwargs.pop('env_var', None)
        
        # Get the default value from environment if available
        if self.env_var and self.env_var in os.environ:
            kwargs['default'] = os.environ[self.env_var]
            
        # If a default exists and the option is required, make it not required
        if kwargs.get('default') is not None and kwargs.get('required'):
            kwargs['required'] = False
            
        super().__init__(*args, **kwargs)
        
    def get_help_record(self, ctx):
        """Add environment variable info to help text."""
        help_text = super().get_help_record(ctx)
        
        if help_text and self.env_var:
            help_text = (
                help_text[0], 
                f"{help_text[1]} [env: {self.env_var}]"
            )
            
        return help_text


class EnvVarFlag(click.Option):
    """Click boolean flag that can be set from an environment variable."""
    
    def __init__(self, *args, **kwargs):
        # Extract env_var from kwargs
        self.env_var = kwargs.pop('env_var', None)
        
        # Set is_flag to True to make it a boolean flag
        kwargs['is_flag'] = True
        
        # If env var is set, use its value
        if self.env_var and self.env_var in os.environ:
            env_value = os.environ[self.env_var].lower()
            if env_value in ('true', '1', 'yes'):
                kwargs['default'] = True
            elif env_value in ('false', '0', 'no'):
                kwargs['default'] = False
        
        super().__init__(*args, **kwargs)
        
    def get_help_record(self, ctx):
        """Add environment variable info to help text."""
        help_text = super().get_help_record(ctx)
        
        if help_text and self.env_var:
            help_text = (
                help_text[0], 
                f"{help_text[1]} [env: {self.env_var}]"
            )
            
        return help_text


def env_var_option(*param_decls, **attrs):
    """Decorator to create a click option that supports environment variables."""
    return click.option(*param_decls, cls=EnvVarOption, **attrs)


def env_var_flag(*param_decls, **attrs):
    """Decorator to create a click flag that supports environment variables."""
    return click.option(*param_decls, cls=EnvVarFlag, **attrs)


# Shared command options with environment variable support
def common_options(f: Callable) -> Callable:
    """Common options for all commands with environment variable support."""
    f = env_var_option(
        "--config-file", "-c",
        help="Path to YAML config file",
        type=click.Path(exists=True, dir_okay=False),
        env_var="LOOKER_CONFIG_FILE"
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
    f = env_var_flag(
        "--remote-reset",
        help="Reset branch to remote state",
        env_var="LOOKER_REMOTE_RESET"
    )(f)
    f = env_var_option(
        "--log-dir",
        help="Directory for log files",
        default="logs",
        show_default=True,
        env_var="LOOKER_LOG_DIR"
    )(f)
    f = env_var_flag(
        "--verbose", "-v",
        help="Enable verbose logging",
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
@click.version_option(message="Looker Validator v%(version)s")
def cli():
    """Looker Validator - A continuous integration tool for Looker and LookML."""
    pass


@cli.command()
@env_var_option(
    "--config-file", "-c",
    help="Path to YAML config file",
    type=click.Path(exists=True, dir_okay=False),
    env_var="LOOKER_CONFIG_FILE"
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
@env_var_flag(
    "--fail-fast",
    help="Only run explore-level queries",
    env_var="LOOKER_FAIL_FAST"
)
@env_var_flag(
    "--profile", "-p",
    help="Profile query execution time",
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
@env_var_flag(
    "--incremental",
    help="Only show errors unique to the branch",
    env_var="LOOKER_INCREMENTAL"
)
@env_var_option(
    "--target",
    help="Target branch for incremental comparison (default: production)",
    env_var="LOOKER_TARGET"
)
@env_var_flag(
    "--ignore-hidden",
    help="Ignore hidden dimensions",
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
@env_var_flag(
    "--exclude-personal",
    help="Exclude content in personal folders",
    env_var="LOOKER_EXCLUDE_PERSONAL"
)
@env_var_flag(
    "--incremental",
    help="Only show errors unique to the branch",
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
@env_var_option(
    "--concurrency",
    help="Number of concurrent test executions",
    type=int,
    default=15,
    show_default=True,
    env_var="LOOKER_CONCURRENCY"
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
            concurrency=kwargs.get("concurrency", 15)
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
@env_var_option(
    "--timeout",
    help="Request timeout in seconds (handled by connection settings)",
    type=int,
    default=7200,  # 2 hour default for large projects
    show_default=True,
    env_var="LOOKER_LOOKML_TIMEOUT"
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