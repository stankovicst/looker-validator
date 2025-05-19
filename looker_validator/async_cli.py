"""
Asynchronous command-line interface for Looker Validator.
"""

import asyncio
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple, cast

import click
import yaml

from looker_validator import __version__
from looker_validator.async_client import AsyncLookerClient
from looker_validator.exceptions import LookerValidatorException
from looker_validator.logger import setup_logger
from looker_validator.validators.assert_validator import AsyncAssertValidator
from looker_validator.validators.content_validator import AsyncContentValidator
from looker_validator.validators.lookml_validator import AsyncLookMLValidator
from looker_validator.validators.sql_validator import AsyncSQLValidator

logger = logging.getLogger(__name__)


class ConfigFileOption(click.Option):
    """Click option that loads values from a YAML config file."""
    
    def __init__(self, *args: Any, **kwargs: Any):
        """Initialize the option."""
        self.config_file_param = kwargs.pop('config_file_param', 'config_file')
        super().__init__(*args, **kwargs)
        
    def handle_parse_result(self, ctx: click.Context, opts: Dict[str, Any], args: List[Any]) -> Tuple[Any, List[Any]]:
        """Handle parsing results from config file."""
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


def env_var_option(*param_decls: Any, **kwargs: Any) -> click.Option:
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


def common_options(f: Any) -> Any:
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
    f = env_var_option(
        "--use-personal-branch",
        help="Use personal branch instead of temporary branch",
        is_flag=True,
        env_var="LOOKER_USE_PERSONAL_BRANCH"
    )(f)
    return f


@click.group()
@click.version_option(__version__)
def cli() -> None:
    """Looker Validator - A continuous integration tool for Looker and LookML.
    
    Run various validation checks on your Looker instance and LookML code:
      - assert: Run data tests defined in your LookML
      - content: Validate dashboards and looks for errors
      - lookml: Check LookML syntax and structure
      - sql: Validate SQL generation for all dimensions
    """
    pass


# Async implementation for connect command
async def _async_connect(ctx: click.Context, **kwargs: Any) -> None:
    """Async implementation of connect command."""
    # Set up logging
    log_level = logging.DEBUG if kwargs.get("verbose") else logging.INFO
    setup_logger(level=log_level)
    
    try:
        # Connect to Looker API
        base_url = kwargs["base_url"]
        client_id = kwargs["client_id"]
        client_secret = kwargs["client_secret"]
        port = kwargs.get("port")
        api_version = kwargs.get("api_version", "4.0")
        timeout = kwargs.get("timeout", 600)
        
        async with AsyncLookerClient(
            base_url=base_url,
            client_id=client_id,
            client_secret=client_secret,
            port=port,
            api_version=api_version,
            timeout=timeout
        ) as client:
            # Get Looker version
            version = await client.get_looker_release_version()
            
            click.echo(click.style(
                f"Connected to Looker version {version} using API {api_version}",
                fg="green"
            ))
            sys.exit(0)
    except Exception as e:
        click.echo(click.style(f"Error: {str(e)}", fg="red"))
        if kwargs.get("verbose"):
            import traceback
            click.echo(traceback.format_exc())
        sys.exit(1)


# Synchronous wrapper for connect command
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
    "--timeout",
    help="API request timeout in seconds",
    type=int,
    default=600,
    show_default=True,
    env_var="LOOKER_TIMEOUT"
)
@env_var_option(
    "--verbose", "-v",
    help="Enable verbose logging",
    is_flag=True,
    env_var="LOOKER_VERBOSE"
)
@click.pass_context
def connect(ctx: click.Context, **kwargs: Any) -> None:
    """Test connection to your Looker instance and API credentials."""
    asyncio.run(_async_connect(ctx, **kwargs))


# Async implementation for lookml command
async def _async_lookml(ctx: click.Context, **kwargs: Any) -> None:
    """Async implementation of lookml command."""
    # Set up logging
    log_level = logging.DEBUG if kwargs.get("verbose") else logging.INFO
    setup_logger(level=log_level)
    
    try:
        # Connect to Looker API
        async with AsyncLookerClient(
            base_url=kwargs["base_url"],
            client_id=kwargs["client_id"],
            client_secret=kwargs["client_secret"],
            port=kwargs.get("port"),
            api_version=kwargs.get("api_version", "4.0"),
            timeout=kwargs.get("timeout", 600)
        ) as client:
            # Create validator
            validator = AsyncLookMLValidator(
                client=client,
                project=kwargs["project"],
                branch=kwargs.get("branch"),
                commit_ref=kwargs.get("commit_ref"),
                remote_reset=kwargs.get("remote_reset", False),
                severity=kwargs.get("severity", "warning"),
                log_dir=kwargs.get("log_dir", "logs"),
                pin_imports=kwargs.get("pin_imports"),
                use_personal_branch=kwargs.get("use_personal_branch", False),
                incremental=kwargs.get("incremental", False),
                target=kwargs.get("target"),
            )
            
            # Run validation
            result = await validator.validate()
            
            # Output results
            _output_results(result)
            
            # Exit with appropriate code
            sys.exit(0 if result.status == "passed" else 1)
    except Exception as e:
        click.echo(click.style(f"Error: {str(e)}", fg="red"))
        if kwargs.get("verbose"):
            import traceback
            click.echo(traceback.format_exc())
        sys.exit(1)


# Synchronous wrapper for lookml command
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
    "--incremental",
    help="Only validate explores unique to the branch",
    is_flag=True,
    env_var="LOOKER_INCREMENTAL"
)
@env_var_option(
    "--target",
    help="Target branch for incremental comparison (default: production)",
    env_var="LOOKER_TARGET"
)
@click.pass_context
def lookml(ctx: click.Context, **kwargs: Any) -> None:
    """Validate LookML syntax and structure in your project."""
    asyncio.run(_async_lookml(ctx, **kwargs))


# Async implementation for content command
async def _async_content(ctx: click.Context, **kwargs: Any) -> None:
    """Async implementation of content command."""
    # Set up logging
    log_level = logging.DEBUG if kwargs.get("verbose") else logging.INFO
    setup_logger(level=log_level)
    
    try:
        # Connect to Looker API
        async with AsyncLookerClient(
            base_url=kwargs["base_url"],
            client_id=kwargs["client_id"],
            client_secret=kwargs["client_secret"],
            port=kwargs.get("port"),
            api_version=kwargs.get("api_version", "4.0"),
            timeout=kwargs.get("timeout", 600)
        ) as client:
            # Create validator
            validator = AsyncContentValidator(
                client=client,
                project=kwargs["project"],
                branch=kwargs.get("branch"),
                commit_ref=kwargs.get("commit_ref"),
                remote_reset=kwargs.get("remote_reset", False),
                explores=kwargs.get("explores"),
                folders=kwargs.get("folders"),
                exclude_personal=kwargs.get("exclude_personal", False),
                incremental=kwargs.get("incremental", False),
                target=kwargs.get("target"),
                log_dir=kwargs.get("log_dir", "logs"),
                pin_imports=kwargs.get("pin_imports"),
                use_personal_branch=kwargs.get("use_personal_branch", False),
            )
            
            # Run validation
            result = await validator.validate()
            
            # Output results
            _output_results(result)
            
            # Exit with appropriate code
            sys.exit(0 if result.status == "passed" else 1)
    except Exception as e:
        click.echo(click.style(f"Error: {str(e)}", fg="red"))
        if kwargs.get("verbose"):
            import traceback
            click.echo(traceback.format_exc())
        sys.exit(1)


# Synchronous wrapper for content command
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
    help="Folder IDs to include/exclude (e.g. '25', '-33')",
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
@click.pass_context
def content(ctx: click.Context, **kwargs: Any) -> None:
    """Validate dashboards and looks for errors in your Looker instance."""
    asyncio.run(_async_content(ctx, **kwargs))


# Async implementation for sql command
async def _async_sql(ctx: click.Context, **kwargs: Any) -> None:
    """Async implementation of sql command."""
    # Set up logging
    log_level = logging.DEBUG if kwargs.get("verbose") else logging.INFO
    setup_logger(level=log_level)
    
    try:
        # Connect to Looker API
        async with AsyncLookerClient(
            base_url=kwargs["base_url"],
            client_id=kwargs["client_id"],
            client_secret=kwargs["client_secret"],
            port=kwargs.get("port"),
            api_version=kwargs.get("api_version", "4.0"),
            timeout=kwargs.get("timeout", 600)
        ) as client:
            # Create validator
            validator = AsyncSQLValidator(
                client=client,
                project=kwargs["project"],
                branch=kwargs.get("branch"),
                commit_ref=kwargs.get("commit_ref"),
                remote_reset=kwargs.get("remote_reset", False),
                explores=kwargs.get("explores"),
                concurrency=kwargs.get("concurrency", 10),
                fail_fast=kwargs.get("fail_fast", False),
                profile=kwargs.get("profile", False),
                runtime_threshold=kwargs.get("runtime_threshold", 5),
                incremental=kwargs.get("incremental", False),
                target=kwargs.get("target"),
                ignore_hidden=kwargs.get("ignore_hidden", False),
                chunk_size=kwargs.get("chunk_size", 500),
                log_dir=kwargs.get("log_dir", "logs"),
                pin_imports=kwargs.get("pin_imports"),
                use_personal_branch=kwargs.get("use_personal_branch", False),
            )
            
            # Run validation
            result = await validator.validate()
            
            # Output results
            _output_results(result)
            
            # Exit with appropriate code
            sys.exit(0 if result.status == "passed" else 1)
    except Exception as e:
        click.echo(click.style(f"Error: {str(e)}", fg="red"))
        if kwargs.get("verbose"):
            import traceback
            click.echo(traceback.format_exc())
        sys.exit(1)


# Synchronous wrapper for sql command
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
    help="Stop after first error is encountered",
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
    help="Runtime threshold for profiler in seconds",
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
    help="Maximum dimensions per query",
    type=int,
    default=500,
    show_default=True,
    env_var="LOOKER_CHUNK_SIZE"
)
@click.pass_context
def sql(ctx: click.Context, **kwargs: Any) -> None:
    """Validate SQL generation for all dimensions in your LookML models."""
    asyncio.run(_async_sql(ctx, **kwargs))


# Async implementation for assert_command
async def _async_assert_command(ctx: click.Context, **kwargs: Any) -> None:
    """Async implementation of assert_command."""
    # Set up logging
    log_level = logging.DEBUG if kwargs.get("verbose") else logging.INFO
    setup_logger(level=log_level)
    
    try:
        # Connect to Looker API
        async with AsyncLookerClient(
            base_url=kwargs["base_url"],
            client_id=kwargs["client_id"],
            client_secret=kwargs["client_secret"],
            port=kwargs.get("port"),
            api_version=kwargs.get("api_version", "4.0"),
            timeout=kwargs.get("timeout", 600)
        ) as client:
            # Create validator
            validator = AsyncAssertValidator(
                client=client,
                project=kwargs["project"],
                branch=kwargs.get("branch"),
                commit_ref=kwargs.get("commit_ref"),
                remote_reset=kwargs.get("remote_reset", False),
                explores=kwargs.get("explores"),
                concurrency=kwargs.get("concurrency", 15),
                log_dir=kwargs.get("log_dir", "logs"),
                pin_imports=kwargs.get("pin_imports"),
                use_personal_branch=kwargs.get("use_personal_branch", False),
            )
            
            # Run validation
            result = await validator.validate()
            
            # Output results
            _output_results(result)
            
            # Exit with appropriate code
            sys.exit(0 if result.status == "passed" else 1)
    except Exception as e:
        click.echo(click.style(f"Error: {str(e)}", fg="red"))
        if kwargs.get("verbose"):
            import traceback
            click.echo(traceback.format_exc())
        sys.exit(1)


# Synchronous wrapper for assert_command
@cli.command(name="assert")
@common_options
@env_var_option(
    "--explores",
    help="Model/explore selectors (e.g., 'model_a/*', '-model_b/explore_c')",
    multiple=True,
    env_var="LOOKER_EXPLORES"
)
@env_var_option(
    "--concurrency",
    help="Number of concurrent tests",
    type=int,
    default=15,
    show_default=True,
    env_var="LOOKER_CONCURRENCY"
)
@click.pass_context
def assert_command(ctx: click.Context, **kwargs: Any) -> None:
    """Run data tests (assertions) defined in your LookML models."""
    asyncio.run(_async_assert_command(ctx, **kwargs))


def _parse_pin_imports(pin_imports: Optional[str]) -> Optional[Dict[str, str]]:
    """Parse pin_imports string into dictionary.
    
    Args:
        pin_imports: String in format "project:ref,project2:ref2"
        
    Returns:
        Dictionary mapping project names to refs
    """
    if not pin_imports:
        return None
        
    result = {}
    pairs = pin_imports.split(",")
    
    for pair in pairs:
        if ":" in pair:
            project, ref = pair.strip().split(":", 1)
            result[project] = ref
    
    return result


def _output_results(result: Any) -> None:
    """Output validation results using Rich for proper table formatting."""
    import json
    import os
    from rich.console import Console
    from rich.table import Table
    from rich.text import Text
    from rich.panel import Panel
    from rich.box import DOUBLE, SIMPLE, ROUNDED

    # Create console
    console = Console()
    
    # Print header
    console.print(f"\n[bold]{'=' * 80}[/bold]")
    console.print(f"[bold]🔍 {result.validator.upper()} VALIDATION RESULTS[/bold]")
    console.print(f"[bold]{'=' * 80}[/bold]")
    
    # Summary counts
    passed = sum(1 for t in result.tested if t.status == "passed")
    failed = sum(1 for t in result.tested if t.status == "failed")
    skipped = sum(1 for t in result.tested if t.status == "skipped")
    
    total_explores = len(result.tested)
    success_rate = f"{(passed / total_explores * 100):.1f}%" if total_explores > 0 else "N/A"
    
    # Summary table
    console.print(f"\n[cyan]SUMMARY[/cyan]")
    
    summary_table = Table(box=SIMPLE, show_header=True, header_style="bold")
    summary_table.add_column("Total Explores", style="white")
    summary_table.add_column("Passed", style="green")
    summary_table.add_column("Failed", style="red")
    summary_table.add_column("Skipped", style="yellow")
    summary_table.add_column("Success Rate", style="white")
    
    summary_table.add_row(
        str(total_explores),
        str(passed),
        str(failed),
        str(skipped),
        success_rate
    )
    
    console.print(summary_table)
    
    # Error details table - customize based on validator type
    if result.errors:
        console.print(f"\n[red]ERROR DETAILS[/red]")
        
        # Sort errors for better readability
        sorted_errors = sorted(result.errors, key=lambda e: (e.model, e.explore))
        
        # Use different table formats based on validator type
        if result.validator == "content":
            # For content validation, use content-specific columns
            error_table = Table(box=ROUNDED, show_header=True, header_style="bold", expand=True)
            error_table.add_column("Explorer", style="yellow", max_width=20)
            error_table.add_column("Content", style="blue", max_width=25)
            error_table.add_column("Error", max_width=40)
            error_table.add_column("URL", style="cyan", max_width=25)
            
            for error in sorted_errors:
                explorer = f"{error.model}/{error.explore}"
                
                # Get content metadata
                field_name = ""
                content_type = ""
                title = ""
                folder = ""
                url = ""
                tile_info = ""
                
                # Extract metadata from different possible locations
                if hasattr(error, 'metadata'):
                    metadata = error.metadata
                    field_name = metadata.get('field_name', '')
                    content_type = metadata.get('content_type', '')
                    title = metadata.get('title', '')
                    folder = metadata.get('folder', '')
                    url = metadata.get('url', '')
                    
                    # Format tile information if available
                    if metadata.get('tile_type') and metadata.get('tile_title'):
                        tile_info = f"{metadata.get('tile_type', '').title()}: {metadata.get('tile_title', '')}"
                
                # Create content display format
                content_display = ""
                if title:
                    content_display += f"[bold]{title}[/bold]"
                if folder:
                    content_display += f"\nFolder: {folder}"
                if tile_info:
                    content_display += f"\n{tile_info}"
                if field_name:
                    content_display += f"\nField: {field_name}"
                if content_type:
                    content_display += f"\nType: {content_type.title()}"
                
                # Add row to table
                error_table.add_row(
                    explorer,
                    content_display or "-",
                    error.message,
                    url or "-"
                )
        elif result.validator == "sql":
            # For SQL validation, use expanded format with full URLs
            for i, error in enumerate(sorted_errors):
                explorer = f"{error.model}/{error.explore}"
                
                # Get dimension name
                dimension = ""
                if hasattr(error, 'dimension') and error.dimension:
                    dimension = error.dimension
                elif hasattr(error, 'metadata') and error.metadata and 'dimension' in error.metadata:
                    dimension = error.metadata.get('dimension', '')
                
                # Get lookml_url 
                lookml_url = ""
                if hasattr(error, 'lookml_url') and error.lookml_url:
                    lookml_url = error.lookml_url
                elif hasattr(error, 'metadata') and error.metadata and 'lookml_url' in error.metadata:
                    lookml_url = error.metadata.get('lookml_url', '')
                
                # Create panel for each error
                error_panel = Panel(
                    f"[yellow]Explorer:[/yellow] {explorer}\n"
                    f"[blue]Dimension:[/blue] {dimension or '-'}\n"
                    f"[white]Error:[/white] {error.message}\n"
                    f"[cyan]LookML URL:[/cyan] {lookml_url or '-'}",
                    title=f"Error #{i+1}",
                    border_style="red"
                )
                console.print(error_panel)
        else:
            # For other validators, use the standard columns
            error_table = Table(box=ROUNDED, show_header=True, header_style="bold", expand=True)
            error_table.add_column("Explorer", style="yellow", max_width=25)
            error_table.add_column("Dimension", style="blue", max_width=25)
            error_table.add_column("Error", max_width=50)
            error_table.add_column("LookML", max_width=25)
            
            for error in sorted_errors:
                explorer = f"{error.model}/{error.explore}"
                
                # Get dimension from different possible attribute locations
                dimension = ""
                if hasattr(error, 'dimension') and error.dimension:
                    dimension = error.dimension
                elif hasattr(error, 'metadata') and error.metadata and 'dimension' in error.metadata:
                    dimension = error.metadata.get('dimension', '')
                
                # Get lookml_url from different possible attribute locations
                lookml_url = ""
                if hasattr(error, 'lookml_url') and error.lookml_url:
                    lookml_url = error.lookml_url
                elif hasattr(error, 'metadata') and error.metadata and 'lookml_url' in error.metadata:
                    lookml_url = error.metadata.get('lookml_url', '')
                
                # Add row to table
                error_table.add_row(
                    explorer,
                    dimension or "-",
                    error.message,
                    lookml_url or "-"
                )
        
        # Only print the table for non-SQL validators
        if result.validator != "sql":
            console.print(error_table)
    
    # Timing information
    if result.timing:
        console.print(f"\n[cyan]PERFORMANCE[/cyan]")
        
        timing_table = Table(box=SIMPLE, show_header=True, header_style="bold")
        timing_table.add_column("Operation", style="white")
        timing_table.add_column("Duration", style="white")
        
        for op, duration in result.timing.items():
            duration_str = f"{duration:.2f}s"
            timing_table.add_row(op, duration_str)
        
        console.print(timing_table)
    
    # Final status
    console.print(f"\n[bold]{'=' * 80}[/bold]")
    status_icon = "✅" if result.status == "passed" else "❌"
    status_color = "green" if result.status == "passed" else "red"
    status_text = "PASSED" if result.status == "passed" else "FAILED"
    
    console.print(f"{status_icon} [{status_color}]Status: {status_text}[/{status_color}]")
    console.print(f"[bold]{'=' * 80}[/bold]\n")
    
    # Save results to JSON file
    log_dir = "logs"  # Default log directory
    os.makedirs(log_dir, exist_ok=True)
    
    result_file = os.path.join(log_dir, f"{result.validator}_results.json")
    with open(result_file, "w") as f:
        json.dump(result.to_dict(), f, indent=2)
        
    console.print(f"Detailed results saved to {result_file}")


def main() -> None:
    """Main entry point."""
    # Handle KeyboardInterrupt gracefully
    try:
        # Use standard Click call for the CLI group
        cli()
    except KeyboardInterrupt:
        click.echo("\nOperation cancelled by user.")
        sys.exit(1)
    except LookerValidatorException as e:
        click.echo(click.style(f"Error: {e}", fg="red"))
        sys.exit(e.exit_code)
    except Exception as e:
        click.echo(click.style(f"Unexpected error: {e}", fg="red"))
        sys.exit(1)


if __name__ == "__main__":
    main()