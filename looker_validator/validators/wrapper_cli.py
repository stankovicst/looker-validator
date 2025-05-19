"""
Wrapper around async_cli to provide synchronous entry points for Click.
"""

import asyncio
import sys
from typing import Any

import click

from looker_validator.async_cli import cli as async_cli
from looker_validator.exceptions import LookerValidatorException

# Create a new Click group for synchronous commands
@click.group(help=async_cli.help)
@click.version_option()
def cli():
    """Looker Validator - A continuous integration tool for Looker and LookML."""
    pass

# For each command in the async CLI, create a synchronous wrapper
for cmd_name in async_cli.commands:
    cmd = async_cli.commands[cmd_name]
    
    # Create a wrapper function with the same name
    def create_wrapper(async_cmd):
        @click.pass_context
        def wrapper(ctx, **kwargs):
            # Transfer parameters from ctx to the async command
            try:
                asyncio.run(async_cmd.callback(**kwargs))
            except KeyboardInterrupt:
                click.echo("\nOperation cancelled by user.")
                sys.exit(1)
            except LookerValidatorException as e:
                click.echo(click.style(f"Error: {e}", fg="red"))
                sys.exit(e.exit_code)
            except Exception as e:
                click.echo(click.style(f"Unexpected error: {e}", fg="red"))
                if kwargs.get("verbose"):
                    import traceback
                    click.echo(traceback.format_exc())
                sys.exit(1)
        
        # Make the wrapper look like the original
        wrapper.__name__ = async_cmd.name
        wrapper.__doc__ = async_cmd.help
        return wrapper

    # Create the wrapper function
    wrapper_func = create_wrapper(cmd)
    
    # Add the command to the CLI group with all the same parameters
    cli.command(name=cmd.name, help=cmd.help)(wrapper_func)

def main():
    """Main entry point."""
    cli()

if __name__ == "__main__":
    main()