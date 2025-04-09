# looker_validator/config.py
"""
Configuration handling for Looker Validator.
Defines DEFAULT_CONCURRENCY and uses custom exceptions.
"""

import os
import logging
from typing import Dict, List, Optional, Any, Union, Tuple
import yaml
from pathlib import Path # Import Path

# Use central exceptions
from .exceptions import ConfigError

logger = logging.getLogger(__name__)

# Define the default concurrency constant centrally
DEFAULT_CONCURRENCY = 10 # Default number of concurrent workers/tests

class Config:
    """
    Configuration handler for Looker Validator.
    Loads settings from parameters, environment variables, and YAML file with defined precedence.
    """

    # Define attributes with type hints for clarity
    base_url: Optional[str]
    client_id: Optional[str]
    client_secret: Optional[str]
    port: Optional[int]
    api_version: str
    project: Optional[str]
    branch: Optional[str]
    timeout: int
    commit_ref: Optional[str]
    remote_reset: bool
    explores: List[str]
    folders: List[str]
    exclude_personal: bool # Internal config name remains exclude_personal
    incremental: bool
    target: Optional[str]
    concurrency: int
    severity: str # For LookML validator
    # Keep other options commented out or remove if not used
    # fail_fast: bool
    # profile: bool
    # max_depth: int
    # runtime_threshold: int
    pin_imports: Optional[str]
    # ignore_hidden: bool
    log_dir: str
    # chunk_size: int
    verbose: bool
    additional_config: Dict[str, Any] # Store extra config from file

    def __init__(
        self,
        config_file: Optional[str] = None,
        # Include all potential kwargs from CLI options for type checking support
        base_url: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        port: Optional[int] = None,
        api_version: Optional[str] = None,
        project: Optional[str] = None,
        branch: Optional[str] = None,
        commit_ref: Optional[str] = None,
        remote_reset: Optional[bool] = None, # CLI passes bool directly
        timeout: Optional[int] = None,
        explores: Optional[Tuple[str]] = None, # Click passes multiple options as tuple
        folders: Optional[Tuple[str]] = None,  # Click passes multiple options as tuple
        exclude_personal: Optional[bool] = None, # This value will be derived from --include-personal in cli.py
        incremental: Optional[bool] = None,
        target: Optional[str] = None,
        concurrency: Optional[int] = None,
        severity: Optional[str] = None,
        # fail_fast: Optional[bool] = None,
        # profile: Optional[bool] = None,
        # max_depth: Optional[int] = None,
        # runtime_threshold: Optional[int] = None,
        pin_imports: Optional[str] = None,
        # ignore_hidden: Optional[bool] = None,
        log_dir: Optional[str] = None,
        # chunk_size: Optional[int] = None,
        verbose: Optional[bool] = None,
        **kwargs # Allow passthrough, though specific args are preferred
    ):
        """Initialize configuration from file, environment variables, and parameters.

        Precedence order: Command-line parameters > Environment variables > Config file > Defaults.

        Args:
            config_file: Path to YAML config file.
            **kwargs: Configuration parameters corresponding to CLI options and env vars.
        """
        config_from_file = {}
        if config_file:
            config_from_file = self._load_config_file(config_file)

        # Helper to get value with precedence: param -> env -> file -> default
        def _get_value(param_val: Optional[Any], env_key: str, file_key: str, default: Any, type_converter: Optional[callable] = None):
            val = param_val
            env_val = None # Track if value came from env
            if val is None:
                env_val = os.environ.get(env_key)
                if env_val is not None:
                    val = env_val
                else:
                    val = config_from_file.get(file_key, default)
            # IMPORTANT: Check val against None explicitly, because False is a valid value
            if val is None:
                 val = default

            if type_converter and val is not None:
                try:
                    # Handle boolean conversion from string carefully
                    if type_converter == bool and isinstance(val, str):
                         val = val.lower() in ('true', '1', 'yes', 'y')
                    # Handle conversion for lists from comma-separated strings (for env vars)
                    elif type_converter == list and isinstance(val, str):
                        val = [item.strip() for item in val.split(',') if item.strip()]
                    # Handle conversion for lists from tuples (from Click multiple=True)
                    elif type_converter == list and isinstance(val, tuple):
                        val = list(val)
                    else:
                         val = type_converter(val)
                except (ValueError, TypeError) as e:
                    source = f"env var '{env_key}'" if env_val is not None else f"config file key '{file_key}' or default"
                    logger.warning(f"Invalid value '{val}' for {file_key} (from {source}). Using default '{default}'. Error: {e}")
                    val = default
            # Ensure lists are returned as lists, even if empty or default
            if type_converter == list and val is None:
                val = []
            return val

        # Resolve core connection settings
        self.base_url = _get_value(base_url, "LOOKER_BASE_URL", "base_url", None)
        self.client_id = _get_value(client_id, "LOOKER_CLIENT_ID", "client_id", None)
        self.client_secret = _get_value(client_secret, "LOOKER_CLIENT_SECRET", "client_secret", None)
        self.port = _get_value(port, "LOOKER_PORT", "port", None, int)
        self.api_version = _get_value(api_version, "LOOKER_API_VERSION", "api_version", "4.0", str)
        self.timeout = _get_value(timeout, "LOOKER_TIMEOUT", "timeout", 600, int)

        # Resolve project/branch settings
        self.project = _get_value(project, "LOOKER_PROJECT", "project", None)
        self.branch = _get_value(branch, "LOOKER_GIT_BRANCH", "branch", None)
        self.commit_ref = _get_value(commit_ref, "LOOKER_COMMIT_REF", "commit_ref", None)
        self.remote_reset = _get_value(remote_reset, "LOOKER_REMOTE_RESET", "remote_reset", False, bool)
        self.pin_imports = _get_value(pin_imports, "LOOKER_PIN_IMPORTS", "pin_imports", None)

        # Resolve validator-specific settings
        # Use list as the type converter for explores and folders
        self.explores = _get_value(explores, "LOOKER_EXPLORES", "explores", [], list)
        self.folders = _get_value(folders, "LOOKER_FOLDERS", "folders", [], list)

        # --- ENSURE CORRECT DEFAULT ---
        # The default value for exclude_personal should be True.
        # The actual value passed in `exclude_personal` param comes from cli.py
        # where it's derived from the --include-personal flag.
        # The env var name corresponds to the internal meaning (exclusion).
        self.exclude_personal = _get_value(exclude_personal, "LOOKER_EXCLUDE_PERSONAL", "exclude_personal", True, bool)
        # --- END ENSURE CORRECT DEFAULT ---

        self.incremental = _get_value(incremental, "LOOKER_INCREMENTAL", "incremental", False, bool)
        self.target = _get_value(target, "LOOKER_TARGET", "target", None)
        # Use the defined constant for the default concurrency
        self.concurrency = _get_value(concurrency, "LOOKER_CONCURRENCY", "concurrency", DEFAULT_CONCURRENCY, int)
        self.severity = _get_value(severity, "LOOKER_SEVERITY", "severity", "warning", str)
        # Keep other SQL options commented out unless re-added
        # self.fail_fast = _get_value(fail_fast, "LOOKER_FAIL_FAST", "fail_fast", False, bool)
        # self.profile = _get_value(profile, "LOOKER_PROFILE", "profile", False, bool)
        # self.max_depth = _get_value(max_depth, "LOOKER_MAX_DEPTH", "max_depth", 5, int)
        # self.runtime_threshold = _get_value(runtime_threshold, "LOOKER_RUNTIME_THRESHOLD", "runtime_threshold", 5, int)
        # self.ignore_hidden = _get_value(ignore_hidden, "LOOKER_IGNORE_HIDDEN", "ignore_hidden", False, bool)
        # self.chunk_size = _get_value(chunk_size, "LOOKER_CHUNK_SIZE", "chunk_size", 500, int)

        # General settings
        self.log_dir = _get_value(log_dir, "LOOKER_LOG_DIR", "log_dir", "logs", str)
        self.verbose = _get_value(verbose, "LOOKER_VERBOSE", "verbose", False, bool)

        # Store any additional configuration from file not explicitly handled
        self.additional_config = {
            k: v for k, v in config_from_file.items()
            if k not in self.__dict__ # Check against resolved attributes
        }

        self._validate_config()
        logger.debug(f"Configuration loaded: {self.as_dict(mask_secrets=True)}")


    def _load_config_file(self, config_file_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        logger.debug(f"Attempting to load configuration from file: {config_file_path}")
        try:
            # Ensure the path exists before trying to open
            if not Path(config_file_path).is_file():
                 logger.warning(f"Config file not found: {config_file_path}")
                 return {}

            with open(config_file_path, "r", encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
                if config_data and isinstance(config_data, dict):
                    logger.debug(f"Successfully loaded config file: {config_file_path}")
                    return config_data
                elif config_data is None:
                    logger.debug(f"Config file '{config_file_path}' is empty.")
                    return {}
                else:
                     logger.warning(f"Config file '{config_file_path}' does not contain a valid YAML dictionary.")
                     return {}
        except FileNotFoundError: # Should be caught by Path check, but keep for safety
            logger.warning(f"Config file not found: {config_file_path}")
            return {}
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML config file {config_file_path}: {e}", exc_info=True)
            raise ConfigError(f"Error parsing configuration file '{config_file_path}': {e}") from e
        except Exception as e:
            logger.error(f"Failed to load config file {config_file_path}: {e}", exc_info=True)
            raise ConfigError(f"Failed to load configuration file '{config_file_path}': {e}") from e


    def _validate_config(self):
        """Validate that required configuration values are present and consistent."""
        required_fields = {
            "base_url": "Looker instance URL (--base-url or LOOKER_BASE_URL)",
            "client_id": "Looker API client ID (--client-id or LOOKER_CLIENT_ID)",
            "client_secret": "Looker API client secret (--client-secret or LOOKER_CLIENT_SECRET)",
        }
        missing_fields = []
        for field, description in required_fields.items():
            if not getattr(self, field):
                missing_fields.append(f"- {description}")

        # Project is required by most validators, but not 'connect' command.
        # We check for project within the commands that need it now.

        if missing_fields:
            error_msg = "Missing required configuration values:\n" + "\n".join(missing_fields)
            raise ConfigError(error_msg) # Use specific exception

        # Validate mutually exclusive options
        if self.commit_ref and self.branch:
             logger.warning("Both --branch and --commit-ref specified. --commit-ref takes precedence.")
             # No need to modify self.branch here, BranchManager init handles precedence

        if self.commit_ref and self.remote_reset:
             raise ConfigError("Cannot use both --commit-ref and --remote-reset simultaneously.")

        # Validate integer ranges
        if self.concurrency <= 0:
             logger.warning(f"Concurrency must be positive, using default {DEFAULT_CONCURRENCY}.")
             self.concurrency = DEFAULT_CONCURRENCY
        if self.timeout <= 0:
             logger.warning("Timeout must be positive, using default 600.")
             self.timeout = 600
        # Add other range validations if needed (chunk_size, max_depth, etc.)

        logger.debug("Configuration validation passed.")


    def as_dict(self, mask_secrets: bool = False) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        config_dict = {
            k: v for k, v in self.__dict__.items()
            if not k.startswith("_") and k != "additional_config"
        }
        if mask_secrets:
            if 'client_secret' in config_dict and config_dict['client_secret']:
                config_dict['client_secret'] = f"{str(config_dict['client_secret'])[:4]}..."
        return config_dict

    def __str__(self) -> str:
        """String representation of configuration (with secrets masked)."""
        config_dict = self.as_dict(mask_secrets=True)
        config_str = "Configuration:\n"
        for key, value in sorted(config_dict.items()):
            config_str += f"  {key}: {value}\n"
        return config_str.strip()
