"""
Configuration handling for Looker Validator.
"""

import os
import logging
from typing import Dict, List, Optional, Any, Union
import yaml

logger = logging.getLogger(__name__)

class Config:
    """Configuration handler for Looker Validator."""

    def __init__(
        self,
        config_file: Optional[str] = None,
        base_url: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        port: Optional[int] = None,
        api_version: Optional[str] = None,
        project: Optional[str] = None,
        branch: Optional[str] = None,
        timeout: Optional[int] = None,
        **kwargs
    ):
        """Initialize configuration from file, environment variables, and parameters.

        Args:
            config_file: Path to YAML config file
            base_url: Looker instance URL
            client_id: Looker API client ID
            client_secret: Looker API client secret
            port: Looker API port
            api_version: Looker API version
            project: Looker project name
            branch: Git branch name
            timeout: API request timeout in seconds
            **kwargs: Additional configuration parameters
        """
        # Load config file if provided
        config_from_file = {}
        if config_file:
            config_from_file = self._load_config_file(config_file)
            
        # Set configuration with precedence: params > env vars > config file
        self.base_url = base_url or os.environ.get("LOOKER_BASE_URL") or config_from_file.get("base_url")
        self.client_id = client_id or os.environ.get("LOOKER_CLIENT_ID") or config_from_file.get("client_id")
        self.client_secret = client_secret or os.environ.get("LOOKER_CLIENT_SECRET") or config_from_file.get("client_secret")
        
        # Handle port conversion to int
        port_from_env = os.environ.get("LOOKER_PORT")
        if port_from_env:
            try:
                port_from_env = int(port_from_env)
            except ValueError:
                logger.warning(f"Invalid port in environment variable: {port_from_env}")
                port_from_env = None
                
        port_from_file = config_from_file.get("port")
        self.port = port or port_from_env or port_from_file
        
        self.api_version = api_version or os.environ.get("LOOKER_API_VERSION") or config_from_file.get("api_version") or "4.0"
        self.project = project or os.environ.get("LOOKER_PROJECT") or config_from_file.get("project")
        self.branch = branch or os.environ.get("LOOKER_GIT_BRANCH") or config_from_file.get("branch")
        
        # Handle timeout conversion to int
        timeout_from_env = os.environ.get("LOOKER_TIMEOUT")
        if timeout_from_env:
            try:
                timeout_from_env = int(timeout_from_env)
            except ValueError:
                logger.warning(f"Invalid timeout in environment variable: {timeout_from_env}")
                timeout_from_env = None
                
        timeout_from_file = config_from_file.get("timeout")
        self.timeout = timeout or timeout_from_env or timeout_from_file or 600  # Default 10 minutes
        
        # Other parameters
        self.commit_ref = kwargs.get("commit_ref") or os.environ.get("LOOKER_COMMIT_REF") or config_from_file.get("commit_ref")
        self.remote_reset = kwargs.get("remote_reset") or (os.environ.get("LOOKER_REMOTE_RESET", "").lower() == "true") or config_from_file.get("remote_reset", False)
        self.explores = kwargs.get("explores") or config_from_file.get("explores", [])
        self.folders = kwargs.get("folders") or config_from_file.get("folders", [])
        self.exclude_personal = kwargs.get("exclude_personal") or (os.environ.get("LOOKER_EXCLUDE_PERSONAL", "").lower() == "true") or config_from_file.get("exclude_personal", False)
        self.incremental = kwargs.get("incremental") or (os.environ.get("LOOKER_INCREMENTAL", "").lower() == "true") or config_from_file.get("incremental", False)
        self.target = kwargs.get("target") or os.environ.get("LOOKER_TARGET") or config_from_file.get("target")
        self.concurrency = int(kwargs.get("concurrency") or os.environ.get("LOOKER_CONCURRENCY", "10") or config_from_file.get("concurrency", 10))
        self.severity = kwargs.get("severity") or os.environ.get("LOOKER_SEVERITY") or config_from_file.get("severity", "warning")
        self.fail_fast = kwargs.get("fail_fast") or (os.environ.get("LOOKER_FAIL_FAST", "").lower() == "true") or config_from_file.get("fail_fast", False)
        self.profile = kwargs.get("profile") or (os.environ.get("LOOKER_PROFILE", "").lower() == "true") or config_from_file.get("profile", False)
        self.max_depth = int(kwargs.get("max_depth") or os.environ.get("LOOKER_MAX_DEPTH", "5") or config_from_file.get("max_depth", 5))
        self.runtime_threshold = int(kwargs.get("runtime_threshold") or os.environ.get("LOOKER_RUNTIME_THRESHOLD", "5") or config_from_file.get("runtime_threshold", 5))
        self.pin_imports = kwargs.get("pin_imports") or os.environ.get("LOOKER_PIN_IMPORTS") or config_from_file.get("pin_imports")
        self.ignore_hidden = kwargs.get("ignore_hidden") or (os.environ.get("LOOKER_IGNORE_HIDDEN", "").lower() == "true") or config_from_file.get("ignore_hidden", False)
        self.log_dir = kwargs.get("log_dir") or os.environ.get("LOOKER_LOG_DIR") or config_from_file.get("log_dir", "logs")
        self.chunk_size = int(kwargs.get("chunk_size") or os.environ.get("LOOKER_CHUNK_SIZE", "500") or config_from_file.get("chunk_size", 500))
        
        # Store any additional configuration from file
        self.additional_config = {k: v for k, v in config_from_file.items() if k not in self.__dict__}
        
        # Validate required fields
        self._validate_config()

    def _load_config_file(self, config_file: str) -> Dict[str, Any]:
        """Load configuration from YAML file.

        Args:
            config_file: Path to YAML configuration file

        Returns:
            Dictionary of configuration values
        """
        try:
            with open(config_file, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            logger.warning(f"Failed to load config file {config_file}: {str(e)}")
            return {}

    def _validate_config(self):
        """Validate that required configuration values are present."""
        required_fields = {
            "base_url": "Looker instance URL (--base-url or LOOKER_BASE_URL)",
            "client_id": "Looker API client ID (--client-id or LOOKER_CLIENT_ID)",
            "client_secret": "Looker API client secret (--client-secret or LOOKER_CLIENT_SECRET)",
        }
        
        missing_fields = []
        for field, description in required_fields.items():
            if not getattr(self, field):
                missing_fields.append(f"{field}: {description}")
                
        if missing_fields:
            raise ValueError(
                "Missing required configuration values:\n" + 
                "\n".join(missing_fields)
            )
            
        # Validate mutually exclusive options
        if self.commit_ref and self.remote_reset:
            raise ValueError("Cannot use both --commit-ref and --remote-reset")
            
        # Validate fail_fast and incremental combination for SQL validator
        if self.fail_fast and self.incremental:
            logger.warning("Cannot use --fail-fast with --incremental. --fail-fast will be ignored.")
            self.fail_fast = False

    def as_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary.

        Returns:
            Dictionary of configuration values
        """
        # Get all attributes that don't start with underscore
        return {
            k: v for k, v in self.__dict__.items() 
            if not k.startswith("_") and k != "additional_config"
        }

    def __str__(self) -> str:
        """String representation of configuration.

        Returns:
            String with configuration values
        """
        # Format config as YAML-style string
        config_str = "Configuration:\n"
        for key, value in self.as_dict().items():
            # Mask sensitive values
            if key in ["client_id", "client_secret"]:
                value = f"{str(value)[:4]}..." if value else None
            config_str += f"  {key}: {value}\n"
        return config_str