# FILE: looker_validator/validators/base.py
"""
Base validator class that all validator implementations extend.
Refactored to delegate Git state management to the BranchManager via context management.
Fixed ImportError for LookerValidationError.
"""

import logging
import os
import time
import json
import hashlib
# import gc # Removed gc
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple, Set

from looker_sdk.error import SDKError

# Import connection and central exceptions
from ..connection import LookerConnection
# Corrected imports: Removed non-existent LookerValidationError
from ..exceptions import (
    ValidatorError, # Use the base error defined centrally
    LookerApiError,
    LookerBranchError,
    ConfigError
    # Specific validation errors (SQLValidationError etc.) are typically raised/handled
    # in subclasses or caught as ValidatorError if needed here.
)
# Import the updated BranchManager
from ..branch_manager import BranchManager
# Import printer for status updates (optional)
from ..printer import print_info, print_debug, print_warning, print_fail

logger = logging.getLogger(__name__)


class BaseValidator(ABC):
    """
    Abstract base class for all Looker validators.

    Provides common functionality like connection handling, explore filtering,
    caching, and timing. Delegates Git state management to BranchManager.

    Subclasses must implement the `_execute_validation` method.
    """

    def __init__(self, connection: LookerConnection, project: str, **kwargs):
        """Initialize the validator.

        Args:
            connection: LookerConnection instance providing SDK access.
            project: Name of the primary Looker project being validated.
            **kwargs: Additional validator options passed from CLI or config. Expected keys:
                explores (List[str]): List of explore selectors.
                log_dir (str): Directory for logs and artifacts.
                branch (Optional[str]): Git branch name.
                commit_ref (Optional[str]): Specific Git commit SHA.
                remote_reset (bool): Whether to reset branch to remote.
                pin_imports (Optional[str]): Comma-separated string "project:ref,...".
                use_personal_branch (bool): Use personal dev branch.
        """
        # FIX: Remove project from kwargs to prevent duplicate parameter error
        if 'project' in kwargs:
            del kwargs['project']
            logger.debug("Removed duplicate 'project' parameter from kwargs")
            
        if not project:
            raise ConfigError("Project name cannot be empty for validator.")

        self.connection = connection
        self.sdk = connection.sdk # Convenience accessor
        self.project = project
        self.api_version = connection.api_version

        # Configuration options stored directly by BaseValidator or needed for setup
        self.explore_selectors: List[str] = kwargs.get("explores", [])
        self.log_dir: str = kwargs.get("log_dir", "logs")

        # Store all raw kwargs for passing relevant ones to BranchManager
        self._raw_options = kwargs

        # Internal state
        self.cache_dir: Optional[str] = None # Initialized when needed

        # Ensure log directory exists (cache dir created on demand)
        try:
            os.makedirs(self.log_dir, exist_ok=True)
        except OSError as e:
            logger.error(f"Failed to create log directory '{self.log_dir}': {e}", exc_info=True)
            raise ValidatorError(f"Failed to create log directory '{self.log_dir}': {e}") from e


    @abstractmethod
    def _execute_validation(self) -> List[Dict[str, Any]]:
        """
        Core validation logic implemented by subclasses.

        This method is executed *after* the correct Looker Git state (branch, workspace,
        pinned imports) has been set up by the BranchManager context manager
        within the main `validate` method.

        Returns:
            A list of dictionaries, where each dictionary represents a distinct error found.
            An empty list indicates successful validation.
        """
        pass # Subclasses must implement this


    def validate(self) -> List[Dict[str, Any]]:
        """
        Runs the full validation process for this validator.

        Handles setting up the correct Looker Git state using BranchManager,
        executes the specific validation logic via _execute_validation(),
        and ensures state cleanup via the BranchManager's context.

        Returns:
            A list of structured error dictionaries found during validation.
            Returns a list containing a single error dictionary if setup fails.
        """
        start_time = time.time()
        validator_name = self.__class__.__name__
        print_info(f"Starting {validator_name} for project '{self.project}'...")
        logger.info(f"Starting {validator_name} for project '{self.project}'...")

        all_errors: List[Dict[str, Any]] = []

        # Extract options relevant to BranchManager from stored kwargs
        branch = self._raw_options.get("branch")
        commit_ref = self._raw_options.get("commit_ref")
        remote_reset = self._raw_options.get("remote_reset", False)
        pin_imports_str = self._raw_options.get("pin_imports")
        use_personal_branch = self._raw_options.get("use_personal_branch", False)

        try:
            # Parse pinned imports string into dict format expected by BranchManager
            parsed_pin_imports = self._parse_pin_imports(pin_imports_str) # Can raise ConfigError

            # Use BranchManager as a context manager to handle setup and cleanup
            logger.debug("Instantiating BranchManager...")
            branch_manager = BranchManager(
                sdk=self.sdk,
                project=self.project,
                branch=branch,
                commit_ref=commit_ref,
                remote_reset=remote_reset,
                pin_imports=parsed_pin_imports,
                use_personal_branch=use_personal_branch
            )
            logger.debug("Entering BranchManager context...")
            with branch_manager:
                # BranchManager.__enter__ handles setup (branch/commit/pins)
                logger.info(f"Git state ready. Executing core validation logic for {validator_name}...")
                all_errors = self._execute_validation() # Call the subclass's implementation
                logger.info(f"Core validation logic finished for {validator_name}.")
            # BranchManager.__exit__ handles cleanup automatically here

            self.log_timing(f"{validator_name}", start_time)
            return all_errors

        except (LookerBranchError, LookerApiError, ValidatorError, ConfigError) as e:
             # Catch errors specifically related to setup (parsing pins, BranchManager) or core validation logic
             logger.error(f"{validator_name} failed: {e}", exc_info=True)
             print_fail(f"{validator_name} failed: {e}")
             # Return a structured error representing the failure of the whole validator run
             return [{
                 "validator": validator_name,
                 "type": e.__class__.__name__, # Type is the exception class name
                 "severity": "error",
                 "message": f"Validator run failed during setup or execution: {e}",
             }]
        except Exception as e:
             # Catch any other unexpected errors
             logger.error(f"Unexpected error during {validator_name}: {e}", exc_info=True)
             print_fail(f"Unexpected error during {validator_name}: {e}")
             return [{
                 "validator": validator_name,
                 "type": "Unexpected Internal Error",
                 "severity": "error",
                 "message": f"An unexpected error occurred: {e}",
             }]


    # --- Methods removed from BaseValidator (now handled by BranchManager) ---
    # - setup_branch()
    # - cleanup()
    # - pin_imported_projects()

    # --- Helper methods retained or modified ---

    def _parse_pin_imports(self, pin_imports_str: Optional[str]) -> Dict[str, str]:
        """Parses the pin_imports string (e.g., "proj1:ref1,proj2:ref2") into a dictionary."""
        # (Implementation retained from base_py_updated_v2)
        parsed: Dict[str, str] = {}
        if not pin_imports_str:
            return parsed
        logger.debug(f"Parsing pinned imports string: '{pin_imports_str}'")
        try:
            pin_specs = pin_imports_str.split(",")
            for pin_spec in pin_specs:
                pin_spec = pin_spec.strip();
                if not pin_spec: continue
                if ":" not in pin_spec: raise ConfigError(f"Invalid pin format (missing ':'): '{pin_spec}'")
                project_name, ref = pin_spec.split(":", 1)
                project_name = project_name.strip(); ref = ref.strip()
                if not project_name or not ref: raise ConfigError(f"Invalid pin format (empty project or ref): '{pin_spec}'")
                parsed[project_name] = ref
            logger.debug(f"Parsed import pins: {parsed}")
            return parsed
        except ConfigError: raise
        except Exception as e:
            logger.error(f"Failed to parse pin_imports string '{pin_imports_str}': {e}", exc_info=True)
            raise ConfigError(f"Invalid format for pin_imports string: '{pin_imports_str}'. Use 'proj1:ref1,proj2:ref2'.") from e

    def _get_all_explores(self) -> List[Dict[str, str]]:
        """Gets all non-hidden explores for the validator's project."""
        # (Implementation retained from base_py_updated_v2)
        logger.info(f"Fetching all non-hidden explores for project '{self.project}'...")
        start_time = time.time()
        explores: List[Dict[str, str]] = []
        try:
            all_models = self.sdk.all_lookml_models(fields="name,project_name,has_content,explores(name,hidden)")
            project_models = [m for m in all_models if m.project_name == self.project and m.name and m.explores]
            logger.debug(f"Found {len(project_models)} models in project '{self.project}' with explores.")
            for model in project_models:
                model_name = str(model.name)
                if model.explores:
                    for explore in model.explores:
                        if explore.name and not getattr(explore,'hidden', False):
                             explores.append({"model": model_name, "name": str(explore.name)})
            logger.info(f"Found {len(explores)} non-hidden explores in project '{self.project}'.")
            self.log_timing("Fetching explores", start_time)
            return explores
        except SDKError as e:
            error_msg = f"API error fetching explores for project '{self.project}': {e}"
            logger.error(error_msg, exc_info=True)
            raise LookerApiError(error_msg, status_code=e.status if hasattr(e, 'status') else None, original_exception=e)
        except Exception as e:
            error_msg = f"Unexpected error fetching explores for project '{self.project}': {e}"
            logger.error(error_msg, exc_info=True)
            raise ValidatorError(error_msg) from e

    def _filter_explores(self, explores: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Filter explores based on include/exclude selectors."""
        # (Implementation retained from base_py_updated_v2)
        if not self.explore_selectors:
            logger.debug("No explore selectors provided, using all found explores.")
            return explores
        start_time = time.time()
        includes, excludes = self.resolve_explores()
        logger.debug(f"Filtering explores. Include patterns: {includes}, Exclude patterns: {excludes}")
        filtered_explores = [ex for ex in explores if self.matches_selector(ex["model"], ex["name"], includes, excludes)]
        count_before = len(explores); count_after = len(filtered_explores)
        logger.info(f"Filtered explores: {count_after} selected out of {count_before}.")
        self.log_timing("Filtering explores", start_time)
        return filtered_explores

    def resolve_explores(self) -> Tuple[List[str], List[str]]:
        """Resolve explore selectors into include/exclude lists."""
        # (Implementation retained from base_py_updated_v2)
        includes: List[str] = []; excludes: List[str] = []
        for selector in self.explore_selectors:
            selector = selector.strip();
            if not selector: continue
            if selector.startswith("-"): excludes.append(selector[1:])
            else: includes.append(selector)
        return includes, excludes

    def matches_selector(self, model_name: str, explore_name: str, includes: List[str], excludes: List[str]) -> bool:
        """Check if a model/explore matches the include/exclude selectors."""
        # (Implementation retained from base_py_updated_v2)
        full_name = f"{model_name}/{explore_name}"
        for exclude in excludes:
            if exclude == f"{model_name}/*" or exclude == "*/*" or exclude == "*": return False
            elif exclude == f"*/{explore_name}": return False
            elif exclude == full_name: return False
        if not includes: return True
        for include in includes:
            if include == f"{model_name}/*" or include == "*/*" or include == "*": return True
            elif include == f"*/{explore_name}": return True
            elif include == full_name: return True
        return False

    def log_timing(self, name: str, start_time: float):
        """Log the execution time of an operation."""
        # (Implementation retained from base_py_updated_v2)
        elapsed = time.time() - start_time
        logger.info(f"Completed '{name}' in {elapsed:.2f} seconds.")

    # === Cache system methods (Retained from base_py_updated_v2) ===
    def _setup_cache_dir(self):
        if self.cache_dir is None:
            self.cache_dir = os.path.join(self.log_dir, "cache")
            try: os.makedirs(self.cache_dir, exist_ok=True); logger.debug(f"Cache directory ensured at: {self.cache_dir}")
            except OSError as e: logger.warning(f"Failed to create cache directory '{self.cache_dir}': {e}. Caching will be disabled.", exc_info=True); self.cache_dir = None

    def _generate_cache_key(self, *args: str) -> str:
        key_string = f"{self.project}";
        for arg in args: key_string += f"_{arg}"
        return hashlib.md5(key_string.encode('utf-8')).hexdigest()

    def _save_validation_cache(self, cache_key: str, result: Any):
        self._setup_cache_dir();
        if not self.cache_dir: return
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")
        try:
            with open(cache_file, "w", encoding='utf-8') as f: json.dump(result, f, indent=2)
            logger.debug(f"Saved cache for key '{cache_key}' to {cache_file}")
        except (IOError, TypeError, json.JSONDecodeError) as e: logger.warning(f"Failed to save cache file '{cache_file}': {e}", exc_info=True)

    def _check_validation_cache(self, cache_key: str) -> Optional[Any]:
        self._setup_cache_dir();
        if not self.cache_dir: return None
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding='utf-8') as f: cache_data = json.load(f)
                logger.debug(f"Using cached results from key '{cache_key}' ({cache_file})")
                return cache_data
            except (IOError, json.JSONDecodeError) as e: logger.warning(f"Failed to read/decode cache file '{cache_file}': {e}. Ignoring cache.", exc_info=True); return None
        else: logger.debug(f"No cache found for key '{cache_key}'"); return None

    def _clear_cache(self, cache_key: Optional[str] = None):
        self._setup_cache_dir();
        if not self.cache_dir: return
        if cache_key is None:
            cleared_count = 0
            try:
                for item in os.listdir(self.cache_dir):
                    if item.endswith(".json"):
                        file_path = os.path.join(self.cache_dir, item);
                        try: os.remove(file_path); cleared_count += 1
                        except OSError as e: logger.warning(f"Failed to remove cache file '{file_path}': {e}")
                logger.info(f"Cleared {cleared_count} cache files from '{self.cache_dir}'.")
            except OSError as e: logger.error(f"Error listing cache directory '{self.cache_dir}': {e}")
        else:
            cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")
            if os.path.exists(cache_file):
                try: os.remove(cache_file); logger.info(f"Cleared cache for key '{cache_key}'.")
                except OSError as e: logger.warning(f"Failed to remove cache file '{cache_file}': {e}")
            else: logger.debug(f"No cache file found to clear for key '{cache_key}'.")

