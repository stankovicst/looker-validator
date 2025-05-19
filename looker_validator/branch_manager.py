# looker_validator/branch_manager.py
"""
Manages Looker Git state (branches, workspaces) via the Looker API using context management.
Refactored based on the user's original looker_sdk-based code.
Fixed AttributeError for SDK type hint.
"""

import asyncio
import hashlib
import logging
import time
import hashlib
# Import Any for type hinting the SDK object
from typing import Dict, List, Optional, Set, Tuple, Self, Any

import looker_sdk
from looker_sdk.sdk.api40 import models as models40
from looker_sdk.error import SDKError

# Import custom exceptions and printer utility
from .exceptions import LookerApiError, LookerBranchError, ValidatorError, ConfigError
from .printer import print_info, print_debug, print_warning, print_fail

logger = logging.getLogger(__name__)


class BranchManager:
    """
    Manages Looker project Git state using the Looker API via context management.

    Handles workspace switching, branch checkout/creation/deletion, remote resets,
    personal branch usage, and recursive setup for pinned imported projects.

    Usage:
        try:
            # Pass the initialized SDK object (e.g., from LookerConnection)
            branch_manager = BranchManager(sdk=connection.sdk, project='my_proj', ...)
            with branch_manager:
                # Looker instance is now in the desired Git state
                # Perform validation actions...
        except (LookerBranchError, LookerApiError) as e:
            # Handle setup errors
        # State is automatically restored upon exiting the 'with' block
    """

    def __init__(
        self,
        # Use typing.Any for SDK type hint to avoid import errors
        # Actual expected type is looker_sdk.sdk.api40.methods.Looker40SDK
        sdk: Any,
        project: str,
        branch: Optional[str] = None,
        commit_ref: Optional[str] = None,
        remote_reset: bool = False,
        pin_imports: Optional[Dict[str, str]] = None, # Expect parsed dict
        use_personal_branch: bool = False,
        # Internal args for recursion control
        _processed_imports: Optional[Set[str]] = None,
        _depth: int = 0
    ):
        """Initialize the branch manager for a specific project and target state.

        Args:
            sdk: Initialized Looker SDK instance (API 4.0).
            project: Looker project name.
            branch: Target Git branch name (None for production). Commit ref takes precedence.
            commit_ref: Target Git commit reference (overrides branch).
            remote_reset: Reset branch to remote state before use (only applies if branch is specified and commit_ref is None).
            pin_imports: Dictionary {import_project_name: ref} for imported projects.
            use_personal_branch: Use personal dev branch instead of temp branch for checkout.
            _processed_imports: Internal set to track processed imports during recursion.
            _depth: Internal recursion depth counter.
        """
        if not project:
             raise ConfigError("Project name cannot be empty for BranchManager.")
        if branch and commit_ref:
             logger.warning(f"Both branch ('{branch}') and commit_ref ('{commit_ref}') specified for project '{project}'. Commit ref will be used.")
             branch = None # Commit ref takes precedence

        self.sdk = sdk # Store the passed SDK instance
        self.project = project
        self.target_branch = branch
        self.target_commit_ref = commit_ref
        self.remote_reset = remote_reset if branch and not commit_ref else False
        self.parsed_pin_imports = pin_imports or {}
        self.use_personal_branch = use_personal_branch

        # Internal state tracking
        self._initial_workspace: Optional[str] = None
        self._initial_branch: Optional[str] = None
        self._current_branch_name: Optional[str] = None
        self._is_temp_branch: bool = False
        self._personal_branch_name: Optional[str] = None
        self._import_managers: List[Self] = []
        self._processed_imports: Set[str] = _processed_imports if _processed_imports is not None else set()
        self._depth = _depth
        self._max_depth = 5

        if self._depth > self._max_depth:
             raise LookerBranchError(f"Maximum import recursion depth ({self._max_depth}) reached for project '{self.project}'. Check for circular dependencies in pinned imports.")


    def __enter__(self) -> Self:
        """Sets up the required Git state upon entering the 'with' block."""
        prefix = "  " * self._depth
        print_debug(f"{prefix}Entering BranchManager context for project '{self.project}'...")
        logger.debug(f"{prefix}Entering BranchManager context for project '{self.project}' (Target: Branch='{self.target_branch}', Commit='{self.target_commit_ref}', RemoteReset='{self.remote_reset}', UsePersonal='{self.use_personal_branch}')")

        if self.project in self._processed_imports:
             logger.warning(f"{prefix}Project '{self.project}' was already processed in this context run. Skipping setup to avoid cycles.")
             return self

        self._store_initial_state() # Raises LookerBranchError on critical failure
        self._setup_target_state() # Raises LookerBranchError/LookerApiError on failure

        self._processed_imports.add(self.project)
        self._setup_pinned_imports() # Logs warnings on failure

        current_workspace_final = self._get_current_workspace()
        current_branch_final = self._get_current_branch_name() if current_workspace_final == "dev" else None
        print_debug(f"{prefix}BranchManager context setup complete for '{self.project}'. Current state: Workspace='{current_workspace_final}', Branch='{current_branch_final}'.")
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cleans up Git state upon exiting the 'with' block."""
        prefix = "  " * self._depth
        print_debug(f"{prefix}Exiting BranchManager context for project '{self.project}'...")
        logger.debug(f"{prefix}Exiting BranchManager context for project '{self.project}'. Exception type: {exc_type}")

        cleanup_successful = True

        # 1. Clean up import projects first (LIFO)
        while self._import_managers:
            manager = self._import_managers.pop()
            try:
                manager.__exit__(exc_type, exc_val, exc_tb)
            except Exception as import_exit_e:
                logger.error(f"{prefix}Error cleaning up imported project '{manager.project}': {import_exit_e}", exc_info=True)
                print_warning(f"{prefix}Error cleaning up imported project '{manager.project}': {import_exit_e}")
                cleanup_successful = False

        # 2. Delete temp branch if created by this instance
        if self._is_temp_branch and self._current_branch_name:
            if not self._cleanup_temp_branch():
                 cleanup_successful = False

        # 3. Restore initial state for the current project
        if not self._restore_initial_state():
             cleanup_successful = False

        self._processed_imports.discard(self.project)

        if cleanup_successful:
             print_debug(f"{prefix}BranchManager context cleanup finished successfully for '{self.project}'.")
        else:
             print_warning(f"{prefix}BranchManager context cleanup for '{self.project}' finished, but some steps failed (check logs).")

        return False # Do not suppress exceptions


    # --- Internal Helper Methods ---
    # (Implementations remain the same as in branch_manager_py_refactored_v1)
    # ... (Methods _store_initial_state to _setup_pinned_imports omitted for brevity,
    #      assuming they are the same as in the previous artifact
    #      branch_manager_py_refactored_v1) ...

    # --- Placeholder for omitted methods ---
    # Make sure to include the full implementations of all methods from
    # branch_manager_py_refactored_v1 here.

    def _store_initial_state(self):
        """Stores the initial workspace and branch name."""
        prefix = "  " * self._depth
        logger.debug(f"{prefix}Storing initial Git state for project '{self.project}'...")
        try:
            self._initial_workspace = self._get_current_workspace()
            self._initial_branch = self._get_current_branch_name() if self._initial_workspace == "dev" else None
            logger.info(f"{prefix}Initial state for '{self.project}': Workspace='{self._initial_workspace}', Branch='{self._initial_branch}'")
        except (LookerApiError, LookerBranchError) as e:
            logger.critical(f"{prefix}CRITICAL: Failed to store initial Git state for project '{self.project}': {e}", exc_info=True)
            raise LookerBranchError(f"Failed to store initial Git state for '{self.project}': {e}", original_exception=e)
        except Exception as e:
            logger.critical(f"{prefix}CRITICAL: Unexpected error storing initial Git state for project '{self.project}': {e}", exc_info=True)
            raise LookerBranchError(f"Unexpected error storing initial Git state for '{self.project}': {e}", original_exception=e)

    def _restore_initial_state(self) -> bool:
        """Restores the workspace and branch to their initial states. Returns True on success."""
        prefix = "  " * self._depth
        logger.debug(f"{prefix}Restoring initial Git state for project '{self.project}' (Target: Workspace='{self._initial_workspace}', Branch='{self._initial_branch}')...")
        restore_needed = False
        try:
            current_workspace = self._get_current_workspace()
            current_branch = self._get_current_branch_name() if current_workspace == "dev" else None

            if current_workspace != self._initial_workspace:
                restore_needed = True
                logger.debug(f"{prefix}Workspace needs restore ({current_workspace} -> {self._initial_workspace})")
            elif current_workspace == "dev" and current_branch != self._initial_branch:
                 restore_needed = True
                 logger.debug(f"{prefix}Branch needs restore ('{current_branch}' -> '{self._initial_branch}')")

            if not restore_needed:
                 logger.debug(f"{prefix}Initial state matches current state. No restore needed.")
                 return True

            print_info(f"{prefix}Restoring initial Git state for project '{self.project}'...")
            if self._initial_workspace:
                self._set_workspace(self._initial_workspace)
            if self._initial_workspace == "dev" and self._initial_branch:
                if self._branch_exists(self._initial_branch):
                    logger.debug(f"{prefix}Checking out initial branch '{self._initial_branch}'...")
                    self._checkout_branch(self._initial_branch, skip_existence_check=True)
                    logger.info(f"{prefix}Successfully restored initial branch '{self._initial_branch}'.")
                else:
                    logger.warning(f"{prefix}Initial branch '{self._initial_branch}' no longer exists. Attempting to checkout default branch.")
                    print_warning(f"{prefix}Initial branch '{self._initial_branch}' no longer exists. Attempting checkout of default branch.")
                    if not self._checkout_default_branch():
                         logger.critical(f"{prefix}CRITICAL: Failed to checkout default branch after initial branch '{self._initial_branch}' was not found.")
                         return False
            logger.info(f"{prefix}Initial Git state restored successfully for project '{self.project}'.")
            return True
        except (LookerApiError, LookerBranchError) as e:
            logger.critical(f"{prefix}CRITICAL: Failed to restore initial Git state for project '{self.project}': {e}", exc_info=True)
            print_fail(f"{prefix}CRITICAL: Failed to restore initial Git state for project '{self.project}': {e}")
            return False
        except Exception as e:
            logger.critical(f"{prefix}CRITICAL: Unexpected error restoring initial Git state for project '{self.project}': {e}", exc_info=True)
            print_fail(f"{prefix}CRITICAL: Unexpected error restoring initial Git state for project '{self.project}': {e}")
            return False

    def _get_current_workspace(self) -> str:
        """Gets the current session's workspace ('dev' or 'production')."""
        try:
            session = self.sdk.session()
            workspace = getattr(session, 'workspace_id', 'unknown')
            if workspace not in ('dev', 'production'):
                 logger.warning(f"Unknown workspace ID reported by session API: '{workspace}'. Defaulting to 'production'.")
                 return 'production'
            return workspace
        except SDKError as e:
            raise LookerApiError(f"API error getting current workspace: {e}", status_code=e.status if hasattr(e, 'status') else None, original_exception=e)
        except Exception as e:
            raise LookerApiError(f"Unexpected error getting current workspace: {e}", original_exception=e)

    def _get_current_branch_name(self) -> Optional[str]:
        """Gets the current checked-out dev branch name, or None if in production."""
        if self._get_current_workspace() == "production":
            return None
        try:
            branch = self.sdk.git_branch(project_id=self.project)
            return getattr(branch, 'name', None)
        except SDKError as e:
            if hasattr(e, 'status') and e.status == 404:
                 logger.warning(f"API reported 404 getting active branch for project '{self.project}' while in dev mode.")
                 return None
            else:
                 raise LookerApiError(f"API error getting active branch for project '{self.project}': {e}", status_code=e.status if hasattr(e, 'status') else None, original_exception=e)
        except Exception as e:
            raise LookerApiError(f"Unexpected error getting active branch for project '{self.project}': {e}", original_exception=e)

    def _set_workspace(self, workspace_id: str):
        """Switches the session to the specified workspace ('dev' or 'production')."""
        prefix = "  " * self._depth
        if workspace_id not in ("dev", "production"): raise ValueError("Workspace ID must be 'dev' or 'production'")
        try:
            current_workspace = self._get_current_workspace()
            if current_workspace != workspace_id:
                 logger.info(f"{prefix}Switching workspace to '{workspace_id}'...")
                 print_info(f"{prefix}Switching workspace to '{workspace_id}'...")
                 self.sdk.update_session(models40.WriteApiSession(workspace_id=workspace_id))
                 logger.debug(f"{prefix}Workspace successfully set to '{workspace_id}'")
            else: logger.debug(f"{prefix}Already in '{workspace_id}' workspace.")
        except SDKError as e:
            error_msg = f"Failed to switch workspace to '{workspace_id}': {e}"; logger.error(error_msg, exc_info=True)
            raise LookerApiError(error_msg, status_code=e.status if hasattr(e, 'status') else None, original_exception=e)
        except Exception as e:
            error_msg = f"Unexpected error switching workspace to '{workspace_id}': {e}"; logger.error(error_msg, exc_info=True)
            raise LookerApiError(error_msg, original_exception=e)

    def _branch_exists(self, branch_name: str) -> bool:
        """Checks if a branch exists for the project via API."""
        prefix = "  " * self._depth
        logger.debug(f"{prefix}Checking existence of branch '{branch_name}'...")
        try:
            # Get all branches and check if our branch is in the list
            all_branches = self.sdk.all_git_branches(project_id=self.project)
            for branch in all_branches:
                if getattr(branch, 'name', '') == branch_name:
                    logger.debug(f"{prefix}Branch '{branch_name}' exists.")
                    return True
            logger.debug(f"{prefix}Branch '{branch_name}' does not exist.")
            return False
        except SDKError as e:
            raise LookerApiError(f"API error checking branches: {e}", 
                status_code=e.status if hasattr(e, 'status') else None, 
                original_exception=e)
        except Exception as e:
            raise LookerApiError(f"Unexpected error checking branches: {e}", 
                original_exception=e)

    def _checkout_branch(self, branch_name: str, skip_existence_check: bool = False):
        """Checks out a specific, existing branch."""
        prefix = "  " * self._depth
        logger.info(f"{prefix}Checking out branch '{branch_name}' for project '{self.project}'...")
        print_info(f"{prefix}Checking out branch '{branch_name}'...")
        try:
            self._set_workspace("dev")
            if not skip_existence_check and not self._branch_exists(branch_name):
                raise LookerBranchError(f"Branch '{branch_name}' not found for project '{self.project}'.")
            
            # Simpler approach - let's try sending just the required data
            body = models40.WriteGitBranch(name=branch_name)
            # Use update_git_branch without branch_name parameter
            self.sdk.update_git_branch(project_id=self.project, body=body)
            
            self._current_branch_name = branch_name
            self._is_temp_branch = False
            logger.info(f"{prefix}Successfully checked out branch '{branch_name}'.")
        except SDKError as e:
            error_msg = f"API error checking out branch '{branch_name}': {e}"
            logger.error(error_msg, exc_info=True)
            raise LookerBranchError(error_msg, status_code=e.status if hasattr(e, 'status') else None, original_exception=e) from e
        except Exception as e:
            error_msg = f"Unexpected error checking out branch '{branch_name}': {e}"
            logger.error(error_msg, exc_info=True)
            raise LookerBranchError(error_msg, original_exception=e) from e

    def _reset_branch_to_remote(self, branch_name: str):
        """Resets the currently checked-out dev branch to its remote state."""
        prefix = "  " * self._depth
        logger.info(f"{prefix}Resetting branch '{branch_name}' to remote state for project '{self.project}'...")
        print_info(f"{prefix}Resetting branch '{branch_name}' to remote...")
        try:
            current_branch = self._get_current_branch_name()
            if self._get_current_workspace() != "dev" or current_branch != branch_name:
                 logger.warning(f"{prefix}Attempting reset, but not currently on branch '{branch_name}' in dev mode. Checking out first.")
                 self._checkout_branch(branch_name)
            self.sdk.reset_git_branch(project_id=self.project)
            logger.info(f"{prefix}Successfully reset branch '{branch_name}' to remote state.")
        except SDKError as e:
            error_msg = f"API error resetting branch '{branch_name}' to remote: {e}"; logger.error(error_msg, exc_info=True)
            raise LookerBranchError(error_msg, status_code=e.status if hasattr(e, 'status') else None, original_exception=e) from e
        except Exception as e:
            error_msg = f"Unexpected error resetting branch '{branch_name}' to remote: {e}"; logger.error(error_msg, exc_info=True)
            raise LookerBranchError(error_msg, original_exception=e) from e

    def _checkout_commit_via_temp_branch(self, commit_ref: str) -> str:
        """Checks out a commit by creating and checking out a temporary branch."""
        prefix = "  " * self._depth
        timestamp_hash = hashlib.sha1(str(time.time()).encode('utf-8')).hexdigest()[:6]
        temp_branch_name = f"tmp_validator_{commit_ref[:7]}_{timestamp_hash}"
        logger.info(f"{prefix}Checking out commit '{commit_ref}' via temporary branch '{temp_branch_name}'...")
        print_info(f"{prefix}Checking out commit '{commit_ref}'...")
        try:
            self._set_workspace("dev")
            logger.debug(f"{prefix}Creating temporary branch '{temp_branch_name}' from ref '{commit_ref}'...")
            create_body = models40.WriteGitBranch(name=temp_branch_name, ref=commit_ref)
            self.sdk.create_git_branch(project_id=self.project, body=create_body)
            logger.debug(f"{prefix}Temporary branch '{temp_branch_name}' created.")
            logger.debug(f"{prefix}Checking out temporary branch '{temp_branch_name}'...")
            checkout_body = models40.WriteGitBranch(name=temp_branch_name)
            self.sdk.update_git_branch(project_id=self.project, body=checkout_body)
            self._current_branch_name = temp_branch_name; self._is_temp_branch = True
            logger.info(f"{prefix}Successfully checked out commit '{commit_ref}' via temp branch '{temp_branch_name}'.")
            return temp_branch_name
        except SDKError as e:
            error_msg = f"API error checking out commit '{commit_ref}': {e}"; logger.error(error_msg, exc_info=True)
            raise LookerBranchError(error_msg, status_code=e.status if hasattr(e, 'status') else None, original_exception=e) from e
        except Exception as e:
            error_msg = f"Unexpected error checking out commit '{commit_ref}': {e}"; logger.error(error_msg, exc_info=True)
            raise LookerBranchError(error_msg, original_exception=e) from e

    def _checkout_personal_branch_and_reset(self, target_ref: str) -> str:
        """Checks out user's personal branch and hard resets it to target_ref."""
        prefix = "  " * self._depth
        logger.info(f"{prefix}Setting up personal dev branch, resetting to ref '{target_ref}'...")
        print_info(f"{prefix}Setting up personal dev branch...")
        try:
            self._set_workspace("dev")
            if not self._personal_branch_name: self._personal_branch_name = self._find_personal_branch_name()
            logger.debug(f"{prefix}Using personal branch: '{self._personal_branch_name}'")
            personal_branch = self._personal_branch_name
            self._checkout_branch(personal_branch)
            logger.debug(f"{prefix}Resetting personal branch '{personal_branch}' to remote...")
            self.sdk.reset_git_branch(project_id=self.project)
            logger.debug(f"{prefix}Hard resetting personal branch '{personal_branch}' to ref '{target_ref}'...")
            reset_body = models40.WriteGitBranch(name=personal_branch, ref=target_ref)
            self.sdk.update_git_branch(project_id=self.project, body=reset_body)
            self._current_branch_name = personal_branch; self._is_temp_branch = False
            logger.info(f"{prefix}Successfully set up personal branch '{personal_branch}' to ref '{target_ref}'.")
            return personal_branch
        except SDKError as e:
            error_msg = f"API error setting up personal branch to ref '{target_ref}': {e}"; logger.error(error_msg, exc_info=True)
            raise LookerBranchError(error_msg, status_code=e.status if hasattr(e, 'status') else None, original_exception=e) from e
        except Exception as e:
            error_msg = f"Unexpected error setting up personal branch to ref '{target_ref}': {e}"; logger.error(error_msg, exc_info=True)
            raise LookerBranchError(error_msg, original_exception=e) from e

    def _find_personal_branch_name(self) -> str:
        """Finds the current user's personal dev branch name."""
        prefix = "  " * self._depth
        logger.debug(f"{prefix}Searching for personal dev branch for project '{self.project}'...")
        try:
            branches = self.sdk.all_git_branches(project_id=self.project)
            for branch in branches:
                if getattr(branch, 'personal', False) and not getattr(branch, 'readonly', True):
                    branch_name = getattr(branch, 'name', None)
                    if branch_name: logger.debug(f"{prefix}Found personal branch: '{branch_name}'"); return branch_name
            raise LookerBranchError(f"No personal, writable dev branch found for the current user in project '{self.project}'.")
        except SDKError as e: raise LookerApiError(f"API error searching for personal branch: {e}", status_code=e.status if hasattr(e, 'status') else None, original_exception=e) from e
        except Exception as e: raise LookerApiError(f"Unexpected error searching for personal branch: {e}", original_exception=e) from e

    def _cleanup_temp_branch(self) -> bool:
        """Deletes the temporary branch created by this instance. Returns True on success."""
        prefix = "  " * self._depth
        if not self._is_temp_branch or not self._current_branch_name: logger.debug(f"{prefix}No temporary branch to clean up for this instance."); return True
        branch_to_delete = self._current_branch_name
        logger.info(f"{prefix}Cleaning up temporary branch '{branch_to_delete}' for project '{self.project}'...")
        print_info(f"{prefix}Cleaning up temporary branch '{branch_to_delete}'...")
        cleanup_success = True
        try:
            self._set_workspace("dev")
            target_branch = self._initial_branch
            if not target_branch or target_branch == branch_to_delete: target_branch = self._get_default_branch_name()
            if target_branch and target_branch != branch_to_delete:
                 logger.debug(f"{prefix}Switching to branch '{target_branch}' before deleting '{branch_to_delete}'...")
                 try: self._checkout_branch(target_branch)
                 except (LookerBranchError, LookerApiError) as e: logger.warning(f"{prefix}Could not switch to branch '{target_branch}' before deleting temp branch: {e}. Proceeding with delete attempt anyway.")
            else: logger.warning(f"{prefix}Could not determine a safe branch to switch to before deleting '{branch_to_delete}'. Attempting delete from current state.")
            logger.debug(f"{prefix}Attempting to delete branch '{branch_to_delete}'...")
            try: self.sdk.delete_git_branch(project_id=self.project, branch_name=branch_to_delete); logger.info(f"{prefix}Successfully deleted temporary branch '{branch_to_delete}'.")
            except SDKError as branch_e: logger.error(f"{prefix}Failed to delete temporary branch '{branch_to_delete}': {branch_e}", exc_info=True); print_warning(f"{prefix}Failed to delete temporary branch '{branch_to_delete}': {branch_e}"); cleanup_success = False
            self._current_branch_name = target_branch; self._is_temp_branch = False
        except SDKError as e: logger.error(f"{prefix}API error during cleanup of temporary branch '{branch_to_delete}': {e}", exc_info=True); print_warning(f"{prefix}API error during cleanup of temporary branch '{branch_to_delete}': {e}"); cleanup_success = False
        except Exception as e: logger.error(f"{prefix}Unexpected error cleaning up temporary branch '{branch_to_delete}': {e}", exc_info=True); print_warning(f"{prefix}Unexpected error cleaning up temporary branch '{branch_to_delete}': {e}"); cleanup_success = False
        return cleanup_success

    def _get_default_branch_name(self) -> Optional[str]:
         """Tries to find the default branch name ('main' or 'master')."""
         prefix = "  " * self._depth; logger.debug(f"{prefix}Trying to find default branch ('main' or 'master')...")
         try:
              branches = self.sdk.all_git_branches(project_id=self.project, fields="name"); branch_names = {b.name for b in branches if b.name}
              if "main" in branch_names: return "main"
              if "master" in branch_names: return "master"
              logger.warning(f"{prefix}Could not find 'main' or 'master' branch in project '{self.project}'."); return None
         except SDKError as e: logger.warning(f"{prefix}API error finding default branch for project '{self.project}': {e}"); return None

    def _setup_target_state(self):
         """Sets up the primary project's Git state based on instance config."""
         prefix = "  " * self._depth; logger.debug(f"{prefix}Setting up target Git state for project '{self.project}'...")
         if self.target_commit_ref: self._checkout_commit_via_temp_branch(self.target_commit_ref)
         elif self.target_branch:
             if self.use_personal_branch: self._checkout_personal_branch_and_reset(f"origin/{self.target_branch}")
             else: self._checkout_branch(self.target_branch);
             if self.remote_reset: self._reset_branch_to_remote(self.target_branch)
         else: logger.info(f"{prefix}Using production workspace for project '{self.project}'."); self._set_workspace("production"); self._current_branch_name = None; self._is_temp_branch = False

    def _setup_pinned_imports(self):
        """Recursively sets up BranchManager contexts for pinned imported projects."""
        prefix = "  " * self._depth
        if not self.parsed_pin_imports: logger.debug(f"{prefix}No pinned imports specified for project '{self.project}'."); return
        logger.debug(f"{prefix}Setting up pinned imported projects for '{self.project}': {self.parsed_pin_imports}")
        for import_project, ref in self.parsed_pin_imports.items():
            if import_project == self.project: logger.warning(f"{prefix}Skipping attempt to pin project '{import_project}' to itself."); continue
            if import_project in self._processed_imports: logger.debug(f"{prefix}Import project '{import_project}' already processed in this run, skipping."); continue
            logger.info(f"{prefix}Setting up pinned import '{import_project}' to ref '{ref}'..."); print_info(f"{prefix}Setting up pinned import '{import_project}' @ '{ref}'...")
            try:
                is_branch = not ref.startswith('refs/') and len(ref) != 40; commit_ref_for_import = ref if not is_branch else None; branch_for_import = ref if is_branch else None
                import_manager = BranchManager(sdk=self.sdk, project=import_project, branch=branch_for_import, commit_ref=commit_ref_for_import, remote_reset=False, pin_imports=self.parsed_pin_imports, use_personal_branch=False, _processed_imports=self._processed_imports, _depth=self._depth + 1)
                import_manager.__enter__(); self._import_managers.append(import_manager)
            except (LookerBranchError, LookerApiError, ValidatorError, ConfigError) as e: logger.warning(f"{prefix}Failed to set up pinned import project '{import_project}' to ref '{ref}': {e}", exc_info=True); print_warning(f"{prefix}Failed to set up pinned import project '{import_project}': {e}")
            except Exception as e: logger.warning(f"{prefix}Unexpected error setting up pinned import project '{import_project}': {e}", exc_info=True); print_warning(f"{prefix}Unexpected error setting up pinned import project '{import_project}': {e}")

