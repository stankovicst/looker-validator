# looker_validator/connection.py
"""
Manages the connection to the Looker API.
Fixed AttributeError for client_secret and SDK type hint.
"""

import os
import logging
from typing import Optional, Tuple, Any # Import Any for type hint

import looker_sdk
from looker_sdk.sdk.api40 import models as models40
from looker_sdk.error import SDKError # Import specific SDK error

# Import custom exceptions (ensure exceptions_py_fixed_v1 is applied)
from .exceptions import LookerAuthenticationError, LookerConnectionError, LookerApiError, LookerBranchError

logger = logging.getLogger(__name__)

class LookerConnection:
    """Class to handle connection to the Looker API."""

    # Use typing.Any for the SDK type hint to avoid AttributeError at import time.
    # The actual type returned by init40() is noted in the comment.
    sdk: Any # Actual type hint: looker_sdk.sdk.api40.methods.Looker40SDK
    # Add type hints for instance variables
    base_url: Optional[str]
    client_id: Optional[str]
    client_secret: Optional[str] # Store the secret passed during init
    config_file: str
    config_section: Optional[str]
    port: Optional[int]
    api_version: str
    timeout: int
    verify_ssl: bool

    def __init__(
        self,
        base_url: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        config_file: str = 'looker.ini',
        config_section: Optional[str] = None,
        port: Optional[int] = None,
        api_version: str = "4.0",
        timeout: int = 600,
        verify_ssl: bool = True,
    ):
        """Initialize a connection to the Looker API.

        Reads connection details from parameters, environment variables, or a config file.

        Args:
            base_url: URL of the Looker instance.
            client_id: Looker API client ID.
            client_secret: Looker API client secret.
            config_file: Path to the Looker SDK configuration file.
            config_section: Section in the config file to use.
            port: Port for the Looker API.
            api_version: Looker API version.
            timeout: Request timeout in seconds.
            verify_ssl: Whether to verify SSL certificates.
        """
        # Store provided args needed for initialization or reference
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret # *** Store the client secret ***
        self.config_file = config_file
        self.config_section = config_section
        self.port = port
        self.api_version = api_version
        self.timeout = timeout
        self.verify_ssl = verify_ssl

        # Initialize the SDK client
        self.sdk = self._init_sdk()

    def _init_sdk(self) -> Any:
        """Initializes the Looker SDK client using direct environment variables."""
        logger.info(f"Initializing Looker SDK (API {self.api_version})...")
        
        # Set SDK environment variables directly
        if self.base_url:
            os.environ["LOOKERSDK_BASE_URL"] = self.base_url
        if self.client_id:
            os.environ["LOOKERSDK_CLIENT_ID"] = self.client_id
        if self.client_secret:
            os.environ["LOOKERSDK_CLIENT_SECRET"] = self.client_secret
        
        try:
            # Simple initialization just like your working test.py
            sdk_instance = looker_sdk.init40()
            logger.debug(f"SDK initialized with environment variables.")
            return sdk_instance
        except Exception as e:
            error_msg = f"Failed to initialize Looker SDK: {e}"
            logger.error(error_msg, exc_info=True)
            raise LookerAuthenticationError(error_msg, original_exception=e)


    def test_connection(self) -> str:
        """Test the connection to the Looker API."""
        # (Implementation from connection_py_fixed_v2)
        logger.info("Testing Looker API connection...")
        try:
            me_response = self.sdk.me(fields="display_name,email")
            version_info = self.sdk.versions()
            version = getattr(version_info, 'looker_release_version', 'N/A')
            api_version_used = getattr(version_info, 'current_version', {}).get('version', 'N/A')

            success_message = (
                f"Successfully connected to Looker version {version} using API {api_version_used}."
                f"\nAuthenticated as: {me_response.display_name} ({me_response.email})."
                f"\nTimeout: {self.timeout}s."
            )
            logger.info("Connection test successful.")
            logger.info(f"Authenticated as: {me_response.display_name} ({me_response.email})")
            logger.info(f"Looker Version: {version} | API Version: {api_version_used}")
            return success_message

        except SDKError as e:
            error_msg = f"Looker API connection test failed: {e}"
            logger.error(error_msg, exc_info=True)
            raise LookerConnectionError(error_msg, original_exception=e)
        except Exception as e:
            error_msg = f"Unexpected error during Looker API connection test: {e}"
            logger.error(error_msg, exc_info=True)
            raise LookerConnectionError(error_msg, original_exception=e)

    def _set_workspace(self, workspace_id: str):
        """Switches the session to the specified workspace ('dev' or 'production')."""
        # (Implementation from connection_py_fixed_v2)
        if workspace_id not in ("dev", "production"): raise ValueError("Workspace ID must be 'dev' or 'production'")
        try:
            current_session = self.sdk.session()
            current_workspace = getattr(current_session, 'workspace_id', 'unknown')
            if current_workspace != workspace_id:
                 logger.info(f"Switching workspace to '{workspace_id}'...")
                 self.sdk.update_session(models40.WriteApiSession(workspace_id=workspace_id))
                 logger.debug(f"Workspace successfully set to '{workspace_id}'")
            else: logger.debug(f"Already in '{workspace_id}' workspace.")
        except SDKError as e:
            error_msg = f"Failed to switch workspace to '{workspace_id}': {e}"; logger.error(error_msg, exc_info=True)
            raise LookerApiError(error_msg, status_code=e.status if hasattr(e, 'status') else None, original_exception=e)
        except Exception as e:
            error_msg = f"Unexpected error switching workspace to '{workspace_id}': {e}"; logger.error(error_msg, exc_info=True)
            raise LookerApiError(error_msg, original_exception=e)

    def switch_project_branch(self, project: str, branch: Optional[str] = None):
        """Switch to a branch for a Looker project, or to production if branch is None."""
        # (Implementation from connection_py_fixed_v2)
        try:
            if branch:
                logger.info(f"Switching project '{project}' to development branch '{branch}'...")
                self._set_workspace("dev")
                try:
                     git_branch = self.sdk.git_branch(project_id=project, branch_name=branch)
                     logger.debug(f"Branch '{branch}' found for project '{project}'. Readonly: {git_branch.readonly}")
                except SDKError as e:
                     if hasattr(e, 'status') and e.status == 404:
                         error_msg = f"Branch '{branch}' not found for project '{project}'."; logger.error(error_msg)
                         raise LookerBranchError(error_msg, original_exception=e)
                     else:
                         error_msg = f"API error checking branch '{branch}' for project '{project}': {e}"; logger.error(error_msg, exc_info=True)
                         raise LookerApiError(error_msg, status_code=e.status if hasattr(e, 'status') else None, original_exception=e)
                logger.debug(f"Attempting checkout of branch '{branch}'...")
                body = models40.WriteGitBranch(name=branch)
                self.sdk.update_git_branch(project_id=project, body=body)
                logger.info(f"Successfully checked out branch '{branch}' for project '{project}'.")
            else:
                logger.info(f"Switching project '{project}' to production workspace...")
                self._set_workspace("production")
                logger.info(f"Successfully switched project '{project}' to production.")
        except SDKError as e:
            error_msg = f"API error switching project '{project}' to branch '{branch}': {e}"; logger.error(error_msg, exc_info=True)
            exc_class = LookerBranchError if "branch" in str(e).lower() else LookerApiError
            raise exc_class(error_msg, status_code=e.status if hasattr(e, 'status') else None, original_exception=e)
        except Exception as e:
            error_msg = f"Unexpected error switching project '{project}' to branch '{branch}': {e}"; logger.error(error_msg, exc_info=True)
            raise LookerBranchError(error_msg, original_exception=e)

    def checkout_commit(self, project: str, commit_ref: str) -> str:
        """Checkout a specific commit for a Looker project by creating a temporary branch."""
        # (Implementation from connection_py_fixed_v2)
        temp_branch = f"validator_tmp_{commit_ref[:8]}_{os.urandom(3).hex()}"
        logger.info(f"Checking out commit '{commit_ref}' for project '{project}' via temporary branch '{temp_branch}'...")
        try:
            self._set_workspace("dev")
            logger.debug(f"Creating temporary branch '{temp_branch}' from ref '{commit_ref}'...")
            create_body = models40.WriteGitBranch(name=temp_branch, ref=commit_ref)
            self.sdk.create_git_branch(project_id=project, body=create_body)
            logger.debug(f"Temporary branch '{temp_branch}' created.")
            logger.debug(f"Checking out temporary branch '{temp_branch}'...")
            checkout_body = models40.WriteGitBranch(name=temp_branch)
            self.sdk.update_git_branch(project_id=project, body=checkout_body)
            logger.info(f"Successfully checked out commit '{commit_ref}' via temp branch '{temp_branch}'.")
            return temp_branch
        except SDKError as e:
            error_msg = f"API error checking out commit '{commit_ref}' for project '{project}': {e}"; logger.error(error_msg, exc_info=True)
            if "already exists" not in str(e):
                 try: logger.warning(f"Attempting cleanup of failed temporary branch '{temp_branch}'..."); self.cleanup_temp_branch(project, temp_branch, switch_back=False)
                 except Exception as cleanup_e: logger.error(f"Failed to cleanup temporary branch '{temp_branch}' after error: {cleanup_e}")
            raise LookerBranchError(error_msg, status_code=e.status if hasattr(e, 'status') else None, original_exception=e)
        except Exception as e:
            error_msg = f"Unexpected error checking out commit '{commit_ref}' for project '{project}': {e}"; logger.error(error_msg, exc_info=True)
            raise LookerBranchError(error_msg, original_exception=e)

    def reset_to_remote(self, project: str, branch: str):
        """Reset local branch changes to match the remote branch state via Looker API."""
        # (Implementation from connection_py_fixed_v2)
        logger.info(f"Resetting branch '{branch}' to remote state for project '{project}'...")
        try:
            self.switch_project_branch(project, branch)
            logger.debug(f"Executing reset_git_branch for project '{project}'...")
            self.sdk.reset_git_branch(project_id=project)
            logger.info(f"Successfully reset branch '{branch}' to remote state.")
        except SDKError as e:
            error_msg = f"API error resetting branch '{branch}' to remote for project '{project}': {e}"; logger.error(error_msg, exc_info=True)
            raise LookerBranchError(error_msg, status_code=e.status if hasattr(e, 'status') else None, original_exception=e)
        except Exception as e:
            error_msg = f"Unexpected error resetting branch '{branch}' to remote for project '{project}': {e}"; logger.error(error_msg, exc_info=True)
            if isinstance(e, LookerBranchError) or isinstance(e, LookerApiError): raise e
            else: raise LookerBranchError(error_msg, original_exception=e)

    def cleanup_temp_branch(self, project: str, temp_branch: str, switch_back: bool = True):
        """Clean up (delete) a temporary branch created via Looker API."""
        # (Implementation from connection_py_fixed_v2)
        logger.info(f"Cleaning up temporary branch '{temp_branch}' for project '{project}'...")
        try:
            self._set_workspace("dev")
            default_branch = "main"
            try:
                branches = self.sdk.all_git_branches(project_id=project);
                if any(b.name == "master" for b in branches): default_branch = "master"
                logger.debug(f"Found default branch '{default_branch}' for project '{project}'.")
                checkout_body = models40.WriteGitBranch(name=default_branch)
                self.sdk.update_git_branch(project_id=project, body=checkout_body)
                logger.debug(f"Switched to branch '{default_branch}' before deleting temp branch.")
            except SDKError as e: logger.warning(f"Could not switch to default branch before deleting '{temp_branch}': {e}. Proceeding with delete attempt.")
            logger.debug(f"Attempting to delete temporary branch '{temp_branch}'...")
            try: self.sdk.delete_git_branch(project_id=project, branch_name=temp_branch); logger.info(f"Successfully deleted temporary branch '{temp_branch}'.")
            except SDKError as branch_e: logger.warning(f"Could not delete temporary branch '{temp_branch}': {branch_e}", exc_info=True)
            if switch_back: self._set_workspace("production"); logger.debug("Switched workspace back to production after cleanup.")
        except SDKError as e:
            error_msg = f"API error during cleanup of temporary branch '{temp_branch}': {e}"; logger.error(error_msg, exc_info=True)
            raise LookerApiError(error_msg, status_code=e.status if hasattr(e, 'status') else None, original_exception=e)
        except Exception as e:
            error_msg = f"Unexpected error during cleanup of temporary branch '{temp_branch}': {e}"; logger.error(error_msg, exc_info=True)

