"""
Manages the connection to the Looker API.
"""

import os
import logging
from typing import Optional, Tuple

import looker_sdk
from looker_sdk.sdk.api40 import models as models40

logger = logging.getLogger(__name__)

class LookerConnection:
    """Class to handle connection to the Looker API."""

    def __init__(
        self,
        base_url: str,
        client_id: str,
        client_secret: str,
        port: Optional[int] = None,
        api_version: str = "4.0",  # Force API 4.0 since that's what's available
        timeout: int = 600,  # Increased timeout to 10 minutes
    ):
        """Initialize a connection to the Looker API.

        Args:
            base_url: URL of the Looker instance (e.g., https://company.looker.com)
            client_id: Looker API client ID
            client_secret: Looker API client secret
            port: Port for the Looker API (defaults to 19999 or 443 for cloud instances)
            api_version: Looker API version (defaults to 4.0)
            timeout: Request timeout in seconds (defaults to 600 seconds / 10 minutes)
        """
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.api_version = "4.0"  # Force to 4.0 since 3.1 isn't available
        self.timeout = timeout

        # Determine the port based on URL or parameter
        if port:
            self.port = port
        elif "cloud.looker.com" in base_url:
            self.port = 443  # GCP hosted instances use standard HTTPS port
        else:
            self.port = 19999  # Default API port for legacy instances
        
        # Initialize the SDK client
        self.sdk = self._init_sdk()

    def _init_sdk(self):
        """Initialize the Looker SDK client with extended timeout."""
        # Set environment variables for SDK initialization
        os.environ["LOOKERSDK_BASE_URL"] = self.base_url
        os.environ["LOOKERSDK_API_VERSION"] = self.api_version
        os.environ["LOOKERSDK_CLIENT_ID"] = self.client_id
        os.environ["LOOKERSDK_CLIENT_SECRET"] = self.client_secret
        if self.port:
            os.environ["LOOKERSDK_API_PORT"] = str(self.port)
    
        # Set timeout for all SDK requests
        os.environ["LOOKERSDK_TIMEOUT"] = str(self.timeout)
    
        try:
            # Use init40() for initialization - simplest approach
            sdk = looker_sdk.init40()
            logger.debug(f"SDK initialized with API version {self.api_version}, timeout {self.timeout}s")
            return sdk
            
        except Exception as e:
            logger.error(f"Failed to initialize Looker SDK: {str(e)}")
            raise ConnectionError(f"Failed to connect to Looker API: {str(e)}")

    def test_connection(self) -> Tuple[bool, str]:
        """Test the connection to the Looker API.

        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            # Get Looker version
            me_response = self.sdk.me()
            version_info = self.sdk.versions()
            version = version_info.looker_release_version
            
            success_message = (
                f"Connected to Looker version {version} using Looker API {self.api_version}"
                f"\nAuthenticated as: {me_response.display_name} ({me_response.email})"
                f"\nTimeout set to: {self.timeout} seconds"
            )
            logger.info(success_message)
            return True, success_message
        except Exception as e:
            error_message = f"Failed to connect to Looker API: {str(e)}"
            logger.error(error_message)
            return False, error_message

    def switch_project_branch(self, project: str, branch: Optional[str] = None):
        """Switch to a branch for a Looker project.

        Args:
            project: Name of the Looker project
            branch: Name of the Git branch (None for production)
        """
        try:
            if branch:
                logger.info(f"Switching project {project} to branch {branch}")
                # Check if the branch exists
                branch_list = self.sdk.all_git_branches(project)
                branch_exists = any(b.name == branch for b in branch_list)
            
                if not branch_exists:
                    logger.error(f"Branch {branch} does not exist for project {project}")
                    raise ValueError(f"Branch {branch} does not exist for project {project}")
            
                # Set development mode and checkout the branch
                self.sdk.update_session(models40.WriteApiSession(workspace_id="dev"))
            
                # Create a proper body for the API call
                body = models40.WriteGitBranch(name=branch)
                self.sdk.update_git_branch(project, branch, body=body)
            else:
                logger.info(f"Using production branch for project {project}")
                self.sdk.update_session(models40.WriteApiSession(workspace_id="production"))
            
            return True
        except Exception as e:
            logger.error(f"Failed to switch to branch {branch}: {str(e)}")
            raise e

    def checkout_commit(self, project: str, commit_ref: str):
        """Checkout a specific commit for a Looker project.

        Args:
            project: Name of the Looker project
            commit_ref: Git commit reference/SHA
        """
        try:
            logger.info(f"Checking out commit {commit_ref} for project {project}")
            # Create temporary branch from commit
            temp_branch = f"validator-temp-{commit_ref[:8]}"
            
            # Set development mode
            self.sdk.update_session(models40.WriteApiSession(workspace_id="dev"))
            
            # Create temp branch from commit
            self.sdk.create_git_branch(
                project_id=project,
                body=models40.WriteGitBranch(name=temp_branch, ref=commit_ref)
            )
            
            # Checkout the temp branch - Fixed: Added body parameter
            body = models40.WriteGitBranch(name=temp_branch)
            self.sdk.update_git_branch(project, temp_branch, body=body)
            
            return temp_branch
        except Exception as e:
            logger.error(f"Failed to checkout commit {commit_ref}: {str(e)}")
            raise e

    def reset_to_remote(self, project: str, branch: str):
        """Reset local branch to match remote branch.

        Args:
            project: Name of the Looker project
            branch: Name of the Git branch
        """
        try:
            logger.info(f"Resetting branch {branch} to remote for project {project}")
            # First set development mode and checkout the branch - Fixed: Added body parameter
            self.sdk.update_session(models40.WriteApiSession(workspace_id="dev"))
            body = models40.WriteGitBranch(name=branch)
            self.sdk.update_git_branch(project, branch, body=body)
            
            # Reset to remote
            self.sdk.reset_git_branch(project)
            return True
        except Exception as e:
            logger.error(f"Failed to reset branch {branch} to remote: {str(e)}")
            raise e

    def cleanup_temp_branch(self, project: str, temp_branch: str):
        """Clean up temporary branch after validation.

        Args:
            project: Name of the Looker project
            temp_branch: Name of the temporary Git branch
        """
        try:
            logger.info(f"Cleaning up temporary branch {temp_branch} for project {project}")
            # Switch back to production
            self.sdk.update_session(models40.WriteApiSession(workspace_id="production"))
            
            # Delete the temporary branch
            try:
                self.sdk.delete_git_branch(project, temp_branch)
            except Exception as branch_e:
                logger.warning(f"Could not delete temporary branch {temp_branch}: {str(branch_e)}")
            
            return True
        except Exception as e:
            logger.error(f"Failed to clean up temporary branch: {str(e)}")
            # Don't raise since this is a cleanup operation
            return False