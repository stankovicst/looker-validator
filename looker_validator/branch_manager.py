"""
Branch manager for handling Looker Git operations.
"""

import logging
import time
import hashlib
from typing import Dict, List, Optional, Set

import looker_sdk
from looker_sdk.sdk.api40 import models as models40

from looker_validator.exceptions import LookerApiError, ValidatorError, BranchError

logger = logging.getLogger(__name__)


class BranchManager:
    """Manager for Git branches in Looker projects.
    
    Handles branch checkout, creation, and cleanup operations.
    """
    
    def __init__(
        self,
        client,
        project: str,
        remote_reset: bool = False,
        pin_imports: Optional[Dict[str, str]] = None,
        use_personal_branch: bool = False
    ):
        """Initialize the branch manager.
        
        Args:
            client: LookerConnection instance
            project: Looker project name
            remote_reset: Reset branch to remote state
            pin_imports: Dictionary of project:ref pairs for imported projects
            use_personal_branch: Use personal branch instead of temp branch
        """
        self.client = client
        self.sdk = client.sdk
        self.project = project
        self.remote_reset = remote_reset
        self.pin_imports = pin_imports or {}
        self.use_personal_branch = use_personal_branch
        
        # State tracking
        self.initial_workspace = None
        self.initial_branch = None
        self.current_branch = None
        self.is_temp_branch = False
        self.personal_branch = None
        self.import_managers = []
        self.processed_imports = set()
    
    def __enter__(self):
        """Set up Git branch state."""
        logger.debug(f"Setting up branch manager for {self.project}")
        
        # Store initial state
        self._store_initial_state()
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up Git branch state."""
        logger.debug(f"Cleaning up branch manager for {self.project}")
        
        # Clean up import projects first
        for manager in self.import_managers:
            manager.__exit__(exc_type, exc_val, exc_tb)
        
        # Delete temp branch if created
        if self.is_temp_branch and self.current_branch:
            self._cleanup_temp_branch()
        
        # Restore initial state
        self._restore_initial_state()
    
    def _store_initial_state(self):
        """Store the initial workspace and branch."""
        self.initial_workspace = self._get_current_workspace()
        self.initial_branch = self._get_current_branch()
        
        logger.debug(f"Initial state: workspace={self.initial_workspace}, branch={self.initial_branch}")
    
    def _restore_initial_state(self):
        """Restore to the initial workspace and branch."""
        if self.initial_workspace == "production":
            self._set_workspace("production")
        else:
            self._set_workspace("dev")
            if self.initial_branch:
                self.sdk.update_git_branch(self.project, self.initial_branch)
    
    def _get_current_workspace(self) -> str:
        """Get the current workspace."""
        try:
            session = self.sdk.session()
            return session.workspace_id
        except Exception as e:
            logger.error(f"Failed to get current workspace: {str(e)}")
            return "production"  # Default to production
    
    def _get_current_branch(self) -> Optional[str]:
        """Get the current branch name."""
        try:
            if self._get_current_workspace() == "production":
                return None
                
            branch = self.sdk.git_branch(self.project)
            return branch.name
        except Exception as e:
            logger.error(f"Failed to get current branch: {str(e)}")
            return None
    
    def _set_workspace(self, workspace: str):
        """Set the workspace to dev or production."""
        if workspace not in ("dev", "production"):
            raise ValueError("Workspace must be 'dev' or 'production'")
            
        try:
            current = self._get_current_workspace()
            if current != workspace:
                logger.debug(f"Switching workspace from '{current}' to '{workspace}'")
                self.sdk.update_session(models40.WriteApiSession(workspace_id=workspace))
        except Exception as e:
            raise ValidatorError(f"Failed to set workspace to {workspace}: {str(e)}")
    
    def _get_project_imports(self) -> List[str]:
        """Get list of imported projects."""
        try:
            manifest = self.sdk.manifest(self.project)
            return [p.name for p in manifest.imports if not p.is_remote]
        except Exception:
            return []  # No imports or error getting manifest
    
    def _create_temp_branch(self, ref: str) -> str:
        """Create a temporary branch based on the given ref."""
        # Generate a unique branch name
        branch_name = f"tmp_validator_{self._generate_hash()}"
        
        logger.debug(f"Creating temporary branch '{branch_name}' from ref '{ref}'")
        
        # Create the branch
        self._set_workspace("dev")
        self.sdk.create_git_branch(
            project_id=self.project,
            body=models40.WriteGitBranch(name=branch_name, ref=ref)
        )
        
        # Checkout the branch
        self.sdk.update_git_branch(self.project, branch_name)
        
        self.current_branch = branch_name
        self.is_temp_branch = True
        
        return branch_name
    
    def _get_personal_branch(self) -> str:
        """Get the user's personal branch."""
        branches = self.sdk.all_git_branches(self.project)
        
        for branch in branches:
            if branch.personal and not branch.readonly:
                logger.debug(f"Found personal branch: {branch.name}")
                return branch.name
                
        raise ValidatorError("Could not find personal branch for the current user")
    
    def _checkout_personal_branch(self, ref: str) -> str:
        """Checkout personal branch and reset to the given ref."""
        self._set_workspace("dev")
        
        if not self.personal_branch:
            self.personal_branch = self._get_personal_branch()
            
        # Checkout the branch
        self.sdk.update_git_branch(self.project, self.personal_branch)
        
        # Reset to remote
        self.sdk.reset_git_branch(self.project)
        
        # Hard reset to ref
        self.sdk.update_git_branch(
            self.project,
            self.personal_branch,
            body=models40.WriteGitBranch(name=self.personal_branch, ref=ref)
        )
        
        self.current_branch = self.personal_branch
        
        return self.personal_branch
    
    def _cleanup_temp_branch(self):
        """Clean up the temporary branch."""
        if not self.is_temp_branch or not self.current_branch:
            return
            
        try:
            logger.debug(f"Cleaning up temporary branch '{self.current_branch}'")
            
            # We need to be in dev mode to delete branches
            self._set_workspace("dev")
            
            # Switch to the initial branch before deleting
            if self.initial_branch:
                self.sdk.update_git_branch(self.project, self.initial_branch)
                
            # Delete the temporary branch
            self.sdk.delete_git_branch(self.project, self.current_branch)
            
            self.current_branch = self.initial_branch
            self.is_temp_branch = False
        except Exception as e:
            logger.warning(f"Failed to clean up temporary branch: {str(e)}")
    
    def _generate_hash(self) -> str:
        """Generate a short hash for temporary branch names."""
        hash_obj = hashlib.sha1()
        hash_obj.update(str(time.time()).encode('utf-8'))
        return hash_obj.hexdigest()[:8]
    
    def setup_branch(self, branch: Optional[str] = None, commit_ref: Optional[str] = None):
        """Set up branch for validation.
        
        Args:
            branch: Git branch name (None for production)
            commit_ref: Git commit reference
        """
        logger.debug(f"Setting up branch for validation: branch={branch}, commit_ref={commit_ref}")
        
        # Handle imported projects first
        self._setup_imported_projects()
        
        # Branch was specified, use dev mode and checkout the branch
        if branch:
            self._set_workspace("dev")
            
            if self.use_personal_branch:
                # Use personal branch, reset to remote branch
                branch_ref = f"origin/{branch}"
                self._checkout_personal_branch(branch_ref)
            else:
                # Regular checkout
                self.sdk.update_git_branch(self.project, branch)
                self.current_branch = branch
                
                # Reset to remote if requested
                if self.remote_reset:
                    logger.debug(f"Resetting branch '{branch}' to remote")
                    self.sdk.reset_git_branch(self.project)
        
        # Commit reference was specified, create temp branch
        elif commit_ref:
            self._create_temp_branch(commit_ref)
        
        # Neither branch nor commit ref was specified, use production
        else:
            self._set_workspace("production")
            self.current_branch = None
    
    def _setup_imported_projects(self):
        """Set up branches for imported projects."""
        # Get all imports
        imports = self._get_project_imports()
        
        if not imports:
            logger.debug(f"Project '{self.project}' has no imported projects")
            return
            
        logger.debug(f"Project '{self.project}' imports: {imports}")
        
        # Process only imports with pins
        for import_project in imports:
            # Skip if we've already processed this import
            if import_project in self.processed_imports:
                continue
                
            # If project is pinned, set up branch manager for it
            if import_project in self.pin_imports:
                ref = self.pin_imports[import_project]
                logger.debug(f"Setting up branch for imported project '{import_project}' @ {ref}")
                
                # Create branch manager for import
                import_manager = BranchManager(
                    self.client,
                    import_project,
                    remote_reset=self.remote_reset,
                    pin_imports=self.pin_imports,
                    use_personal_branch=self.use_personal_branch
                )
                
                # Enter context and set up branch
                import_manager.__enter__()
                import_manager.setup_branch(branch=ref if not ref.startswith('refs/') else None, 
                                            commit_ref=ref if ref.startswith('refs/') else None)
                
                # Add to managed imports
                self.import_managers.append(import_manager)
                
            # Mark as processed to avoid circular imports
            self.processed_imports.add(import_project)