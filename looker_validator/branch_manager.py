"""
Enhanced asynchronous branch manager for handling Looker Git operations.
"""

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from looker_validator.async_client import AsyncLookerClient
from looker_validator.exceptions import BranchError, LookerApiError, LookerValidatorException

logger = logging.getLogger(__name__)


@dataclass
class ProjectState:
    """State of a Looker project's Git branch."""
    project: str
    workspace: str
    branch: str
    commit: str


class AsyncBranchManager:
    """Async manager for Git branches in Looker projects.
    
    Handles branch checkout, creation, deletion, and state management.
    """
    
    def __init__(
        self,
        client: AsyncLookerClient,
        project: str,
        remote_reset: bool = False,
        pin_imports: Optional[Dict[str, str]] = None,
        use_personal_branch: bool = False,
        skip_imports: Optional[List[str]] = None
    ):
        """Initialize the branch manager.
        
        Args:
            client: AsyncLookerClient instance
            project: Looker project name
            remote_reset: Whether to reset to the remote branch state
            pin_imports: Dict of project:ref pairs for imported projects
            use_personal_branch: Whether to use personal branch
            skip_imports: List of projects to skip importing
        """
        self.client = client
        self.project = project
        self.remote_reset = remote_reset
        self.pin_imports = pin_imports or {}
        self.history: List[ProjectState] = []
        
        self.commit: Optional[str] = None
        self.branch: Optional[str] = None
        self.is_temp_branch: bool = False
        self.use_personal_branch: bool = use_personal_branch
        self.personal_branch: Optional[str] = None
        self.import_managers: List[AsyncBranchManager] = []
        self.skip_imports: List[str] = [] if skip_imports is None else skip_imports
        self.processed_imports: Set[str] = set()
    
    async def __aenter__(self) -> "AsyncBranchManager":
        """Set up Git branch state."""
        logger.debug(f"Setting up branch manager for {self.project}")
        
        # Store initial state
        state = await self.get_project_state()
        self.history = [state]
        
        # Set up imported projects if in dev mode
        if state.workspace == "dev" and self.pin_imports:
            await self._setup_imported_projects()
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Clean up Git branch state."""
        logger.debug(f"Cleaning up branch manager for {self.project}")
        
        # Clean up import projects first
        for manager in self.import_managers:
            await manager.__aexit__(exc_type, exc_val, exc_tb)
        
        # Delete temp branch if created
        if self.is_temp_branch and self.branch:
            await self._cleanup_temp_branch()
        
        # Restore initial state
        await self._restore_initial_state()
    
    async def get_project_state(self) -> ProjectState:
        """Get the current project state.
        
        Returns:
            ProjectState with workspace, branch, and commit
        """
        workspace = await self.client.get_workspace()
        branch_info = await self.client.get_active_branch(self.project)
        
        return ProjectState(
            project=self.project,
            workspace=workspace,
            branch=branch_info["name"],
            commit=branch_info["ref"]
        )
    
    async def _restore_initial_state(self) -> None:
        """Restore the initial workspace and branch."""
        if not self.history:
            return
            
        initial_state = self.history[0]
        
        if initial_state.workspace == "production":
            await self.client.update_workspace("production")
        else:
            await self.client.update_workspace("dev")
            if initial_state.branch:
                await self.client.checkout_branch(self.project, initial_state.branch)
    
    async def setup_branch(
        self, 
        branch: Optional[str] = None, 
        commit_ref: Optional[str] = None
    ) -> None:
        """Set up branch for validation.
    
        Args:
            branch: Git branch name (None for production)
            commit_ref: Git commit reference
        """
        logger.debug(f"Setting up branch for validation: branch={branch}, commit_ref={commit_ref}")
    
        self.branch = None
        self.commit = None
    
        # Branch was specified, use dev mode and checkout the branch
        if branch:
            self.branch = branch
            await self.client.update_workspace("dev")
        
            if self.use_personal_branch:
                # Use personal branch, reset to remote branch
                branch_ref = f"origin/{branch}"
                self.branch = await self._checkout_personal_branch(branch_ref)
            else:
                # Regular checkout
                await self.client.checkout_branch(self.project, branch)
            
                # Reset to remote if requested
                if self.remote_reset:
                    logger.debug(f"Resetting branch '{branch}' to remote")
                    await self.client.reset_to_remote(self.project)
                
            # Verify checkout
            await self.verify_branch_checkout()
    
        # Commit reference was specified, create temp branch
        elif commit_ref:
            self.commit = commit_ref
            self.branch = await self._checkout_temp_branch(commit_ref)
    
        # Neither branch nor commit ref was specified, use production
        else:
            await self.client.update_workspace("production")
    
    async def _get_project_imports(self) -> List[str]:
        """Get list of imported projects.
        
        Returns:
            List of project names
        """
        try:
            manifest = await self.client.get_manifest(self.project)
            return [p["name"] for p in manifest.get("imports", []) if not p.get("is_remote", False)]
        except Exception:
            return []  # No imports or error getting manifest
    
    async def _setup_imported_projects(self) -> None:
        """Set up branches for imported projects."""
        # Get all imports
        imports = await self._get_project_imports()
        
        if not imports:
            logger.debug(f"Project '{self.project}' has no imported projects")
            return
            
        logger.debug(f"Project '{self.project}' imports: {imports}")
        
        # Process only imports with pins
        for import_project in imports:
            # Skip if we've already processed this import
            if import_project in self.processed_imports or import_project in self.skip_imports:
                logger.debug(f"Skipping already processed import: {import_project}")
                continue
                
            # If project is pinned, set up branch manager for it
            if import_project in self.pin_imports:
                ref = self.pin_imports[import_project]
                logger.debug(f"Setting up branch for imported project '{import_project}' @ {ref}")
                
                # Create branch manager for import
                import_manager = AsyncBranchManager(
                    self.client,
                    import_project,
                    remote_reset=self.remote_reset,
                    pin_imports=self.pin_imports,
                    use_personal_branch=self.use_personal_branch,
                    skip_imports=self.skip_imports + [self.project]  # Prevent circular imports
                )
                
                # Enter context and set up branch
                await import_manager.__aenter__()
                is_commit = ref and ref.startswith("refs/") or (len(ref) >= 7 and all(c in "0123456789abcdef" for c in ref))
                if is_commit:
                    await import_manager.setup_branch(commit_ref=ref)
                else:
                    await import_manager.setup_branch(branch=ref)
                
                # Add to managed imports
                self.import_managers.append(import_manager)
                
            # Mark as processed to avoid circular imports
            self.processed_imports.add(import_project)
    
    async def _checkout_personal_branch(self, ref: str) -> str:
        """Updates the user's personal branch to the git ref.
        
        Args:
            ref: Git reference to reset to
            
        Returns:
            Branch name
        """
        await self.client.update_workspace("dev")
        if not self.personal_branch:
            self.personal_branch = await self._get_personal_branch()
            
        await self.client.checkout_branch(self.project, self.personal_branch)
        await self.client.reset_to_remote(self.project)
        await self.client.hard_reset_branch(self.project, self.personal_branch, ref)
        
        return self.personal_branch
    
    async def verify_branch_checkout(self) -> bool:
        """Verify that the current branch is what we expect.
    
        Returns:
            True if correct branch is checked out
        """
        if not self.branch:
            return True
        
        try:
            current_branch = await self.client.get_active_branch_name(self.project)
            if current_branch != self.branch:
                logger.warning(f"Branch mismatch: Expected '{self.branch}', found '{current_branch}'")
            
                # Check out the correct branch again
                await self.client.checkout_branch(self.project, self.branch)
            
                # Verify again
                current_branch = await self.client.get_active_branch_name(self.project)
                if current_branch != self.branch:
                    logger.error(f"Failed to checkout branch '{self.branch}', still on '{current_branch}'")
                    return False
            
                logger.info(f"Successfully checked out branch '{self.branch}'")
            
            return True
        except Exception as e:
            logger.error(f"Error verifying branch: {str(e)}")
            return False
    
    async def _get_personal_branch(self) -> str:
        """Finds the name of the user's personal branch.
        
        Returns:
            Personal branch name
        """
        branches = await self.client.get_all_branches(self.project)
        for branch in branches:
            if branch.get("personal") and not branch.get("readonly"):
                return branch["name"]
                
        raise BranchError(
            title=f"Personal branch not found",
            detail=f"Could not find a personal branch for project '{self.project}'"
        )
    
    async def _checkout_temp_branch(self, ref: str) -> str:
        """Creates a temporary branch off a commit or production.
        
        Args:
            ref: Git reference to base branch on
            
        Returns:
            Temporary branch name
        """
        # Save the dev mode state
        await self.client.update_workspace("dev")
        self.history.append(await self.get_project_state())
        
        # Generate a unique branch name
        branch_name = f"tmp_validator_{self._generate_hash()}"
        
        logger.debug(f"Creating temporary branch '{branch_name}' from ref '{ref}'")
        
        # Create and checkout the branch
        await self.client.create_branch(self.project, branch_name, ref)
        await self.client.checkout_branch(self.project, branch_name)
        
        self.is_temp_branch = True
        return branch_name
    
    async def _cleanup_temp_branch(self) -> None:
        """Clean up the temporary branch."""
        if not self.is_temp_branch or not self.branch:
            return
            
        try:
            logger.debug(f"Cleaning up temporary branch '{self.branch}'")
            
            # We need to be in dev mode to delete branches
            await self.client.update_workspace("dev")
            
            # Switch to the initial branch before deleting
            dev_state = self.history[1] if len(self.history) > 1 else None
            if dev_state and dev_state.branch:
                await self.client.checkout_branch(self.project, dev_state.branch)
                
            # Delete the temporary branch
            await self.client.delete_branch(self.project, self.branch)
            
            self.is_temp_branch = False
        except Exception as e:
            logger.warning(f"Failed to clean up temporary branch: {str(e)}")
    
    def _generate_hash(self) -> str:
        """Generate a short hash for temporary branch names."""
        hash_obj = hashlib.sha1()
        hash_obj.update(str(time.time()).encode('utf-8'))
        return hash_obj.hexdigest()[:8]