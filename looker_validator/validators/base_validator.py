"""
Base validator class for async validators.
"""

import logging
import os
import re
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

from looker_validator.async_client import AsyncLookerClient
from looker_validator.branch_manager import AsyncBranchManager
from looker_validator.exceptions import ValidatorError
from looker_validator.result_model import ValidationResult

logger = logging.getLogger(__name__)


class AsyncBaseValidator(ABC):
    """Base class for async validators."""
    
    def __init__(
        self,
        client: AsyncLookerClient,
        project: str,
        branch: Optional[str] = None,
        commit_ref: Optional[str] = None,
        remote_reset: bool = False,
        explores: Optional[List[str]] = None,
        log_dir: str = "logs",
        pin_imports: Optional[Dict[str, str]] = None,
        use_personal_branch: bool = False,
    ):
        """Initialize the base validator.
        
        Args:
            client: AsyncLookerClient instance
            project: Looker project name
            branch: Git branch name
            commit_ref: Git commit reference
            remote_reset: Whether to reset to remote branch state
            explores: List of explores to validate in format "model/explore"
            log_dir: Directory for logs
            pin_imports: Dictionary of project:ref pairs for imports
            use_personal_branch: Whether to use personal branch
        """
        self.client = client
        self.project = project
        self.branch = branch
        self.commit_ref = commit_ref
        self.remote_reset = remote_reset
        self.explore_selectors = explores or ["*/*"]
        self.log_dir = log_dir
        self.pin_imports_str = None
        
        # Parse pin_imports string to dict if provided as string
        self.pin_imports = {}
        if pin_imports:
            if isinstance(pin_imports, dict):
                self.pin_imports = pin_imports
            elif isinstance(pin_imports, str):
                self.pin_imports_str = pin_imports
                pairs = pin_imports.split(",")
                for pair in pairs:
                    if ":" in pair:
                        project_name, ref = pair.strip().split(":", 1)
                        self.pin_imports[project_name] = ref
        
        self.use_personal_branch = use_personal_branch
        
        # Create branch manager
        self.branch_manager = AsyncBranchManager(
            client,
            project,
            remote_reset,
            self.pin_imports,
            use_personal_branch
        )
        
        # Create log directory if it doesn't exist
        os.makedirs(self.log_dir, exist_ok=True)
        
        # Create cache directory
        self.cache_dir = os.path.join(self.log_dir, "cache")
        os.makedirs(self.cache_dir, exist_ok=True)
    
    @abstractmethod
    async def validate(self) -> ValidationResult:
        """Run validation.
        
        Returns:
            ValidationResult with the validation results
        """
        pass
    
    async def setup_branch(self, force_checkout: bool = False) -> None:
        """Set up branch for validation."""
        try:
            logger.info(f"Setting up branch for project {self.project}")
        
            # Enter branch manager context
            await self.branch_manager.__aenter__()
        
            # Set up branch
            await self.branch_manager.setup_branch(self.branch, self.commit_ref)
        
            # Verify branch state after setup
            workspace = await self.client.get_workspace()
            active_branch = await self.client.get_active_branch_name(self.project)
            logger.info(f"Validation will run in workspace '{workspace}' on branch '{active_branch}'")
        
            if self.branch and active_branch != self.branch:
                logger.warning(f"Active branch '{active_branch}' doesn't match requested branch '{self.branch}'")
                # Re-attempt checkout if needed
                await self.client.checkout_branch(self.project, self.branch)
                logger.info(f"Re-checked out branch '{self.branch}'")
            
            return True
        except Exception as e:
            logger.error(f"Failed to set up branch: {str(e)}")
            raise ValidatorError(
                title="Branch setup failed",
                detail=f"Failed to set up branch for validation: {str(e)}"
            )
    
    async def cleanup(self) -> None:
        """Clean up after validation."""
        try:
            # Exit branch manager context
            await self.branch_manager.__aexit__(None, None, None)
        except Exception as e:
            logger.error(f"Failed to clean up: {str(e)}")
    
    def parse_explore_selector(self, selector: str) -> Tuple[str, str, bool]:
        """Parse an explore selector string.
        
        Args:
            selector: Explore selector string in format "model/explore" or "-model/explore"
            
        Returns:
            Tuple of (model, explore, is_exclude)
        """
        is_exclude = selector.startswith("-")
        if is_exclude:
            selector = selector[1:]
        
        parts = selector.split("/", 1)
        
        if len(parts) == 1:
            return parts[0], "*", is_exclude
        else:
            return parts[0], parts[1], is_exclude
    
    def resolve_explores(self) -> Tuple[List[str], List[str]]:
        """Resolve explore selectors to includes and excludes.
        
        Returns:
            Tuple of (includes, excludes) lists
        """
        includes = []
        excludes = []
        
        for selector in self.explore_selectors:
            if selector.startswith("-"):
                excludes.append(selector[1:])
            else:
                includes.append(selector)
        
        return includes, excludes
    
    def _is_explore_selected(self, model: str, explore: str) -> bool:
        """Check if an explore is selected by the selectors.
        
        Args:
            model: Model name
            explore: Explore name
            
        Returns:
            True if the explore is selected
        """
        includes, excludes = self.resolve_explores()
        
        # Format as "model/explore"
        full_name = f"{model}/{explore}"
        
        # Check exclusions first (they take precedence)
        for exclude in excludes:
            # Handle wildcards
            if exclude == f"{model}/*":
                return False
            elif exclude == f"*/{explore}":
                return False
            elif exclude == full_name:
                return False
            
        # If no includes specified, include everything not excluded
        if not includes:
            return True
            
        # Check inclusions
        for include in includes:
            # Handle wildcards
            if include == f"{model}/*":
                return True
            elif include == f"*/{explore}":
                return True
            elif include == full_name:
                return True
            elif include == "*/*":
                return True
                
        # If we have includes but none matched, exclude this explore
        return False