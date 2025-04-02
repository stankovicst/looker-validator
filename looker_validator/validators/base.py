"""
Base validator class that all validator implementations extend.
"""

import logging
import os
import time
import json
import hashlib
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple, Set

from looker_validator.connection import LookerConnection
from looker_validator.exceptions import ValidatorError

logger = logging.getLogger(__name__)

# Keep the class here for backward compatibility
class ValidatorError(Exception):
    """Exception raised when a validation fails."""
    pass


class BaseValidator(ABC):
    """Base class for all Looker validators."""

    def __init__(self, connection: LookerConnection, project: str, **kwargs):
        """Initialize the validator.

        Args:
            connection: LookerConnection instance
            project: Looker project name
            **kwargs: Additional validator options
        """
        self.connection = connection
        self.sdk = connection.sdk
        self.project = project
        self.branch = kwargs.get("branch")
        self.commit_ref = kwargs.get("commit_ref")
        self.remote_reset = kwargs.get("remote_reset", False)
        self.log_dir = kwargs.get("log_dir", "logs")
        self.pin_imports = kwargs.get("pin_imports")
        self.api_version = connection.api_version
        
        # Explore selection
        self.explore_selectors = kwargs.get("explores", [])
        
        # Working branch management
        self.temp_branch = None
        
        # Create log directory if it doesn't exist
        os.makedirs(self.log_dir, exist_ok=True)
        
        # Initialize cache directory
        self.cache_dir = os.path.join(self.log_dir, "cache")
        os.makedirs(self.cache_dir, exist_ok=True)

    @abstractmethod
    def validate(self) -> bool:
        """Run validation.

        Returns:
            True if validation succeeds, False otherwise
        """
        pass

    def setup_branch(self):
        """Set up the branch for validation.
        
        This method handles branch checkout, commit checkout, or remote reset
        based on the provided options.
        
        Returns:
            True if branch setup was successful
        """
        try:
            logger.info(f"Setting up branch for project {self.project}")
            
            if self.commit_ref:
                # Checkout specific commit
                self.temp_branch = self.connection.checkout_commit(
                    self.project, self.commit_ref
                )
                logger.info(f"Checked out commit {self.commit_ref} to temp branch {self.temp_branch}")
            elif self.branch and self.remote_reset:
                # Reset branch to remote state
                self.connection.reset_to_remote(self.project, self.branch)
                logger.info(f"Reset branch {self.branch} to remote state")
                # Then checkout the branch
                self.connection.switch_project_branch(self.project, self.branch)
            else:
                # Regular branch checkout (or production)
                self.connection.switch_project_branch(self.project, self.branch)
                
            return True
        except Exception as e:
            logger.error(f"Failed to set up branch: {str(e)}")
            raise ValidatorError(f"Branch setup failed: {str(e)}")

    def cleanup(self):
        """Clean up after validation.
        
        This method removes any temporary branches if they were created.
        """
        if self.temp_branch:
            logger.info(f"Cleaning up temporary branch {self.temp_branch}")
            self.connection.cleanup_temp_branch(self.project, self.temp_branch)
            self.temp_branch = None

    def resolve_explores(self) -> Tuple[List[str], List[str]]:
        """Resolve explore selectors to include/exclude lists.
        
        Parses the explore selectors (like "model_a/*", "-model_b/explore_c")
        into separate include and exclude lists.
        
        Returns:
            Tuple of (includes, excludes) where each item is a list of 
            "model_name/explore_name" strings
        """
        includes = []
        excludes = []
        
        for selector in self.explore_selectors:
            if selector.startswith("-"):
                # This is an exclude pattern
                excludes.append(selector[1:])  # Remove the leading "-"
            else:
                # This is an include pattern
                includes.append(selector)
                
        return includes, excludes

    def matches_selector(self, model_name: str, explore_name: str, 
                        includes: List[str], excludes: List[str]) -> bool:
        """Check if a model/explore matches the include/exclude selectors.
        
        Args:
            model_name: Name of the model
            explore_name: Name of the explore
            includes: List of include patterns
            excludes: List of exclude patterns
            
        Returns:
            True if the model/explore should be included
        """
        # Format as "model_name/explore_name"
        full_name = f"{model_name}/{explore_name}"
        
        # Check exclusions first (they take precedence)
        for exclude in excludes:
            # Handle wildcards
            if exclude == f"{model_name}/*":
                return False
            elif exclude == f"*/{explore_name}":
                return False
            elif exclude == full_name:
                return False
        
        # If no includes specified, include everything not excluded
        if not includes:
            return True
            
        # Check inclusions
        for include in includes:
            # Handle wildcards
            if include == f"{model_name}/*":
                return True
            elif include == f"*/{explore_name}":
                return True
            elif include == full_name:
                return True
                
        # If we have includes but none matched, exclude this explore
        return False

    def pin_imported_projects(self):
        """Pin imported projects to specific refs if specified."""
        if not self.pin_imports:
            return
            
        try:
            # Parse pin_imports string (format: "project:ref,project2:ref2")
            pin_specs = self.pin_imports.split(",")
            for pin_spec in pin_specs:
                if ":" not in pin_spec:
                    logger.warning(f"Invalid pin format: {pin_spec}, skipping")
                    continue
                    
                project_name, ref = pin_spec.strip().split(":", 1)
                logger.info(f"Pinning imported project {project_name} to {ref}")
                
                # Use SDK to update the imported project ref
                # Note: This assumes your Looker SDK version supports this
                try:
                    self.sdk.update_git_branch(project_name, ref)
                except Exception as import_e:
                    logger.warning(f"Failed to pin imported project {project_name}: {str(import_e)}")
        except Exception as e:
            logger.warning(f"Failed to pin imported projects: {str(e)}")

    def log_timing(self, name: str, start_time: float):
        """Log the execution time of an operation.
        
        Args:
            name: Name of the operation
            start_time: Start time from time.time()
        """
        elapsed = time.time() - start_time
        logger.info(f"Completed {name} in {elapsed:.2f} seconds")
    
    # === Cache system methods ===
    
    def _setup_cache_dir(self):
        """Set up cache directory."""
        self.cache_dir = os.path.join(self.log_dir, "cache")
        os.makedirs(self.cache_dir, exist_ok=True)
    
    def _generate_cache_key(self, model: str, explore: str) -> str:
        """Generate a cache key for explore validation.
        
        Args:
            model: Model name
            explore: Explore name
            
        Returns:
            MD5 hash string as cache key
        """
        key_string = f"{self.project}_{model}_{explore}"
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def _save_validation_cache(self, model: str, explore: str, result: Any):
        """Cache validation results by hash.
        
        Args:
            model: Model name
            explore: Explore name
            result: Result data to cache
        """
        if not hasattr(self, 'cache_dir'):
            self._setup_cache_dir()
            
        cache_key = self._generate_cache_key(model, explore)
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")
        
        try:
            with open(cache_file, "w") as f:
                json.dump(result, f)
            logger.debug(f"Cached results for {model}/{explore}")
        except Exception as e:
            logger.warning(f"Failed to cache results: {str(e)}")
    
    def _check_validation_cache(self, model: str, explore: str) -> Optional[Any]:
        """Check for cached results.
        
        Args:
            model: Model name
            explore: Explore name
            
        Returns:
            Cached result data or None if not found
        """
        if not hasattr(self, 'cache_dir'):
            self._setup_cache_dir()
            
        cache_key = self._generate_cache_key(model, explore)
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")
        
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r") as f:
                    cache_data = json.load(f)
                logger.debug(f"Using cached results for {model}/{explore}")
                return cache_data
            except Exception as e:
                logger.warning(f"Failed to read cache: {str(e)}")
                
        return None
    
    def _clear_cache(self, model: Optional[str] = None, explore: Optional[str] = None):
        """Clear cache for specific model/explore or all caches.
        
        Args:
            model: Model name (None for all models)
            explore: Explore name (None for all explores)
        """
        if not hasattr(self, 'cache_dir'):
            self._setup_cache_dir()
            
        if not model and not explore:
            # Clear all caches
            for cache_file in os.listdir(self.cache_dir):
                if cache_file.endswith(".json"):
                    os.remove(os.path.join(self.cache_dir, cache_file))
            logger.debug("Cleared all validation caches")
        else:
            # Clear specific cache
            cache_key = self._generate_cache_key(model or "", explore or "")
            cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")
            
            if os.path.exists(cache_file):
                os.remove(cache_file)
                logger.debug(f"Cleared cache for {model}/{explore}")
    
    def _cleanup_between_tests(self):
        """Force garbage collection between major operations."""
        import gc
        gc.collect()