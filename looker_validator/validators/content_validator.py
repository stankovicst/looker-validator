"""
Asynchronous content validator for checking dashboards and looks.
"""

import logging
import time
from typing import Dict, List, Optional, Any, Set, Tuple, cast

from looker_validator.async_client import AsyncLookerClient
from looker_validator.exceptions import ContentValidationError
from looker_validator.result_model import ContentError, ValidationResult, TestResult, SkipReason
from looker_validator.validators.base_validator import AsyncBaseValidator

logger = logging.getLogger(__name__)


class AsyncContentValidator(AsyncBaseValidator):
    """Validator for testing Looker content (dashboards and looks)."""
    
    def __init__(
        self,
        client: AsyncLookerClient,
        project: str,
        branch: Optional[str] = None,
        commit_ref: Optional[str] = None,
        remote_reset: bool = False,
        explores: Optional[List[str]] = None,
        folders: Optional[List[str]] = None,
        exclude_personal: bool = False,
        incremental: bool = False,
        target: Optional[str] = None,
        log_dir: str = "logs",
        pin_imports: Optional[Dict[str, str]] = None,
        use_personal_branch: bool = False,
    ):
        """Initialize the content validator.
        
        Args:
            client: AsyncLookerClient instance
            project: Looker project name
            branch: Git branch name
            commit_ref: Git commit reference
            remote_reset: Whether to reset to remote branch state
            explores: List of explores to validate in format "model/explore"
            folders: List of folder IDs to include or exclude (prefix with - to exclude)
            exclude_personal: Whether to exclude personal folders
            incremental: Whether to do incremental validation
            target: Target branch for incremental validation
            log_dir: Directory for logs
            pin_imports: Dictionary of project:ref pairs for imports
            use_personal_branch: Whether to use personal branch
        """
        super().__init__(
            client, 
            project, 
            branch,
            commit_ref,
            remote_reset,
            explores,
            log_dir,
            pin_imports,
            use_personal_branch
        )
        
        self.folders = folders or []
        self.exclude_personal = exclude_personal
        self.incremental = incremental
        self.target = target
        
        # Internal folder tracking
        self.included_folders: List[str] = []
        self.excluded_folders: List[str] = []
    
    async def validate(self) -> ValidationResult:
        """Run content validation.
        
        Returns:
            ValidationResult with the validation results
        """
        start_time = time.time()
        result = ValidationResult(validator="content", status="passed")
        
        try:
            # Set up branch for validation
            await self.setup_branch()
            
            # Process folder include/exclude list
            await self._process_folders()
            
            # Get all models and explores in the project for filtering content errors
            models_explores = await self._get_models_explores()
            
            # Run content validation on the current branch
            logger.info(f"Running content validation for project {self.project}")
            errors = await self._validate_content(models_explores)
            
            # For incremental mode, compare with target branch
            if self.incremental and (self.branch or self.commit_ref):
                # Get errors from target branch
                logger.info(f"Running incremental comparison against {self.target or 'production'}")
                target_errors = await self._run_target_validation(models_explores)
                
                # Filter out errors that also exist in target branch
                errors = self._filter_incremental_errors(errors, target_errors)
                
                logger.info(f"Found {len(errors)} errors unique to this branch")
            
            # Add errors to result
            for error in errors:
                result.add_error(error)
            
            # Add test results
            self._add_test_results(result, errors, models_explores)
            
            # Add timing information
            result.timing["total"] = time.time() - start_time
            
            return result
            
        except Exception as e:
            logger.error(f"Content validation failed: {str(e)}")
            raise ContentValidationError(
                title="Content validation failed",
                detail=f"Failed to validate content: {str(e)}"
            )
            
        finally:
            # Clean up branch manager
            await self.cleanup()
    
    async def _process_folders(self) -> None:
        """Process folder include/exclude selectors and their subfolders."""
        include_folders = []
        exclude_folders = []
        
        # Process direct folder selectors
        for folder in self.folders:
            folder_id = str(folder).strip()
            
            if folder_id.startswith("-"):
                # Remove the leading "-" and add to excludes
                exclude_folders.append(folder_id[1:])
            else:
                include_folders.append(folder_id)
        
        # Get personal folders if needed
        if self.exclude_personal:
            personal_folders = await self._get_personal_folders()
            exclude_folders.extend(personal_folders)
        
        # Expand to include subfolders
        self.included_folders = await self._expand_subfolders(include_folders) if include_folders else []
        self.excluded_folders = await self._expand_subfolders(exclude_folders) if exclude_folders else []
    
    async def _get_personal_folders(self) -> List[str]:
        """Get all personal folders.
        
        Returns:
            List of personal folder IDs
        """
        personal_folders = []
        
        # Get all folders
        all_folders = await self.client.all_folders()
        
        # Filter to personal folders
        for folder in all_folders:
            is_personal = False
            
            # Check various attributes that might indicate a personal folder
            if folder.get("is_personal"):
                is_personal = True
            elif folder.get("is_personal_descendant"):
                is_personal = True
            elif folder.get("personal_folder"):
                is_personal = True
            elif folder.get("parent_id") == "user":
                is_personal = True
            elif folder.get("name", "").lower() == "personal":
                is_personal = True
                
            if is_personal and folder.get("id"):
                personal_folders.append(str(folder["id"]))
        
        logger.debug(f"Found {len(personal_folders)} personal folders to exclude")
        return personal_folders
    
    async def _expand_subfolders(self, folder_ids: List[str]) -> List[str]:
        """Expand a list of folder IDs to include all subfolders.
        
        Args:
            folder_ids: List of folder IDs
            
        Returns:
            List of folder IDs including all subfolders
        """
        if not folder_ids:
            return []
            
        # Ensure input IDs are strings
        folder_ids = [str(fid) for fid in folder_ids]
        expanded_folders = list(folder_ids)
        
        # Get all folders
        all_folders = await self.client.all_folders()
        
        # Create a mapping of parent folders to their children
        parent_to_children = {}
        for folder in all_folders:
            parent_id = folder.get("parent_id")
            folder_id = folder.get("id")
            
            if parent_id and folder_id:
                parent_id = str(parent_id)
                folder_id = str(folder_id)
                
                if parent_id not in parent_to_children:
                    parent_to_children[parent_id] = []
                    
                parent_to_children[parent_id].append(folder_id)
        
        # Recursively add subfolders
        to_process = list(folder_ids)
        processed = set(folder_ids)
        
        while to_process:
            parent_id = to_process.pop(0)
            children = parent_to_children.get(parent_id, [])
            
            for child_id in children:
                if child_id not in processed:
                    expanded_folders.append(child_id)
                    to_process.append(child_id)
                    processed.add(child_id)
        
        logger.debug(f"Expanded {len(folder_ids)} folders to {len(expanded_folders)} folders with subfolders")
        return expanded_folders
    
    def _is_folder_selected(self, folder_id: Optional[str]) -> bool:
        """Determine if content in a folder should be included based on filters.
        
        Args:
            folder_id: Folder ID to check
            
        Returns:
            True if the folder should be included
        """
        # Some content might not have a folder (e.g., LookML dashboards)
        if folder_id is None:
            # Include content without folders unless specific folders are requested
            return not bool(self.included_folders)
        
        # Check exclusions first (they take precedence)
        if folder_id in self.excluded_folders:
            return False
            
        # If includes are specified, only include matching folders
        if self.included_folders and folder_id not in self.included_folders:
            return False
            
        return True
    
    async def _get_models_explores(self) -> Dict[str, Set[str]]:
        """Get all models and explores in the project.
        
        Returns:
            Dictionary mapping model names to sets of explore names
        """
        # Get all LookML models in the project
        logger.debug("Getting LookML models")
        models = await self.client.get_lookml_models(fields=["name", "project_name", "explores"])
        
        # Filter to models in this project
        project_models = [model for model in models if model["project_name"] == self.project]
        
        # Create model->explores mapping
        models_explores = {}
        for model in project_models:
            model_name = model["name"]
            
            if model_name not in models_explores:
                models_explores[model_name] = set()
                
            for explore_data in model["explores"]:
                explore_name = explore_data["name"]
                
                # Check if explore matches selectors
                if self._is_explore_selected(model_name, explore_name):
                    models_explores[model_name].add(explore_name)
        
        return models_explores
    
    async def _validate_content(self, models_explores: Dict[str, Set[str]]) -> List[ContentError]:
        """Run content validation.
        
        Args:
            models_explores: Dictionary mapping model names to sets of explore names
            
        Returns:
            List of ContentError objects
        """
        # Run content validation
        validation = await self.client.content_validation()
        
        # Process errors
        return self._process_content_errors(validation, models_explores)
    
    def _process_content_errors(
        self, 
        validation: Dict[str, Any], 
        models_explores: Dict[str, Set[str]]
    ) -> List[ContentError]:
        """Process content validation errors.
        
        Args:
            validation: Raw validation results from Looker API
            models_explores: Dictionary mapping model names to sets of explore names
            
        Returns:
            List of ContentError objects
        """
        errors = []
        
        # Get content with errors
        content_with_errors = validation.get("content_with_errors", [])
        
        for content in content_with_errors:
            # Determine content type (look or dashboard)
            content_type = None
            content_obj = None
            
            for type_name in ("look", "dashboard", "lookml_dashboard", "scheduled_plan"):
                if type_name in content and content[type_name]:
                    content_type = type_name
                    content_obj = content[type_name]
                    break
            
            if not content_type or not content_obj:
                logger.debug(f"Could not determine content type for error item: {content}")
                continue
            
            # Get folder ID
            folder_id = None
            if "folder" in content_obj and content_obj["folder"]:
                folder_id = str(content_obj["folder"].get("id"))
            elif "space_id" in content_obj:
                folder_id = str(content_obj["space_id"])
            elif "folder_id" in content_obj:
                folder_id = str(content_obj["folder_id"])
            
            # Apply folder filtering
            if not self._is_folder_selected(folder_id):
                continue
            
            # Get folder name
            folder_name = None
            if "folder" in content_obj and content_obj["folder"]:
                folder_name = content_obj["folder"].get("name")
            
            # Get content title and ID
            content_id = content_obj.get("id", "unknown")
            content_title = content_obj.get("title", f"{content_type.capitalize()} {content_id}")
            
            # Create content URL
            if content_type == "lookml_dashboard":
                # Assumes dashboard_id is in the format 'project::dashboard_file_name'
                url_part = content_id.replace('::', '/') if '::' in content_id else content_id
                content_url = f"{self.client.base_url}/dashboards-next/{url_part}"
            else:
                content_url = f"{self.client.base_url}/{content_type}s/{content_id}"
            
            # Process each error for this content
            for error in content.get("errors", []):
                model_name = error.get("model_name")
                explore_name = error.get("explore_name")
                field_name = error.get("field_name", "")
                message = error.get("message", "Unknown error")
                
                # Skip errors that don't match the model/explore filter
                if not (model_name and explore_name):
                    continue
                    
                if (model_name not in models_explores or 
                    explore_name not in models_explores[model_name]):
                    continue
                
                # Get dashboard element info for dashboards
                tile_type = None
                tile_title = None
                
                if content_type in ("dashboard", "lookml_dashboard"):
                    element = content.get("dashboard_element")
                    if element:
                        tile_type = "tile"
                        tile_title = element.get("title", f"Element {element.get('id', 'unknown')}")
                    else:
                        filter_element = content.get("dashboard_filter")
                        if filter_element:
                            tile_type = "filter"
                            tile_title = filter_element.get("title", f"Filter {filter_element.get('id', 'unknown')}")
                
                # Create error object
                content_error = ContentError(
                    model=model_name,
                    explore=explore_name,
                    message=message,
                    field_name=field_name,
                    content_type=content_type,
                    title=content_title,
                    folder=folder_name,
                    url=content_url,
                    tile_type=tile_type,
                    tile_title=tile_title
                )
                
                errors.append(content_error)
        
        return errors
    
    async def _run_target_validation(self, models_explores: Dict[str, Set[str]]) -> List[ContentError]:
        """Run content validation on target branch for incremental comparison.
        
        Args:
            models_explores: Dictionary mapping model names to sets of explore names
            
        Returns:
            List of ContentError objects
        """
        # Save current branch/commit
        current_branch = self.branch
        current_commit = self.commit_ref
        
        try:
            # Switch to target branch
            self.branch = self.target
            self.commit_ref = None if self.target else None
            
            # Set up branch
            await self.setup_branch()
            
            # Run validation on target branch
            logger.info(f"Running content validation on {self.target or 'production'}")
            return await self._validate_content(models_explores)
            
        finally:
            # Restore original branch/commit
            self.branch = current_branch
            self.commit_ref = current_commit
            
            # Switch back
            await self.setup_branch()
    
    def _filter_incremental_errors(
        self, 
        errors: List[ContentError], 
        target_errors: List[ContentError]
    ) -> List[ContentError]:
        """Filter out errors that also exist in target branch.
        
        Args:
            errors: Errors from current branch
            target_errors: Errors from target branch
            
        Returns:
            List of ContentError objects unique to current branch
        """
        if not target_errors:
            return errors
            
        # Create a set of error keys for efficient lookup
        target_error_keys = set()
        for error in target_errors:
            # Create a unique key for the error
            key = (
                error.content_type,
                error.model,
                error.explore,
                error.field_name,
                error.message.strip()
            )
            target_error_keys.add(key)
        
        # Filter errors to those not in target branch
        filtered_errors = []
        for error in errors:
            key = (
                error.content_type,
                error.model,
                error.explore,
                error.field_name,
                error.message.strip()
            )
            
            if key not in target_error_keys:
                filtered_errors.append(error)
        
        return filtered_errors
    
    def _add_test_results(
        self,
        result: ValidationResult,
        errors: List[ContentError],
        models_explores: Dict[str, Set[str]]
    ) -> None:
        """Add test results for all models and explores.
        
        Args:
            result: ValidationResult to update
            errors: List of content errors
            models_explores: Dictionary mapping model names to sets of explore names
        """
        # Create a mapping of model/explore to error status
        error_status: Dict[Tuple[str, str], bool] = {}
        
        # Set all explored combinations to passed initially
        for model, explores in models_explores.items():
            for explore in explores:
                error_status[(model, explore)] = False
        
        # Mark errored model/explore combinations
        for error in errors:
            error_status[(error.model, error.explore)] = True
        
        # Add test results
        for (model, explore), has_error in error_status.items():
            test_result = TestResult(
                model=model,
                explore=explore,
                status="failed" if has_error else "passed"
            )
            result.add_test_result(test_result)