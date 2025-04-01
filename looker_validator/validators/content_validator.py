"""
Enhanced content validator with better folder filtering and content type handling.
"""

import logging
import os
import time
import json
from typing import List, Dict, Any, Optional, Tuple, Set

from looker_validator.validators.base import BaseValidator, ValidatorError
from looker_validator.exceptions import ContentValidationError

logger = logging.getLogger(__name__)


class ContentValidator(BaseValidator):
    """Validator for testing Looker content (Looks and Dashboards)."""

    def __init__(self, connection, project, **kwargs):
        """Initialize Content validator.

        Args:
            connection: LookerConnection instance
            project: Looker project name
            **kwargs: Additional validator options
        """
        super().__init__(connection, project, **kwargs)
        self.folders = kwargs.get("folders", [])
        self.exclude_personal = kwargs.get("exclude_personal", False)
        self.incremental = kwargs.get("incremental", False)
        self.target = kwargs.get("target")
        
        # Content validation results
        self.errors = {}
        self.production_errors = {}  # For incremental validation
        
        # Tracking counts by error type
        self.error_counts = {
            "look": 0,
            "dashboard": 0,
            "lookml_dashboard": 0,
            "scheduled_plan": 0,
            "other": 0  # Catch-all for any other content types
        }

    def validate(self) -> bool:
        """Run content validation.

        Returns:
            True if all content is valid, False otherwise
        """
        start_time = time.time()
        
        try:
            # Set up branch for validation
            self.setup_branch()
            
            # Get all models and explores in the project
            logger.info(f"Finding explores for project {self.project}")
            explores = self._get_all_explores()
            
            if not explores:
                logger.warning(f"No explores found for project {self.project}")
                return True
                
            # Filter explores based on selectors
            explores = self._filter_explores(explores)
            
            if not explores:
                logger.warning(f"No explores match the provided selectors")
                return True
            
            # Process folder include/exclude list
            self.included_folders, self.excluded_folders = self._process_folders()
            
            # Run content validation on the current branch
            logger.info(f"Running content validation for project {self.project}")
            self._validate_content(explores, self.included_folders, self.excluded_folders)
            
            # If in incremental mode, run on target branch as well
            if self.incremental and self.branch:
                logger.info(f"Running incremental comparison against {self.target or 'production'}")
                self._run_target_validation(explores, self.included_folders, self.excluded_folders)
                
                # Filter out errors that also exist in the target branch
                self._filter_incremental_errors()
            
            # Log results
            self._log_results()
            
            self.log_timing("Content validation", start_time)
            
            # Return True if no errors
            return len(self.errors) == 0
        
        finally:
            # Clean up temporary branch if needed
            self.cleanup()

    def _get_all_explores(self) -> List[Dict[str, str]]:
        """Get all explores in the project.

        Returns:
            List of explore dictionaries with 'model' and 'name' keys
        """
        try:
            # Get all models in the project
            models_response = self.sdk.all_lookml_models()
            
            # Find models for this project
            project_models = [
                model for model in models_response
                if model.project_name == self.project
            ]
            
            explores = []
            
            # Get all explores for each model
            for model in project_models:
                model_detail = self.sdk.lookml_model(model.name)
                
                for explore in model_detail.explores:
                    explores.append({
                        "model": model.name,
                        "name": explore.name
                    })
            
            return explores
        
        except Exception as e:
            logger.error(f"Failed to get explores: {str(e)}")
            raise ValidatorError(f"Failed to get explores: {str(e)}")

    def _filter_explores(self, explores: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Filter explores based on selectors.

        Args:
            explores: List of explore dictionaries

        Returns:
            Filtered list of explore dictionaries
        """
        if not self.explore_selectors:
            return explores
            
        includes, excludes = self.resolve_explores()
        
        filtered_explores = [
            explore for explore in explores
            if self.matches_selector(
                explore["model"], 
                explore["name"],
                includes, 
                excludes
            )
        ]
        
        return filtered_explores

    def _process_folders(self) -> Tuple[List[str], List[str]]:
        """Process folder include/exclude selectors and their subfolders.

        Returns:
            Tuple of (include_folders, exclude_folders) with folder IDs
        """
        include_folders = []
        exclude_folders = []
        
        # Process direct folder selectors
        for folder in self.folders:
            try:
                # Strip leading/trailing whitespace
                folder = str(folder).strip()
                
                # Check if it's an exclude pattern
                if folder.startswith("-"):
                    # Remove the leading "-" and add to excludes
                    folder_id = folder[1:]
                    exclude_folders.append(folder_id)
                else:
                    # Add to includes
                    include_folders.append(folder)
            except ValueError:
                logger.warning(f"Invalid folder ID: {folder}")
        
        # Get personal folders if needed
        if self.exclude_personal:
            personal_folders = self._get_personal_folders()
            exclude_folders.extend(personal_folders)
        
        # Expand to include subfolders
        expanded_includes = self._expand_subfolders(include_folders) if include_folders else []
        expanded_excludes = self._expand_subfolders(exclude_folders) if exclude_folders else []
        
        return expanded_includes, expanded_excludes

    def _get_personal_folders(self) -> List[str]:
        """Get all personal folders in the Looker instance.

        Returns:
            List of personal folder IDs
        """
        personal_folders = []
        
        try:
            # Get all folders
            all_folders = self.sdk.all_folders()
            
            # Filter to personal folders
            for folder in all_folders:
                if hasattr(folder, 'is_personal') and folder.is_personal:
                    personal_folders.append(folder.id)
                elif hasattr(folder, 'is_personal_descendant') and folder.is_personal_descendant:
                    personal_folders.append(folder.id)
                # For API 4.0, these attributes might be different
                elif hasattr(folder, 'personal_folder') and folder.personal_folder:
                    personal_folders.append(folder.id)
                elif hasattr(folder, 'parent_id') and folder.parent_id == 'user':
                    personal_folders.append(folder.id)
                
            logger.debug(f"Found {len(personal_folders)} personal folders")
            
        except Exception as e:
            logger.warning(f"Failed to get personal folders: {str(e)}")
            
        return personal_folders

    def _expand_subfolders(self, folder_ids: List[str]) -> List[str]:
        """Expand a list of folder IDs to include all subfolders.

        Args:
            folder_ids: List of folder IDs

        Returns:
            List of folder IDs including all subfolders
        """
        if not folder_ids:
            return []
            
        expanded_folders = list(folder_ids)  # Start with the original folders
        
        try:
            # Get all folders
            all_folders = self.sdk.all_folders()
            
            # Create a mapping of parent folders to their children
            parent_to_children = {}
            for folder in all_folders:
                parent_id = getattr(folder, 'parent_id', None)
                if parent_id:
                    if parent_id not in parent_to_children:
                        parent_to_children[parent_id] = []
                    parent_to_children[parent_id].append(folder.id)
            
            # Recursively add subfolders
            to_process = list(folder_ids)
            while to_process:
                parent_id = to_process.pop(0)
                children = parent_to_children.get(parent_id, [])
                for child_id in children:
                    if child_id not in expanded_folders:
                        expanded_folders.append(child_id)
                        to_process.append(child_id)
            
            logger.debug(f"Expanded {len(folder_ids)} folders to {len(expanded_folders)} with subfolders")
            
        except Exception as e:
            logger.warning(f"Failed to expand subfolders: {str(e)}")
            
        return expanded_folders

    def _validate_content(
        self, 
        explores: List[Dict[str, str]], 
        include_folders: List[str],
        exclude_folders: List[str]
    ):
        """Run content validation.

        Args:
            explores: List of explore dictionaries
            include_folders: List of folder IDs to include
            exclude_folders: List of folder IDs to exclude
        """
        try:
            # Get content validation results
            logger.info("Getting content validation results")
            
            # Create model/explore set for filtering
            explore_set = {f"{e['model']}.{e['name']}" for e in explores}
            
            # Run content validation
            validation = self.sdk.content_validation()
            
            # Log validation details
            logger.debug(f"Content validation response attributes: {dir(validation)}")
            
            # Process content with errors based on API version format
            self._process_content_errors(validation, explore_set, include_folders, exclude_folders)
                
        except Exception as e:
            logger.error(f"Failed to validate content: {str(e)}")
            raise ContentValidationError(f"Content validation failed: {str(e)}")

    def _process_content_errors(
        self,
        validation: Any,
        explore_set: Set[str],
        include_folders: List[str],
        exclude_folders: List[str]
    ):
        """Process content validation errors from different API formats.

        Args:
            validation: Content validation response
            explore_set: Set of model.explore strings for filtering
            include_folders: List of folder IDs to include
            exclude_folders: List of folder IDs to exclude
        """
        # Try API 4.0 attribute 'content_with_errors'
        if hasattr(validation, 'content_with_errors'):
            self._process_api40_content_errors(
                validation.content_with_errors,
                explore_set,
                include_folders,
                exclude_folders
            )
        # Try traditional attributes
        else:
            # Process each error type
            error_types = [
                ('look_errors', self._process_look_errors),
                ('dashboard_errors', self._process_dashboard_errors),
                ('lookml_dashboard_errors', self._process_lookml_dashboard_errors),
                ('scheduled_plan_errors', self._process_scheduled_plan_errors)
            ]
            
            for attr_name, process_func in error_types:
                if hasattr(validation, attr_name) and getattr(validation, attr_name):
                    process_func(
                        getattr(validation, attr_name),
                        explore_set,
                        include_folders,
                        exclude_folders
                    )

    def _process_api40_content_errors(
        self,
        content_items: List[Any],
        explore_set: Set[str],
        include_folders: List[str],
        exclude_folders: List[str]
    ):
        """Process API 4.0 content_with_errors format.

        Args:
            content_items: List of content items with errors
            explore_set: Set of model.explore strings for filtering
            include_folders: List of folder IDs to include
            exclude_folders: List of folder IDs to exclude
        """
        for content in content_items:
            # Determine content type
            content_type = None
            content_obj = None
            
            # Check for different content types
            for type_name in ['look', 'dashboard', 'lookml_dashboard', 'scheduled_plan']:
                if hasattr(content, type_name) and getattr(content, type_name):
                    content_type = type_name
                    content_obj = getattr(content, type_name)
                    break
            
            if not content_type or not content_obj:
                logger.debug(f"Unknown content type: {content}")
                continue
            
            # Check folder filtering
            folder = getattr(content_obj, 'folder', None)
            folder_id = getattr(folder, 'id', None) if folder else None
            
            if not self._should_include_folder(folder_id, include_folders, exclude_folders):
                continue
            
            # Process errors based on content type
            errors = getattr(content, 'errors', [])
            
            for error in errors:
                model_name = getattr(error, 'model_name', None)
                explore_name = getattr(error, 'explore_name', None)
                
                # Skip if not in our explore set
                if not model_name or not explore_name:
                    continue
                    
                if f"{model_name}.{explore_name}" not in explore_set:
                    continue
                
                # Process based on content type
                if content_type == 'look':
                    self._add_look_error(content_obj, error)
                elif content_type == 'dashboard':
                    element = content.dashboard_element if hasattr(content, 'dashboard_element') else None
                    self._add_dashboard_error(content_obj, error, element)
                elif content_type == 'lookml_dashboard':
                    element = content.dashboard_element if hasattr(content, 'dashboard_element') else None
                    self._add_lookml_dashboard_error(content_obj, error, element)
                elif content_type == 'scheduled_plan':
                    self._add_scheduled_plan_error(content_obj, error)
                else:
                    self._add_generic_content_error(content_type, content_obj, error)

    def _should_include_folder(
        self,
        folder_id: Optional[str],
        include_folders: List[str],
        exclude_folders: List[str]
    ) -> bool:
        """Determine if content in a folder should be included based on filters.

        Args:
            folder_id: Folder ID
            include_folders: List of folder IDs to include
            exclude_folders: List of folder IDs to exclude

        Returns:
            True if the folder should be included
        """
        # Always skip content without a folder
        if folder_id is None:
            return False
            
        # Exclusions take precedence
        if folder_id in exclude_folders:
            return False
            
        # If includes are specified, only include matching folders
        if include_folders and folder_id not in include_folders:
            return False
            
        return True

    def _add_look_error(self, look: Any, error: Any):
        """Add a Look error to the results.

        Args:
            look: Look object
            error: Error object
        """
        look_id = getattr(look, 'id', 'unknown')
        error_id = f"look-{look_id}"
        
        self.errors[error_id] = {
            "type": "look",
            "id": look_id,
            "title": getattr(look, 'title', None) or f"Look {look_id}",
            "url": f"{self.connection.base_url}/looks/{look_id}",
            "space_id": getattr(look, 'space_id', None) or getattr(look, 'folder_id', None),
            "space_name": getattr(look, 'space_name', None) or getattr(look, 'folder_name', None),
            "model": getattr(error, 'model_name', None),
            "explore": getattr(error, 'explore_name', None),
            "message": getattr(error, 'message', None) or "Unknown error"
        }
        
        self.error_counts["look"] += 1

    def _add_dashboard_error(self, dashboard: Any, error: Any, element: Any = None):
        """Add a dashboard error to the results.

        Args:
            dashboard: Dashboard object
            error: Error object
            element: Dashboard element object
        """
        dashboard_id = getattr(dashboard, 'id', 'unknown')
        element_id = getattr(element, 'id', 'unknown') if element else 'unknown'
        error_id = f"dashboard-{dashboard_id}-{element_id}"
        
        self.errors[error_id] = {
            "type": "dashboard",
            "id": dashboard_id,
            "element_title": getattr(element, 'title', None) if element else "Unknown element",
            "url": f"{self.connection.base_url}/dashboards/{dashboard_id}",
            "dashboard_title": getattr(dashboard, 'title', None) or f"Dashboard {dashboard_id}",
            "space_id": getattr(dashboard, 'space_id', None) or getattr(dashboard, 'folder_id', None),
            "space_name": getattr(dashboard, 'space_name', None) or getattr(dashboard, 'folder_name', None),
            "model": getattr(error, 'model_name', None),
            "explore": getattr(error, 'explore_name', None),
            "message": getattr(error, 'message', None) or "Unknown error"
        }
        
        self.error_counts["dashboard"] += 1

    def _add_lookml_dashboard_error(self, dashboard: Any, error: Any, element: Any = None):
        """Add a LookML dashboard error to the results.

        Args:
            dashboard: LookML dashboard object
            error: Error object
            element: Dashboard element object
        """
        dashboard_id = getattr(dashboard, 'id', 'unknown')
        element_id = getattr(element, 'id', 'unknown') if element else 'unknown'
        error_id = f"lookml-dashboard-{dashboard_id}-{element_id}"
        
        self.errors[error_id] = {
            "type": "lookml_dashboard",
            "id": dashboard_id,
            "element_title": getattr(element, 'title', None) if element else "Unknown element",
            "url": f"{self.connection.base_url}/dashboards/lookml/{dashboard_id}",
            "dashboard_title": getattr(dashboard, 'title', None) or f"LookML Dashboard {dashboard_id}",
            "model": getattr(error, 'model_name', None),
            "explore": getattr(error, 'explore_name', None),
            "message": getattr(error, 'message', None) or "Unknown error"
        }
        
        self.error_counts["lookml_dashboard"] += 1

    def _add_scheduled_plan_error(self, plan: Any, error: Any):
        """Add a scheduled plan error to the results.

        Args:
            plan: Scheduled plan object
            error: Error object
        """
        plan_id = getattr(plan, 'id', 'unknown')
        error_id = f"scheduled-plan-{plan_id}"
        
        self.errors[error_id] = {
            "type": "scheduled_plan",
            "id": plan_id,
            "title": getattr(plan, 'title', None) or f"Scheduled Plan {plan_id}",
            "url": f"{self.connection.base_url}/admin/scheduled_plans/{plan_id}",
            "user_id": getattr(plan, 'user_id', None),
            "model": getattr(error, 'model_name', None),
            "explore": getattr(error, 'explore_name', None),
            "message": getattr(error, 'message', None) or "Unknown error"
        }
        
        self.error_counts["scheduled_plan"] += 1

    def _add_generic_content_error(self, content_type: str, content_obj: Any, error: Any):
        """Add a generic content error for any other content type.

        Args:
            content_type: Type of content
            content_obj: Content object
            error: Error object
        """
        content_id = getattr(content_obj, 'id', 'unknown')
        error_id = f"{content_type}-{content_id}"
        
        self.errors[error_id] = {
            "type": content_type,
            "id": content_id,
            "title": getattr(content_obj, 'title', None) or f"{content_type.capitalize()} {content_id}",
            "url": f"{self.connection.base_url}/{content_type}s/{content_id}",
            "model": getattr(error, 'model_name', None),
            "explore": getattr(error, 'explore_name', None),
            "message": getattr(error, 'message', None) or "Unknown error"
        }
        
        self.error_counts["other"] += 1

    def _process_look_errors(
        self, 
        look_errors: List[Any],
        explore_set: Set[str],
        include_folders: List[str],
        exclude_folders: List[str]
    ):
        """Process Look validation errors.

        Args:
            look_errors: List of Look error objects
            explore_set: Set of model.explore strings for filtering
            include_folders: List of folder IDs to include
            exclude_folders: List of folder IDs to exclude
        """
        for error in look_errors:
            # Get space ID as integer if possible
            try:
                space_id = getattr(error, 'space_id', None) or getattr(error, 'folder_id', None)
            except (ValueError, TypeError):
                space_id = None
                
            # Skip if in excluded folder
            if not self._should_include_folder(space_id, include_folders, exclude_folders):
                continue
                
            # Check if this error is related to our project
            model_name = getattr(error, 'model_name', None)
            explore_name = getattr(error, 'explore_name', None)
            
            if not model_name or not explore_name or f"{model_name}.{explore_name}" not in explore_set:
                continue
                
            # Record error
            look_id = getattr(error, 'id', 'unknown')
            error_id = f"look-{look_id}"
            
            self.errors[error_id] = {
                "type": "look",
                "id": look_id,
                "title": getattr(error, 'title', None) or f"Look {look_id}",
                "url": f"{self.connection.base_url}/looks/{look_id}",
                "space_id": space_id,
                "space_name": getattr(error, 'space_name', None) or getattr(error, 'folder_name', None) or "Unknown",
                "model": model_name,
                "explore": explore_name,
                "message": getattr(error, 'message', None) or "Unknown error"
            }
            
            # Update count
            self.error_counts["look"] += 1

    def _process_dashboard_errors(
        self, 
        dashboard_errors: List[Any],
        explore_set: Set[str],
        include_folders: List[str],
        exclude_folders: List[str]
    ):
        """Process Dashboard validation errors.

        Args:
            dashboard_errors: List of Dashboard error objects
            explore_set: Set of model.explore strings for filtering
            include_folders: List of folder IDs to include
            exclude_folders: List of folder IDs to exclude
        """
        for error in dashboard_errors:
            # Get space ID
            try:
                space_id = getattr(error, 'space_id', None) or getattr(error, 'folder_id', None)
            except (ValueError, TypeError):
                space_id = None
                
            # Skip if in excluded folder
            if not self._should_include_folder(space_id, include_folders, exclude_folders):
                continue
                
            # Check if this error is related to our project
            model_name = getattr(error, 'model_name', None)
            explore_name = getattr(error, 'explore_name', None)
            
            if not model_name or not explore_name or f"{model_name}.{explore_name}" not in explore_set:
                continue
                
            # Record error
            dashboard_id = getattr(error, 'dashboard_id', 'unknown')
            element_id = getattr(error, 'id', 'unknown')
            error_id = f"dashboard-{dashboard_id}-{element_id}"
            
            self.errors[error_id] = {
                "type": "dashboard",
                "id": dashboard_id,
                "element_title": getattr(error, 'element_title', None) or "Unknown element",
                "url": f"{self.connection.base_url}/dashboards/{dashboard_id}",
                "dashboard_title": getattr(error, 'dashboard_title', None) or f"Dashboard {dashboard_id}",
                "space_id": space_id,
                "space_name": getattr(error, 'space_name', None) or getattr(error, 'folder_name', None) or "Unknown",
                "model": model_name,
                "explore": explore_name,
                "message": getattr(error, 'message', None) or "Unknown error"
            }
            
            # Update count
            self.error_counts["dashboard"] += 1

    def _process_lookml_dashboard_errors(
        self, 
        lookml_dashboard_errors: List[Any],
        explore_set: Set[str]
    ):
        """Process LookML Dashboard validation errors.

        Args:
            lookml_dashboard_errors: List of LookML Dashboard error objects
            explore_set: Set of model.explore strings for filtering
        """
        for error in lookml_dashboard_errors:
            # Check if this error is related to our project
            model_name = getattr(error, 'model_name', None)
            explore_name = getattr(error, 'explore_name', None)
            
            if not model_name or not explore_name or f"{model_name}.{explore_name}" not in explore_set:
                continue
                
            # Record error
            dashboard_id = getattr(error, 'dashboard_id', 'unknown')
            element_id = getattr(error, 'id', 'unknown')
            error_id = f"lookml-dashboard-{dashboard_id}-{element_id}"
            
            self.errors[error_id] = {
                "type": "lookml_dashboard",
                "id": dashboard_id,
                "element_title": getattr(error, 'element_title', None) or "Unknown element",
                "url": f"{self.connection.base_url}/dashboards/lookml/{dashboard_id}",
                "dashboard_title": getattr(error, 'dashboard_title', None) or f"LookML Dashboard {dashboard_id}",
                "model": model_name,
                "explore": explore_name,
                "message": getattr(error, 'message', None) or "Unknown error"
            }
            
            # Update count
            self.error_counts["lookml_dashboard"] += 1

    def _process_scheduled_plan_errors(
        self, 
        scheduled_plan_errors: List[Any],
        explore_set: Set[str],
        include_folders: List[str],
        exclude_folders: List[str]
    ):
        """Process Scheduled Plan validation errors.

        Args:
            scheduled_plan_errors: List of Scheduled Plan error objects
            explore_set: Set of model.explore strings for filtering
            include_folders: List of folder IDs to include
            exclude_folders: List of folder IDs to exclude
        """
        for error in scheduled_plan_errors:
            # Check if this error is related to our project
            model_name = getattr(error, 'model_name', None) 
            explore_name = getattr(error, 'explore_name', None)
            
            if not model_name or not explore_name or f"{model_name}.{explore_name}" not in explore_set:
                continue
                
            # Record error
            plan_id = getattr(error, 'scheduled_plan_id', 'unknown')
            error_id = f"scheduled-plan-{plan_id}"
            
            self.errors[error_id] = {
                "type": "scheduled_plan",
                "id": plan_id,
                "title": getattr(error, 'title', None) or f"Scheduled Plan {plan_id}",
                "url": f"{self.connection.base_url}/admin/scheduled_plans/{plan_id}",
                "user_id": getattr(error, 'user_id', None),
                "model": model_name,
                "explore": explore_name,
                "message": getattr(error, 'message', None) or "Unknown error"
            }
            
            # Update count
            self.error_counts["scheduled_plan"] += 1

    def _run_target_validation(
        self, 
        explores: List[Dict[str, str]], 
        include_folders: List[str],
        exclude_folders: List[str]
    ):
        """Run content validation on target branch for incremental comparison.

        Args:
            explores: List of explore dictionaries
            include_folders: List of folder IDs to include
            exclude_folders: List of folder IDs to exclude
        """
        # Store current branch info
        temp_branch = self.branch
        temp_commit = self.commit_ref
        temp_reset = self.remote_reset
        
        try:
            # Set branch to target or production
            target = self.target or "production"
            logger.info(f"Running content validation on {target} for incremental comparison")
            
            # Set branch to target
            self.branch = target if target != "production" else None
            self.commit_ref = None
            self.remote_reset = False
            
            # Set up target branch
            self.setup_branch()
            
            # Run validation on target branch
            # Store errors in production_errors
            self._validate_content(explores, include_folders, exclude_folders)
            
            # Store production errors and reset
            self.production_errors = self.errors
            self.errors = {}
            
            # Reset error counts
            for key in self.error_counts:
                self.error_counts[key] = 0
            
            # Reset back to original branch
            self.branch = temp_branch
            self.commit_ref = temp_commit
            self.remote_reset = temp_reset
            self.setup_branch()
            
            # Run validation on original branch
            self._validate_content(explores, include_folders, exclude_folders)
            
        except Exception as e:
            logger.error(f"Failed to run incremental validation: {str(e)}")
            # Fall back to regular validation
            logger.warning("Falling back to regular validation")
            
            # Reset back to original branch
            self.branch = temp_branch
            self.commit_ref = temp_commit
            self.remote_reset = temp_reset
            self.setup_branch()
            
            # Clear any production errors
            self.production_errors = {}
            
            # Run validation on original branch
            self._validate_content(explores, include_folders, exclude_folders)

    def _filter_incremental_errors(self):
        """Filter out errors that also exist in the production branch."""
        if not self.production_errors:
            return
            
        # Create a set of errors found in production
        production_error_set = set()
        
        for error_id, error in self.production_errors.items():
            # Create a key based on the error content
            # This handles the case where error IDs change between branches
            error_key = f"{error['type']}-{error['model']}-{error['explore']}-{error['message']}"
            production_error_set.add(error_key)
        
        # Filter out errors that also exist in production
        filtered_errors = {}
        filtered_counts = {key: 0 for key in self.error_counts}
        
        for error_id, error in self.errors.items():
            error_key = f"{error['type']}-{error['model']}-{error['explore']}-{error['message']}"
            
            if error_key not in production_error_set:
                filtered_errors[error_id] = error
                filtered_counts[error['type']] += 1
        
        # Replace errors with filtered errors
        filter_count = len(self.errors) - len(filtered_errors)
        logger.info(f"Filtered out {filter_count} errors that also exist in {self.target or 'production'}")
        self.errors = filtered_errors
        self.error_counts = filtered_counts

    def _log_results(self):
        """Log validation results."""
        total_errors = len(self.errors)
        
        logger.info("\n" + "=" * 80)
        logger.info(f"Content Validation Results for {self.project}")
        logger.info("=" * 80)
        
        if self.incremental:
            logger.info(f"Incremental mode: comparing to {self.target or 'production'}")
            logger.info(f"Showing only errors unique to {self.branch}")
            
        if self.exclude_personal:
            logger.info("Excluding content in personal folders")
            
        if self.folders:
            logger.info(f"Folder selection: {', '.join(map(str, self.folders))}")
            
        logger.info(f"Total content errors: {total_errors}")
        
        if self.errors:
            # Show counts by type
            for error_type, count in self.error_counts.items():
                if count > 0:
                    logger.info(f"{error_type.replace('_', ' ').capitalize()} errors: {count}")
            
            logger.info("\nContent Errors:")
            
            # Sort errors by type and title
            sorted_errors = sorted(
                self.errors.values(),
                key=lambda x: (x["type"], x.get("title", x.get("dashboard_title", "")))
            )
            
            current_type = None
            
            for error in sorted_errors:
                error_type = error["type"]
                
                # Print header for each error type
                if error_type != current_type:
                    logger.info(f"\n{error_type.replace('_', ' ').capitalize()} Errors:")
                    current_type = error_type
                
                # Format error based on type
                if error_type == "look":
                    logger.info(f"  ❌ Look: {error['title']}")
                    logger.info(f"    URL: {error['url']}")
                    logger.info(f"    Model/Explore: {error['model']}/{error['explore']}")
                    logger.info(f"    Error: {error['message']}")
                    
                elif error_type == "dashboard" or error_type == "lookml_dashboard":
                    dashboard_type = "Dashboard" if error_type == "dashboard" else "LookML Dashboard"
                    logger.info(f"  ❌ {dashboard_type}: {error['dashboard_title']}")
                    logger.info(f"    URL: {error['url']}")
                    logger.info(f"    Element: {error['element_title']}")
                    logger.info(f"    Model/Explore: {error['model']}/{error['explore']}")
                    logger.info(f"    Error: {error['message']}")
                    
                elif error_type == "scheduled_plan":
                    logger.info(f"  ❌ Scheduled Plan: {error['title']}")
                    logger.info(f"    URL: {error['url']}")
                    logger.info(f"    Model/Explore: {error['model']}/{error['explore']}")
                    logger.info(f"    Error: {error['message']}")
                    
                else:
                    logger.info(f"  ❌ {error_type.capitalize()}: {error['title']}")
                    logger.info(f"    URL: {error['url']}")
                    logger.info(f"    Model/Explore: {error['model']}/{error['explore']}")
                    logger.info(f"    Error: {error['message']}")
        else:
            logger.info("\nNo content errors found! 🎉")
                
        logger.info("=" * 80)
        
        # Save errors to log file
        if self.errors:
            log_path = os.path.join(self.log_dir, f"content_errors_{self.project}.json")
            try:
                with open(log_path, "w") as f:
                    json.dump(self.errors, f, indent=2)
                logger.info(f"Error details saved to {log_path}")
            except Exception as e:
                logger.warning(f"Failed to save error log: {str(e)}")