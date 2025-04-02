# FILE: looker_validator/validators/content_validator.py
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
            explores = self._get_all_explores() # Inherited from BaseValidator

            if not explores:
                logger.warning(f"No explores found for project {self.project}")
                return True

            # Filter explores based on selectors
            # <<< --- This method is now inherited from BaseValidator --- >>>
            explores = self._filter_explores(explores) # Now uses the method from BaseValidator

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

    # <<< --- REMOVE THIS METHOD --- >>>
    # def _filter_explores(self, explores: List[Dict[str, str]]) -> List[Dict[str, str]]:
    #     """Filter explores based on selectors.
    #
    #     Args:
    #         explores: List of explore dictionaries
    #
    #     Returns:
    #         Filtered list of explore dictionaries
    #     """
    #     if not self.explore_selectors:
    #         return explores
    #
    #     includes, excludes = self.resolve_explores()
    #
    #     filtered_explores = [
    #         explore for explore in explores
    #         if self.matches_selector(
    #             explore["model"],
    #             explore["name"],
    #             includes,
    #             excludes
    #         )
    #     ]
    #
    #     return filtered_explores
    # <<< --- END OF REMOVED METHOD --- >>>

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
                # Check multiple possible attributes for personal folders across API versions
                is_personal = False
                if hasattr(folder, 'is_personal') and folder.is_personal:
                    is_personal = True
                elif hasattr(folder, 'is_personal_descendant') and folder.is_personal_descendant:
                     is_personal = True
                elif hasattr(folder, 'personal_folder') and folder.personal_folder:
                     is_personal = True
                # API 3.1 might represent personal folders differently, e.g., parent_id = 'user'
                elif hasattr(folder, 'parent_id') and folder.parent_id == 'user':
                     is_personal = True
                # Check if the folder name itself indicates a personal folder (less reliable)
                elif hasattr(folder, 'name') and folder.name and folder.name.lower() == 'personal':
                     is_personal = True

                if is_personal and hasattr(folder, 'id') and folder.id:
                   personal_folders.append(str(folder.id)) # Ensure ID is string

            logger.debug(f"Found {len(personal_folders)} personal folders to exclude")

        except Exception as e:
            logger.warning(f"Failed to get personal folders: {str(e)}")

        # Ensure uniqueness
        return list(set(personal_folders))


    def _expand_subfolders(self, folder_ids: List[str]) -> List[str]:
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
        expanded_folders = list(folder_ids)  # Start with the original folders

        try:
            # Get all folders
            all_folders = self.sdk.all_folders()

            # Create a mapping of parent folders to their children
            parent_to_children = {}
            for folder in all_folders:
                if hasattr(folder, 'parent_id') and folder.parent_id and hasattr(folder, 'id') and folder.id:
                   parent_id = str(folder.parent_id) # Ensure parent ID is string
                   child_id = str(folder.id) # Ensure child ID is string
                   if parent_id not in parent_to_children:
                       parent_to_children[parent_id] = []
                   parent_to_children[parent_id].append(child_id)

            # Recursively add subfolders
            to_process = list(folder_ids)
            processed_set = set(folder_ids) # Keep track of processed folders to avoid loops

            while to_process:
                parent_id = to_process.pop(0)
                children = parent_to_children.get(parent_id, [])
                for child_id in children:
                    if child_id not in processed_set:
                        expanded_folders.append(child_id)
                        to_process.append(child_id)
                        processed_set.add(child_id) # Mark as processed

            logger.debug(f"Expanded {len(folder_ids)} folders to {len(expanded_folders)} unique folders with subfolders")

        except Exception as e:
            logger.warning(f"Failed to expand subfolders: {str(e)}")

        return list(set(expanded_folders)) # Return unique list


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
        # Ensure include/exclude folders are sets for efficient lookup
        include_folders_set = set(include_folders)
        exclude_folders_set = set(exclude_folders)

        # Try API 4.0 attribute 'content_with_errors'
        if hasattr(validation, 'content_with_errors') and validation.content_with_errors:
            logger.debug("Processing errors using API 4.0 'content_with_errors' attribute.")
            self._process_api40_content_errors(
                validation.content_with_errors,
                explore_set,
                include_folders_set,
                exclude_folders_set
            )
        # Try legacy attributes
        else:
             logger.debug("Processing errors using legacy attributes (look_errors, dashboard_errors, etc.).")
             error_types_map = {
                 'looks': ('look_errors', self._add_look_error),
                 'dashboards': ('dashboard_errors', self._add_dashboard_error),
                 'lookml_dashboards': ('dashboard_errors', self._add_lookml_dashboard_error), # Reuse dashboard error handling logic?
                 'scheduled_plans': ('scheduled_plan_errors', self._add_scheduled_plan_error)
             }

             processed_error = False
             for content_key, (attr_name, process_func) in error_types_map.items():
                if hasattr(validation, attr_name) and getattr(validation, attr_name):
                    processed_error = True
                    items_with_errors = getattr(validation, attr_name)
                    logger.debug(f"Found {len(items_with_errors)} items in '{attr_name}'")

                    for item in items_with_errors:
                         # Extract necessary info based on legacy structure
                         folder_id = str(getattr(item, 'space_id', None) or getattr(item, 'folder_id', None))

                         # Folder filtering
                         if not self._should_include_folder(folder_id, include_folders_set, exclude_folders_set):
                             continue

                         # Explore filtering - needs model/explore names from the error item
                         model_name = getattr(item, 'model_name', None)
                         explore_name = getattr(item, 'explore_name', None)

                         if not model_name or not explore_name or f"{model_name}.{explore_name}" not in explore_set:
                            continue

                         # --- Adapt the processing function calls ---
                         # Legacy structure might group errors differently,
                         # need to pass the item itself and maybe the error message directly.
                         # Assuming the item contains the error details.

                         if content_key == 'looks':
                             self._add_look_error(item, item) # Pass item as both 'look' and 'error' source
                         elif content_key == 'dashboards':
                             # Need to differentiate between UDD and LookML dashboards if possible
                             # Assume UDD for now
                             # Element might not be directly available in legacy structure
                             self._add_dashboard_error(item, item) # Pass item as both 'dashboard' and 'error' source
                         elif content_key == 'lookml_dashboards':
                             # This might require specific handling if structure differs significantly
                             self._add_lookml_dashboard_error(item, item)
                         elif content_key == 'scheduled_plans':
                             self._add_scheduled_plan_error(item, item) # Pass item as 'plan' and 'error' source

             if not processed_error:
                 logger.warning("Content validation response structure not recognized or no errors found.")


    def _process_api40_content_errors(
        self,
        content_items: List[Any],
        explore_set: Set[str],
        include_folders: Set[str],
        exclude_folders: Set[str]
    ):
        """Process API 4.0 content_with_errors format.

        Args:
            content_items: List of content items with errors
            explore_set: Set of model.explore strings for filtering
            include_folders: Set of folder IDs to include
            exclude_folders: Set of folder IDs to exclude
        """
        for content in content_items:
            # Determine content type and get the content object
            content_type = None
            content_obj = None
            dashboard_element = None # Specific for dashboard errors

            possible_types = ['look', 'dashboard', 'lookml_dashboard', 'scheduled_plan']
            for type_name in possible_types:
                if hasattr(content, type_name) and getattr(content, type_name):
                    content_type = type_name
                    content_obj = getattr(content, type_name)
                    # Check for dashboard element info if it's a dashboard error
                    if content_type in ['dashboard', 'lookml_dashboard'] and hasattr(content, 'dashboard_element'):
                        dashboard_element = content.dashboard_element
                    break

            if not content_type or not content_obj:
                logger.debug(f"Could not determine content type or object for error item: {content}")
                continue

            # Folder Filtering
            # Get folder ID (handle different attribute names like space_id/folder_id)
            folder_id = None
            if hasattr(content_obj, 'folder'):
                 folder_id = str(getattr(content_obj.folder, 'id', None)) if content_obj.folder else None
            elif hasattr(content_obj, 'space_id'):
                 folder_id = str(getattr(content_obj, 'space_id', None))
            elif hasattr(content_obj, 'folder_id'):
                 folder_id = str(getattr(content_obj, 'folder_id', None))

            if not self._should_include_folder(folder_id, include_folders, exclude_folders):
                #logger.debug(f"Skipping content '{getattr(content_obj, 'title', content_type)}' due to folder filter (Folder ID: {folder_id}).")
                continue

            # Process errors associated with this content item
            errors = getattr(content, 'errors', [])
            if not errors:
                 logger.debug(f"Content item '{getattr(content_obj, 'title', content_type)}' listed but has no associated errors.")
                 continue

            for error in errors:
                model_name = getattr(error, 'model_name', None)
                explore_name = getattr(error, 'explore_name', None)

                # Explore Filtering
                if not model_name or not explore_name:
                    logger.debug(f"Skipping error for '{getattr(content_obj, 'title', content_type)}' due to missing model/explore name in error details.")
                    continue

                if f"{model_name}.{explore_name}" not in explore_set:
                     #logger.debug(f"Skipping error for '{getattr(content_obj, 'title', content_type)}' because explore '{model_name}.{explore_name}' is not in the validation set.")
                    continue

                # Add error based on content type
                if content_type == 'look':
                    self._add_look_error(content_obj, error)
                elif content_type == 'dashboard':
                    self._add_dashboard_error(content_obj, error, dashboard_element)
                elif content_type == 'lookml_dashboard':
                     self._add_lookml_dashboard_error(content_obj, error, dashboard_element)
                elif content_type == 'scheduled_plan':
                    self._add_scheduled_plan_error(content_obj, error)
                else:
                    # Should not happen based on loop above, but as fallback
                    self._add_generic_content_error(content_type, content_obj, error)


    def _should_include_folder(
        self,
        folder_id: Optional[str],
        include_folders: Set[str],
        exclude_folders: Set[str]
    ) -> bool:
        """Determine if content in a folder should be included based on filters.

        Args:
            folder_id: Folder ID (as string)
            include_folders: Set of folder IDs to include
            exclude_folders: Set of folder IDs to exclude

        Returns:
            True if the folder should be included
        """
        # Content without a folder (e.g., LookML dashboards) might be handled differently
        # For now, assume content *must* be in a folder to be filterable this way.
        # LookML dashboards might need separate filtering logic if not tied to folders.
        if folder_id is None:
             #logger.debug("Content item has no associated folder_id, including by default.")
             # Let's assume items without folder_id (like LookML dashboards?) should be included unless specifically excluded?
             # Or maybe they should always be excluded if folder filters are active?
             # For now, let's include them if no includes are specified, otherwise exclude.
             return not include_folders


        # Exclusions take precedence
        if folder_id in exclude_folders:
            #logger.debug(f"Folder {folder_id} is explicitly excluded.")
            return False

        # If includes are specified, only include matching folders
        if include_folders and folder_id not in include_folders:
            #logger.debug(f"Folder {folder_id} is not in the include list.")
            return False

        #logger.debug(f"Folder {folder_id} is included.")
        return True


    def _add_look_error(self, look: Any, error: Any):
        """Add a Look error to the results.

        Args:
            look: Look object
            error: Error object associated with the look
        """
        look_id = str(getattr(look, 'id', 'unknown')) # Ensure string ID
        # Use a more specific error ID if multiple errors can occur for the same look
        error_key = f"look-{look_id}-{getattr(error, 'message', 'unknown_error')[:50]}" # Example key

        # Avoid adding duplicate errors for the same look/message
        if error_key in self.errors:
             return

        self.errors[error_key] = {
            "type": "look",
            "id": look_id,
            "title": getattr(look, 'title', None) or f"Look {look_id}",
            "url": f"{self.connection.base_url}/looks/{look_id}",
            "space_id": str(getattr(look, 'space_id', None) or getattr(look, 'folder_id', None)),
            "space_name": getattr(look.folder, 'name', None) if hasattr(look, 'folder') and look.folder else \
                          getattr(look, 'space_name', None) or getattr(look, 'folder_name', "Unknown Space"), # Try to get folder name
            "model": getattr(error, 'model_name', None),
            "explore": getattr(error, 'explore_name', None),
            "message": getattr(error, 'message', None) or "Unknown error"
        }

        self.error_counts["look"] += 1

    def _add_dashboard_error(self, dashboard: Any, error: Any, element: Any = None):
        """Add a User-Defined Dashboard (UDD) error to the results.

        Args:
            dashboard: Dashboard object
            error: Error object associated with the dashboard/element
            element: Dashboard element object (tile or filter) if applicable
        """
        dashboard_id = str(getattr(dashboard, 'id', 'unknown')) # Ensure string ID
        element_id = str(getattr(element, 'id', 'unknown')) if element else 'filter_or_global' # Indicate if error is element-specific or global
        # Make error key specific
        error_key = f"dashboard-{dashboard_id}-{element_id}-{getattr(error, 'message', 'unknown_error')[:50]}"

        if error_key in self.errors:
            return

        element_title = "Dashboard Level Error" # Default title if no specific element
        if element:
            element_title = getattr(element, 'title', None) or f"Element ID {element_id}"


        self.errors[error_key] = {
            "type": "dashboard",
            "id": dashboard_id,
            "element_id": element_id if element else None,
            "element_title": element_title,
            "url": f"{self.connection.base_url}/dashboards/{dashboard_id}",
            "dashboard_title": getattr(dashboard, 'title', None) or f"Dashboard {dashboard_id}",
             "space_id": str(getattr(dashboard, 'space_id', None) or getattr(dashboard, 'folder_id', None)),
             "space_name": getattr(dashboard.folder, 'name', None) if hasattr(dashboard, 'folder') and dashboard.folder else \
                            getattr(dashboard, 'space_name', None) or getattr(dashboard, 'folder_name', "Unknown Space"),
            "model": getattr(error, 'model_name', None),
            "explore": getattr(error, 'explore_name', None),
            "message": getattr(error, 'message', None) or "Unknown error"
        }

        self.error_counts["dashboard"] += 1

    def _add_lookml_dashboard_error(self, dashboard: Any, error: Any, element: Any = None):
        """Add a LookML dashboard error to the results.

        Args:
            dashboard: LookML dashboard object
            error: Error object associated with the dashboard/element
            element: Dashboard element object (tile or filter) if applicable
        """
         # LookML Dashboards use a slug/id like 'model::dashboard_name'
        dashboard_id = str(getattr(dashboard, 'id', None) or getattr(dashboard, 'slug', 'unknown'))
        element_id = str(getattr(element, 'id', 'unknown')) if element else 'filter_or_global'
         # Make error key specific
        error_key = f"lookml-dashboard-{dashboard_id}-{element_id}-{getattr(error, 'message', 'unknown_error')[:50]}"

        if error_key in self.errors:
            return

        element_title = "Dashboard Level Error"
        if element:
             element_title = getattr(element, 'title', None) or f"Element ID {element_id}"

        # Construct URL for LookML dashboard
        # Assumes dashboard_id is in the format 'project::dashboard_file_name'
        url_part = dashboard_id.replace('::', '/') if '::' in dashboard_id else dashboard_id
        url = f"{self.connection.base_url}/dashboards-next/{url_part}" # Use dashboards-next for LookML dashboards


        self.errors[error_key] = {
            "type": "lookml_dashboard",
            "id": dashboard_id,
            "element_id": element_id if element else None,
            "element_title": element_title,
            "url": url,
            "dashboard_title": getattr(dashboard, 'title', None) or f"LookML Dashboard {dashboard_id}",
            # LookML dashboards don't live in spaces/folders
            "space_id": None,
            "space_name": "LookML",
            "model": getattr(error, 'model_name', None),
            "explore": getattr(error, 'explore_name', None),
            "message": getattr(error, 'message', None) or "Unknown error"
        }

        self.error_counts["lookml_dashboard"] += 1

    def _add_scheduled_plan_error(self, plan: Any, error: Any):
        """Add a scheduled plan error to the results.

        Args:
            plan: Scheduled plan object
            error: Error object associated with the plan
        """
        plan_id = str(getattr(plan, 'id', 'unknown')) # Ensure string ID
         # Make error key specific
        error_key = f"scheduled-plan-{plan_id}-{getattr(error, 'message', 'unknown_error')[:50]}"

        if error_key in self.errors:
             return

        self.errors[error_key] = {
            "type": "scheduled_plan",
            "id": plan_id,
            "title": getattr(plan, 'name', None) or f"Scheduled Plan {plan_id}", # Use 'name' for schedules
            "url": f"{self.connection.base_url}/admin/scheduled_plans/{plan_id}",
            "user_id": str(getattr(plan, 'user_id', None)),
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
        content_id = str(getattr(content_obj, 'id', 'unknown')) # Ensure string ID
        error_key = f"{content_type}-{content_id}-{getattr(error, 'message', 'unknown_error')[:50]}"

        if error_key in self.errors:
            return

        # Try to construct a reasonable URL
        url_path_part = content_type.replace('_', '-') + 's' # e.g., scheduled_plan -> scheduled-plans
        url = f"{self.connection.base_url}/{url_path_part}/{content_id}" # Generic guess

        self.errors[error_key] = {
            "type": content_type,
            "id": content_id,
            "title": getattr(content_obj, 'title', None) or f"{content_type.replace('_', ' ').capitalize()} {content_id}",
            "url": url,
            "model": getattr(error, 'model_name', None),
            "explore": getattr(error, 'explore_name', None),
            "message": getattr(error, 'message', None) or "Unknown error"
        }

        self.error_counts["other"] += 1


    def _run_target_validation(
        self,
        explores: List[Dict[str, str]],
        include_folders: List[str], # Keep as list for setup_branch compatibility
        exclude_folders: List[str]  # Keep as list for setup_branch compatibility
    ):
        """Run content validation on target branch for incremental comparison.

        Args:
            explores: List of explore dictionaries
            include_folders: List of folder IDs to include
            exclude_folders: List of folder IDs to exclude
        """
        # Store current branch state
        original_branch = self.branch
        original_commit_ref = self.commit_ref
        original_remote_reset = self.remote_reset
        current_errors = self.errors # Store errors found on the current branch so far
        current_error_counts = self.error_counts.copy()

        target_branch_name = self.target # Target branch name or None for production
        target_display_name = target_branch_name or "production"

        try:
            # Switch to target branch/production
            logger.info(f"Switching to {target_display_name} to gather baseline errors...")
            self.branch = target_branch_name
            self.commit_ref = None # Assume comparing against branch head, not specific commit
            self.remote_reset = False # Don't reset target branch

            # This setup_branch call might fail if target doesn't exist
            self.setup_branch()

            # Reset errors before running target validation
            self.errors = {}
            self.error_counts = {key: 0 for key in self.error_counts}

            # Run validation on target branch
            logger.info(f"Running content validation on {target_display_name}...")
            self._validate_content(explores, include_folders, exclude_folders)

            # Store the errors found on the target branch
            self.production_errors = self.errors
            logger.info(f"Found {len(self.production_errors)} errors on {target_display_name}.")

        except Exception as e:
            logger.error(f"Failed to run validation on target '{target_display_name}': {str(e)}")
            logger.warning("Skipping incremental comparison. Results will show all errors found on the original branch.")
            self.production_errors = {} # Ensure no comparison happens

        finally:
            # Switch back to the original branch configuration
            logger.info(f"Switching back to original branch: {original_branch or 'production'}")
            self.branch = original_branch
            self.commit_ref = original_commit_ref
            self.remote_reset = original_remote_reset

            try:
                self.setup_branch()
                # Restore the errors found on the original branch
                self.errors = current_errors
                self.error_counts = current_error_counts
            except Exception as e:
                 # If switching back fails, we might be in a bad state. Log and maybe raise.
                 logger.critical(f"FATAL: Failed to switch back to original branch '{original_branch}': {str(e)}")
                 # Depending on desired behavior, might want to raise an error here
                 # For now, just log, the comparison might be skipped or incorrect.
                 self.production_errors = {} # Prevent comparison if we can't get back


    def _filter_incremental_errors(self):
        """Filter out errors that also exist in the target (production/baseline) branch."""
        if not self.production_errors or not self.incremental:
            logger.debug("Skipping incremental filtering (not enabled or no baseline errors).")
            return

        logger.info("Performing incremental comparison...")
        # Create a set of baseline error keys for efficient lookup
        # Use a tuple of key fields to define uniqueness
        baseline_error_keys = set()
        for error in self.production_errors.values():
            key = (
                error.get("type"),
                error.get("id"), # Content ID (look, dashboard, plan)
                error.get("element_id"), # Dashboard element ID if applicable
                error.get("model"),
                error.get("explore"),
                # Consider if message should be part of the key. Minor variations might exist.
                # Using the core fields might be more robust.
                error.get("message", "").strip() # Include message for now
            )
            baseline_error_keys.add(key)

        # Filter errors found on the current branch
        unique_errors = {}
        unique_error_counts = {key: 0 for key in self.error_counts}
        filtered_count = 0

        for error_key, error in self.errors.items():
            current_key = (
                error.get("type"),
                error.get("id"),
                error.get("element_id"),
                error.get("model"),
                error.get("explore"),
                error.get("message", "").strip()
            )

            if current_key not in baseline_error_keys:
                unique_errors[error_key] = error
                # Increment count for the correct type
                error_type_key = error.get("type", "other")
                if error_type_key not in unique_error_counts: # Handle potential new error types
                   unique_error_counts[error_type_key] = 0
                unique_error_counts[error_type_key] += 1
            else:
                filtered_count += 1

        logger.info(f"Filtered out {filtered_count} errors that also exist in {self.target or 'production'}.")
        self.errors = unique_errors
        self.error_counts = unique_error_counts


    def _log_results(self):
        """Log validation results."""
        total_errors = len(self.errors)

        logger.info("\n" + "=" * 80)
        logger.info(f"Content Validation Results for Project: {self.project}")
        if self.branch:
            logger.info(f"Branch: {self.branch}")
        else:
            logger.info("Branch: production")
        logger.info("=" * 80)

        if self.incremental and self.branch:
            target_display = self.target or "production"
            logger.info(f"Mode: Incremental (comparing to '{target_display}')")
            logger.info(f"Errors shown are unique to branch '{self.branch}'.")
        else:
            logger.info("Mode: Full Validation")

        if self.exclude_personal:
            logger.info("Filter: Excluding content in personal folders")
        if self.folders:
            include_selectors = [f for f in self.folders if not f.startswith('-')]
            exclude_selectors = [f[1:] for f in self.folders if f.startswith('-')]
            if include_selectors:
                logger.info(f"Filter: Including folders = {', '.join(include_selectors)}")
            if exclude_selectors:
                logger.info(f"Filter: Excluding folders = {', '.join(exclude_selectors)}")


        logger.info(f"\nTotal Unique Content Errors Found: {total_errors}")

        if total_errors > 0:
            # Show counts by type
            logger.info("\nError Summary by Type:")
            for error_type, count in self.error_counts.items():
                if count > 0:
                    type_name = error_type.replace('_', ' ').capitalize()
                    logger.info(f"  - {type_name}: {count}")

            logger.info("\nDetailed Errors:")

            # Sort errors by type, then title/ID for consistent output
            sorted_errors = sorted(
                self.errors.values(),
                key=lambda x: (
                    x.get("type", "z"), # Sort type alphabetically
                    x.get("dashboard_title", x.get("title", x.get("id", ""))) # Sort by title or ID
                )
            )

            current_type = None
            for error in sorted_errors:
                error_type_display = error.get("type", "other").replace('_', ' ').capitalize()

                # Print type header
                if error_type_display != current_type:
                    logger.info(f"\n--- {error_type_display} Errors ---")
                    current_type = error_type_display

                # Format error details
                title = error.get("dashboard_title", error.get("title", f"ID: {error.get('id', 'N/A')}"))
                space = error.get("space_name", "N/A") if error.get("space_id") else "LookML" # Indicate LookML Dashboards
                url = error.get("url", "#")
                model_explore = f"{error.get('model', 'N/A')}/{error.get('explore', 'N/A')}"
                message = error.get("message", "No message")

                log_prefix = f"  ❌ {title}"
                if space != "LookML": # Don't show space for LookML dashboards
                    log_prefix += f" (Space: {space})"
                logger.info(log_prefix)
                logger.info(f"     URL: {url}")
                if error.get("type") in ["dashboard", "lookml_dashboard"] and error.get("element_title"):
                    logger.info(f"     Element: {error.get('element_title')} (ID: {error.get('element_id', 'N/A')})")
                logger.info(f"     Model/Explore: {model_explore}")
                logger.info(f"     Error: {message}")

        else:
            logger.info("\nNo content errors found! 🎉")

        logger.info("=" * 80)

        # Save errors to log file
        if self.errors:
            log_filename = f"content_errors_{self.project}"
            if self.branch:
                 # Include branch name in log file if not production
                 safe_branch_name = "".join(c if c.isalnum() else "_" for c in self.branch)
                 log_filename += f"__{safe_branch_name}"
            log_filename += ".json"
            log_path = os.path.join(self.log_dir, log_filename)

            try:
                os.makedirs(self.log_dir, exist_ok=True) # Ensure log dir exists
                with open(log_path, "w") as f:
                    json.dump(self.errors, f, indent=2, sort_keys=True)
                logger.info(f"Detailed error report saved to: {log_path}")
            except Exception as e:
                logger.warning(f"Failed to save error log to {log_path}: {str(e)}")
