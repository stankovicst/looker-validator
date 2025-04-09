# FILE: looker_validator/validators/content_validator.py
"""
Content validator: Uses Looker's content validation API.
Properly excludes personal folders by default.
"""

import logging
import time
from typing import List, Dict, Any, Optional, Tuple, Set

from looker_sdk.error import SDKError

from .base import BaseValidator
# Use specific exceptions
from ..exceptions import ContentValidationError, LookerApiError, LookerBranchError, ValidatorError

logger = logging.getLogger(__name__)


class ContentValidator(BaseValidator):
    """Validator for testing Looker content (Looks and Dashboards) using the content_validation API."""

    def __init__(self, connection, project, **kwargs):
        """Initialize Content validator."""
        super().__init__(connection, project, **kwargs)

        # Process folder selectors
        self.include_folders: List[str] = []
        self.exclude_folders: List[str] = []

        # Folders are passed as a list from Config class now
        for folder_id in kwargs.get("folders", []):
            folder_id = str(folder_id).strip()
            if not folder_id:
                continue
            if folder_id.startswith("-"):
                self.exclude_folders.append(folder_id[1:])
            else:
                self.include_folders.append(folder_id)

        # --- NO CHANGE NEEDED ---
        # This correctly gets the 'exclude_personal' value from the Config object.
        # The Config object gets its value based on the logic in cli.py (which uses --include-personal flag).
        # The default value from Config (True) is used if not otherwise specified.
        self.exclude_personal = kwargs.get("exclude_personal", True)
        logger.info(f"Personal folder exclusion is {'enabled' if self.exclude_personal else 'disabled'}")
        # --- END NO CHANGE ---

        self.incremental = kwargs.get("incremental", False)
        self.target_branch = kwargs.get("target") # Note: Renamed from self.target for clarity
        self.verbose = kwargs.get("verbose", False)

    def _execute_validation(self) -> List[Dict[str, Any]]:
        """Core content validation logic, called by BaseValidator.validate()."""
        all_errors: List[Dict[str, Any]] = []

        start_time = time.time()

        try:
            # Get and filter explores relevant to this project
            all_explores = self._get_all_explores()
            if not all_explores:
                logger.warning(f"No explores found for project '{self.project}', skipping content validation.")
                return []
            explores_to_validate = self._filter_explores(all_explores)
            if not explores_to_validate:
                logger.warning(f"No explores match the provided selectors for project '{self.project}', skipping content validation.")
                return []

            # Build explore set for easy filtering (model_name.explore_name)
            explore_set = {f"{e['model']}.{e['name']}" for e in explores_to_validate}
            logger.info(f"Content validation will target {len(explore_set)} explores.")

            # STEP 1: Get all folders and build folder relationships
            logger.info(f"Fetching folders for filtering...")
            # Request necessary fields for filtering and identification
            all_folders = self.sdk.all_folders(fields="id,name,is_personal,is_personal_descendant,parent_id")

            # Build parent-child relationships and folder ID/name map
            parent_to_children: Dict[str, List[str]] = {}
            folder_name_map: Dict[str, str] = {}
            personal_folders: Set[str] = set() # Stores IDs of personal folders and their descendants

            logger.info(f"Processing {len(all_folders)} folders")

            for folder in all_folders:
                folder_id = getattr(folder, 'id', None)
                if not folder_id:
                    continue

                folder_id = str(folder_id) # Ensure ID is string for consistency
                folder_name_map[folder_id] = getattr(folder, 'name', f'Unknown Folder ({folder_id})')

                # Build parent->child map
                if hasattr(folder, 'parent_id') and folder.parent_id:
                    parent_id = str(folder.parent_id)
                    if parent_id not in parent_to_children:
                        parent_to_children[parent_id] = []
                    parent_to_children[parent_id].append(folder_id)

                # STEP 2: Identify personal folders (only if exclusion is enabled)
                if self.exclude_personal:
                    is_personal_flag = getattr(folder, 'is_personal', False)
                    is_descendant_flag = getattr(folder, 'is_personal_descendant', False)

                    if is_personal_flag or is_descendant_flag:
                        personal_folders.add(folder_id)
                        if self.verbose:
                             logger.debug(f"Identified potential personal folder: {folder_name_map[folder_id]} (ID: {folder_id}, Personal: {is_personal_flag}, Descendant: {is_descendant_flag})")


            # STEP 3: Get all descendants of identified personal folders (if exclusion enabled)
            # This ensures subfolders within personal folders are also excluded.
            if self.exclude_personal and personal_folders:
                logger.info(f"Found {len(personal_folders)} top-level personal folders/descendants, expanding to include all sub-folders")

                # Use BFS (Breadth-First Search) to find all descendants
                all_personal_descendants = set(personal_folders) # Start with the initially identified ones
                queue = list(personal_folders)
                processed_for_descendants = set() # Avoid infinite loops with circular refs (unlikely but safe)

                while queue:
                    current_folder_id = queue.pop(0)
                    if current_folder_id in processed_for_descendants:
                        continue
                    processed_for_descendants.add(current_folder_id)

                    children = parent_to_children.get(current_folder_id, [])
                    for child_id in children:
                        if child_id not in all_personal_descendants:
                            all_personal_descendants.add(child_id)
                            queue.append(child_id) # Add child to queue to process its children

                if len(all_personal_descendants) > len(personal_folders):
                     logger.info(f"Expanded personal folder set to {len(all_personal_descendants)} to include sub-folders.")
                personal_folders = all_personal_descendants # Update the set to include all descendants

            # STEP 4: Process explicitly included/excluded folders and combine with personal exclusion
            excluded_folders = set(self.exclude_folders) # Start with explicitly excluded IDs
            if self.exclude_personal:
                excluded_folders.update(personal_folders) # Add all personal folder IDs if excluding

            # Expand explicitly included folders to include their descendants
            included_folders_set = set(self.include_folders)
            if included_folders_set:
                logger.info(f"Expanding {len(included_folders_set)} explicitly included folders to include descendants...")
                expanded_includes = set(included_folders_set)
                queue = list(included_folders_set)
                processed_for_includes = set()

                while queue:
                    current_folder_id = queue.pop(0)
                    if current_folder_id in processed_for_includes:
                        continue
                    processed_for_includes.add(current_folder_id)

                    children = parent_to_children.get(current_folder_id, [])
                    for child_id in children:
                        if child_id not in expanded_includes:
                            expanded_includes.add(child_id)
                            queue.append(child_id)

                if len(expanded_includes) > len(included_folders_set):
                    logger.info(f"Expanded included folder set to {len(expanded_includes)} to include sub-folders.")
                included_folders_set = expanded_includes

            # Log final filtering setup
            personal_count_msg = f"{len(personal_folders)} personal folders/descendants" if self.exclude_personal else "personal folders ignored"
            logger.info(f"Folder filtering: Including {len(included_folders_set) if included_folders_set else 'all'} folders, Excluding {len(excluded_folders)} folders ({personal_count_msg})")

            # STEP 5: Run content validation API call
            logger.info(f"Running content validation for project '{self.project}'...")
            try:
                 # Consider adding transport options for longer timeout if needed
                 validation_results = self.sdk.content_validation()
                 logger.info("Received content validation results.")
            except SDKError as e:
                 raise LookerApiError(f"Looker API error during content_validation: {e}") from e


            # STEP 6: Process errors from the API response
            content_with_errors = []
            if hasattr(validation_results, 'content_with_errors'):
                 content_with_errors = validation_results.content_with_errors or [] # Ensure it's a list
                 logger.info(f"Found {len(content_with_errors)} content items with errors in API response")
            else:
                 logger.warning("No 'content_with_errors' attribute found in validation results")
                 return [] # No errors to process

            # Tracking counters
            total_processed = 0
            personal_filtered_count = 0
            excluded_filtered_count = 0
            include_filtered_count = 0
            explore_filtered_count = 0
            errors_added_count = 0

            # STEP 7: Iterate through items with errors and apply filters
            for content_item in content_with_errors:
                 total_processed += 1

                 # Get content type (Look, Dashboard)
                 content_type = None
                 content_obj = None
                 # Looker API 4.0 uses 'look' and 'dashboard' attributes
                 if hasattr(content_item, 'look') and content_item.look:
                     content_type = "look"
                     content_obj = content_item.look
                 elif hasattr(content_item, 'dashboard') and content_item.dashboard:
                     content_type = "dashboard"
                     content_obj = content_item.dashboard
                 # Add checks for other types if necessary (e.g., lookml_dashboard)

                 if not content_obj or not content_type:
                     logger.debug(f"Could not identify content type/object for item {total_processed}")
                     continue

                 content_id = str(getattr(content_obj, 'id', 'unknown'))
                 content_title = getattr(content_obj, 'title', f"{content_type.capitalize()} {content_id}")

                 # Get folder ID and name
                 folder_id = None
                 folder_name = "Unknown Folder"
                 if hasattr(content_obj, 'folder') and content_obj.folder and hasattr(content_obj.folder, 'id'):
                     folder_id = str(content_obj.folder.id)
                     folder_name = folder_name_map.get(folder_id, f"Folder ID {folder_id}")

                 # --- APPLY FILTERS ---
                 # 1. Filter by excluded folders (includes personal if enabled)
                 if folder_id and folder_id in excluded_folders:
                     is_personal_match = self.exclude_personal and folder_id in personal_folders
                     filter_reason = "personal folder" if is_personal_match else "excluded folder"
                     if is_personal_match:
                         personal_filtered_count += 1
                     else:
                         excluded_filtered_count += 1
                     if self.verbose:
                          logger.debug(f"Skipping '{content_title}' (ID: {content_id}) - in {filter_reason} '{folder_name}' (ID: {folder_id})")
                     continue # Skip this item

                 # 2. Filter by included folders (if any are specified)
                 if included_folders_set and (not folder_id or folder_id not in included_folders_set):
                     include_filtered_count += 1
                     if self.verbose:
                          logger.debug(f"Skipping '{content_title}' (ID: {content_id}) - not in included folders")
                     continue # Skip this item

                 # 3. Process errors for this content item, filtering by explore
                 item_errors = getattr(content_item, 'errors', [])
                 if not item_errors: continue # Skip if no errors listed for this item

                 for error in item_errors:
                     model_name = getattr(error, 'model_name', None)
                     explore_name = getattr(error, 'explore_name', None)

                     # Filter errors not related to the target explores for this project
                     if not model_name or not explore_name or f"{model_name}.{explore_name}" not in explore_set:
                         explore_filtered_count += 1
                         if self.verbose:
                              logger.debug(f"Skipping error in '{content_title}' - explore '{model_name}.{explore_name}' not targeted")
                         continue # Skip this specific error

                     # If we reach here, the error is valid according to filters
                     try:
                         error_dict = {
                             "validator": self.__class__.__name__,
                             "type": content_type,
                             "severity": "error", # Content validation errors are always errors
                             "id": content_id,
                             "title": content_title,
                             "url": f"{self.connection.base_url}/{content_type}s/{content_id}",
                             "space_id": folder_id,
                             "space_name": folder_name,
                             "model": model_name,
                             "explore": explore_name,
                             "message": getattr(error, 'message', "Unknown content error"),
                             "field_name": getattr(error, 'field_name', None) # Field causing the error
                         }

                         # Add dashboard element details if applicable
                         if content_type == 'dashboard' and hasattr(content_item, 'dashboard_element') and content_item.dashboard_element:
                             element = content_item.dashboard_element
                             element_id = str(getattr(element, 'id', 'unknown'))
                             element_title = getattr(element, 'title', f"Element {element_id}")
                             error_dict["element_id"] = element_id
                             error_dict["element_title"] = element_title

                         # Remove None values for cleaner output
                         error_dict = {k: v for k, v in error_dict.items() if v is not None}

                         all_errors.append(error_dict)
                         errors_added_count += 1
                     except Exception as e:
                         logger.warning(f"Error creating error dictionary for '{content_title}': {e}", exc_info=True)


            # Log summary of filtering
            logger.info(f"Content validation summary: "
                       f"{total_processed} items processed from API, "
                       f"{personal_filtered_count} filtered by personal folders, "
                       f"{excluded_filtered_count} filtered by excluded folders, "
                       f"{include_filtered_count} filtered by include folders, "
                       f"{explore_filtered_count} errors filtered by explore, "
                       f"{errors_added_count} errors added to results")

            # Log completion status
            if not all_errors:
                logger.info("Content validation completed successfully.")
            else:
                # Use ERROR level for final summary if errors were found
                logger.error(f"Content validation completed with {len(all_errors)} errors.")

            return all_errors

        except LookerApiError as e:
             # Re-raise specific API errors
             raise e
        except Exception as e:
            # Wrap unexpected errors
            error_msg = f"Unexpected error during content validation: {e}"
            logger.exception(error_msg) # Log full traceback for unexpected errors
            raise ValidatorError(error_msg, original_exception=e) from e

    # _get_all_explores and _filter_explores are inherited from BaseValidator
