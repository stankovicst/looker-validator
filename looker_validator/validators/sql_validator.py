"""
SQL Validator: Tests explores by running simple queries against them.
Based on user's original code, updated for new BaseValidator and error handling.
Fixed ImportError for LookerValidationError.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional

from looker_sdk.sdk.api40 import models as models40 # Use API 4.0 models
from looker_sdk.error import SDKError

# FIXED: Add the correct import for BaseValidator
from .base import BaseValidator
# Use central exceptions
from ..exceptions import LookerApiError, SQLValidationError, ValidatorError

logger = logging.getLogger(__name__)


class SQLValidator(BaseValidator):
    """
    Validator for testing explores by running simple, limited queries via Looker API.
    Checks if Looker can successfully generate SQL for explores.
    """

    def __init__(self, connection, project, **kwargs):
        """Initialize SQL validator."""
        super().__init__(connection, project, **kwargs)
        try:
            concurrency_default = 10
            self.concurrency = max(1, int(kwargs.get("concurrency", concurrency_default)))
        except (ValueError, TypeError):
            logger.warning(f"Invalid concurrency value, defaulting to {concurrency_default}.")
            self.concurrency = concurrency_default

    def _execute_validation(self) -> List[Dict[str, Any]]:
        """Core SQL validation logic, called by BaseValidator.validate()."""
        all_errors: List[Dict[str, Any]] = []
        start_time = time.time()

        # Get and filter explores *within* the execution phase, after branch setup
        try:
            all_explores = self._get_all_explores()
            if not all_explores:
                logger.warning(f"No explores found for project '{self.project}', skipping SQL validation.")
                return []

            explores_to_test = self._filter_explores(all_explores)
            if not explores_to_test:
                logger.warning(f"No explores match the provided selectors for project '{self.project}', skipping SQL validation.")
                return []

        except (LookerApiError, ValidatorError) as e:
             logger.error(f"Failed to get or filter explores for SQL validation: {e}", exc_info=True)
             return [{
                 "validator": self.__class__.__name__, "type": "Setup Error", "severity": "error",
                 "message": f"Could not retrieve or filter explores: {e}"
             }]

        logger.info(f"Starting SQL validation for {len(explores_to_test)} explores (Concurrency: {self.concurrency})...")

        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            future_to_explore = {
                executor.submit(self._test_explore, explore): explore
                for explore in explores_to_test
            }
            test_count = len(future_to_explore)
            logger.info(f"Submitted {test_count} SQL test tasks to thread pool.")

            for future in as_completed(future_to_explore):
                explore = future_to_explore[future]
                explore_key = f"{explore['model']}/{explore['name']}"
                try:
                    result_error = future.result()
                    if result_error:
                        all_errors.append(result_error)
                except Exception as exc:
                    logger.error(f"Unexpected internal error processing result for explore {explore_key}: {exc}", exc_info=True)
                    all_errors.append({
                        "validator": self.__class__.__name__, "type": "Internal Validator Error", "severity": "error",
                        "model": explore.get('model'), "explore": explore.get('name'),
                        "message": f"An internal error occurred processing SQL test result: {exc}",
                    })

        return all_errors
    
    def _test_explore(self, explore: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """
        Runs a simple test query against a single explore.

        Args:
            explore: Dictionary containing 'model' and 'name' of the explore.

        Returns:
            A dictionary containing error details if validation fails, otherwise None.
        """
        model_name = explore['model']
        explore_name = explore['name']
        explore_key = f"{model_name}/{explore_name}"
        logger.debug(f"Testing explore: {explore_key}")

        if self._raw_options.get("dry_run", False):
            logger.info(f"Dry run mode: Skipping actual query execution for {explore['model']}/{explore['name']}")
            return None

        try:
            # 1. Find a suitable dimension to query
            logger.debug(f"Fetching fields for explore {explore_key}...")
            # Use specific fields to minimize payload
            explore_details = self.sdk.lookml_model_explore(
                lookml_model_name=model_name,
                explore_name=explore_name,
                fields="fields(dimensions(name,type,hidden))"
            )

            field_to_query: Optional[str] = None
            if explore_details.fields and explore_details.fields.dimensions:
                visible_dimensions = [d for d in explore_details.fields.dimensions if not d.hidden]
                if visible_dimensions:
                    id_fields = [d.name for d in visible_dimensions if d.name and 'id' in d.name.lower()]
                    field_to_query = id_fields[0] if id_fields else visible_dimensions[0].name
                else:
                    logger.warning(f"Explore {explore_key} has no visible dimensions.")
                    return {
                        "validator": self.__class__.__name__, "type": "Explore Configuration", "severity": "warning",
                        "model": model_name, "explore": explore_name,
                        "message": "Explore has no visible dimensions to use for SQL test query.",
                    }
            if not field_to_query:
                logger.warning(f"Could not find a suitable dimension for explore {explore_key}.")
                return {
                    "validator": self.__class__.__name__, "type": "Explore Configuration", "severity": "warning",
                    "model": model_name, "explore": explore_name,
                    "message": "No dimensions found in explore.",
                }

            logger.debug(f"Using field '{field_to_query}' for explore {explore_key}")

            # 2. Create the query body
            query_body = models40.WriteQuery(
                model=model_name, 
                view=explore_name, 
                fields=[field_to_query], 
                limit="1"
            )

            # 3. Run the query
            logger.debug(f"Running inline query for explore {explore_key}...")
            query_result = self.sdk.run_inline_query(result_format="json_detail", body=query_body)

            # Check for errors in the response
            if isinstance(query_result, dict) and query_result.get('errors'):
                first_error = query_result['errors'][0]
                error_message = first_error.get('message_details', first_error.get('message', 'Unknown query error'))
                logger.error(f"Query error testing explore {explore_key}: {error_message}")
                return {
                    "validator": self.__class__.__name__, "type": "Query Error", "severity": "error",
                    "model": model_name, "explore": explore_name, "field": field_to_query,
                    "message": error_message,
                }

            logger.debug(f"Explore {explore_key} SQL generated successfully.")
            return None # Success

        except SDKError as e:
            error_message = f"API error testing explore {explore_key}: {e}"
            logger.error(error_message, exc_info=True)
            clean_error = getattr(e, 'message', str(e))
            if hasattr(e, 'errors') and e.errors and isinstance(e.errors, list) and len(e.errors) > 0:
                first_error = e.errors[0]
                if isinstance(first_error, dict) and 'message' in first_error: clean_error = first_error['message']
            return {
                "validator": self.__class__.__name__,
                "type": "API Error" if "fetch" in error_message.lower() else "SQL Generation Error",
                "severity": "error", "model": model_name, "explore": explore_name,
                "message": clean_error, "status_code": e.status if hasattr(e, 'status') else None,
            }
        except Exception as e:
            error_message = f"Unexpected internal error testing explore {explore_key}: {e}"
            logger.error(error_message, exc_info=True)
            return {
                "validator": self.__class__.__name__, "type": "Internal Validator Error", "severity": "error",
                "model": model_name, "explore": explore_name,
                "message": error_message,
            }
        
    def _test_sql_generation(self, model_name, explore_name, field):
        query = models40.WriteQuery(model=model_name, view=explore_name, fields=[field])
        sql = self.sdk.create_sql_query(body=query)
        # Check if SQL was generated successfully (no errors)
        return sql.sql is not None