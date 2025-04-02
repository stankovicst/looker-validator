# FILE: looker_validator/validators/sql_validator.py
"""
SQL Validator: Tests explores by running simple queries against them.
"""

import logging
import os
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional

from looker_sdk.sdk.api40.models import WriteQuery # Use API 4.0 models
from looker_sdk.error import SDKError

from looker_validator.validators.base import BaseValidator, ValidatorError
# <<< --- CORRECT THIS LINE --- >>>
from looker_validator.exceptions import SQLValidationError # Corrected capitalization

logger = logging.getLogger(__name__)


class SQLValidator(BaseValidator):
    """Validator for testing explores via SQL queries."""

    def __init__(self, connection, project, **kwargs):
        """Initialize SQL validator.

        Args:
            connection: LookerConnection instance
            project: Looker project name
            **kwargs: Additional validator options
        """
        super().__init__(connection, project, **kwargs)
        self.concurrency = kwargs.get("concurrency", 10)
        self.errors = {} # Initialize dictionary to store errors

    def validate(self) -> bool:
        """Run SQL validation on explores.

        Returns:
            True if all tested explores run successfully, False otherwise
        """
        start_time = time.time()

        try:
            # Set up branch for validation
            self.setup_branch()

            # Get all models and explores in the project
            logger.info(f"Finding explores for project {self.project}")
            all_explores = self._get_all_explores() # Inherited from BaseValidator

            if not all_explores:
                logger.warning(f"No explores found for project {self.project}")
                return True

            # Filter explores based on selectors
            explores_to_test = self._filter_explores(all_explores) # Inherited from BaseValidator

            if not explores_to_test:
                logger.warning(f"No explores match the provided selectors for project {self.project}")
                return True

            logger.info(f"Testing {len(explores_to_test)} explores [concurrency = {self.concurrency}]")

            # Use ThreadPoolExecutor for concurrency
            with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
                # Map _test_explore function to each explore
                futures = {executor.submit(self._test_explore, explore): explore for explore in explores_to_test}

                # Process results as they complete
                for future in as_completed(futures):
                    explore = futures[future]
                    try:
                        # Check if the future raised an exception (handled within _test_explore)
                        future.result()
                    except Exception as exc:
                        # This catches exceptions *during* future execution,
                        # but _test_explore should handle SDK errors internally.
                        # Log unexpected errors here.
                        explore_key = f"{explore['model']}/{explore['name']}"
                        logger.error(f"Unexpected error testing explore {explore_key}: {exc}")
                        self.errors[explore_key] = f"Unexpected error: {exc}"

            # Log results
            self._log_results()

            self.log_timing("SQL validation", start_time)

            # Return True if no errors were recorded
            return len(self.errors) == 0

        # <<< --- ADD EXCEPTION TYPE TO CATCH --- >>>
        # Catch the specific validation error if it propagates
        except SQLValidationError as e:
             logger.error(f"SQL Validation failed: {e}")
             # Ensure errors are logged even if validation loop is interrupted
             self._log_results()
             return False
        finally:
            # Clean up temporary branch if needed
            self.cleanup()

    def _test_explore(self, explore: Dict[str, str]):
        """Run a simple test query against a single explore.

        Args:
            explore: Dictionary containing 'model' and 'name' of the explore
        """
        model_name = explore['model']
        explore_name = explore['name']
        explore_key = f"{model_name}/{explore_name}"
        logger.debug(f"Testing explore: {explore_key}")

        try:
            # 1. Find a dimension to query (try common patterns like 'id')
            #    Get explore details to find fields.
            explore_details = self.sdk.lookml_model_explore(
                lookml_model_name=model_name,
                explore_name=explore_name,
                fields="fields" # Request only the fields information
            )

            # Find the first available dimension
            field_to_query = None
            if explore_details.fields and explore_details.fields.dimensions:
                # Prefer 'id' fields if available
                id_fields = [d.name for d in explore_details.fields.dimensions if 'id' in d.name.lower()]
                if id_fields:
                    field_to_query = id_fields[0]
                else:
                    # Fallback to the first dimension listed
                    field_to_query = explore_details.fields.dimensions[0].name

            if not field_to_query:
                 logger.warning(f"Explore {explore_key} has no dimensions, cannot run test query.")
                 # Optionally mark as an error or just skip
                 # self.errors[explore_key] = "Explore has no dimensions to query."
                 return # Skip testing this explore

            logger.debug(f"Using field '{field_to_query}' for explore {explore_key}")

            # 2. Create a simple query body using the found field
            query_body = WriteQuery(
                model=model_name,
                view=explore_name,
                fields=[field_to_query],
                limit="1" # Limit to 1 row for efficiency
            )

            # 3. Create and run the SQL query
            #    Using run_inline_query which combines create and run
            #    Note: run_inline_query might behave differently across versions.
            #    If issues arise, revert to create_query + run_query.
            self.sdk.run_inline_query(result_format="sql", body=query_body)

            # If the above line doesn't raise an SDKError, the query syntax is likely valid
            logger.debug(f"Explore {explore_key} SQL generated successfully.")

        except SDKError as e:
            # Catch Looker SDK errors (e.g., invalid field, SQL generation error)
            error_message = f"SDKError testing explore {explore_key}: {e.message}"
            logger.error(error_message)
            # Extract a cleaner error if possible from e.errors or e.message
            clean_error = e.message # Default to full message
            if hasattr(e, 'errors') and e.errors and isinstance(e.errors, list) and len(e.errors) > 0:
                 # Try to get the first error message if available
                 first_error = e.errors[0]
                 if isinstance(first_error, dict) and 'message' in first_error:
                     clean_error = first_error['message']

            self.errors[explore_key] = clean_error # Store the error message

        except Exception as e:
            # Catch any other unexpected errors during the test
            error_message = f"Unexpected error testing explore {explore_key}: {str(e)}"
            logger.error(error_message, exc_info=True) # Log traceback for unexpected errors
            self.errors[explore_key] = f"Unexpected error: {str(e)}"

    def _log_results(self):
        """Log SQL validation results."""
        total_errors = len(self.errors)

        logger.info("\n" + "=" * 80)
        logger.info(f"SQL Validation Results for Project: {self.project}")
        if self.branch:
            logger.info(f"Branch: {self.branch}")
        else:
            logger.info("Branch: production")
        logger.info("=" * 80)

        if total_errors > 0:
            logger.error(f"\nFound {total_errors} explores with SQL errors:")
            # Sort errors by explore key for consistent output
            sorted_error_keys = sorted(self.errors.keys())
            for explore_key in sorted_error_keys:
                logger.error(f"  ❌ Explore: {explore_key}")
                # Indent error message for readability
                # Ensure error message is a string before splitting
                error_msg_str = str(self.errors[explore_key])
                error_lines = error_msg_str.split('\n')
                for line in error_lines:
                     logger.error(f"     Error: {line.strip()}")
        else:
            logger.info("\nAll tested explores generated SQL successfully! ✅")

        logger.info("=" * 80)

        # Save errors to log file if any exist
        if self.errors:
            log_filename = f"sql_errors_{self.project}"
            if self.branch:
                # Include branch name in log file if not production
                safe_branch_name = "".join(c if c.isalnum() else "_" for c in self.branch)
                log_filename += f"__{safe_branch_name}"
            log_filename += ".json"
            log_path = os.path.join(self.log_dir, log_filename)

            try:
                os.makedirs(self.log_dir, exist_ok=True) # Ensure log dir exists
                # Format for JSON log: key = explore, value = error message
                with open(log_path, "w") as f:
                    json.dump(self.errors, f, indent=2, sort_keys=True)
                logger.info(f"Detailed SQL error report saved to: {log_path}")
            except Exception as e:
                logger.warning(f"Failed to save SQL error log to {log_path}: {str(e)}")

