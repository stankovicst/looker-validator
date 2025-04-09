# FILE: looker_validator/validators/assert_validator.py
"""
Assert validator: Runs LookML data tests (assertions).
Corrected call to all_lookml_tests to remove unsupported 'fields' parameter.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional

from looker_sdk.error import SDKError

from .base import BaseValidator
# Use specific exceptions
from ..exceptions import LookerApiError, AssertValidationError, ValidatorError

logger = logging.getLogger(__name__)

# Default concurrent test limit
DEFAULT_CONCURRENCY = 10

class AssertValidator(BaseValidator):
    """Validator for running LookML data tests (assertions) via the Looker API."""

    def __init__(self, connection, project, **kwargs):
        """Initialize Assert validator."""
        super().__init__(connection, project, **kwargs)
        try:
            # Ensure concurrency is at least 1
            self.concurrency = max(1, int(kwargs.get("concurrency", DEFAULT_CONCURRENCY)))
        except (ValueError, TypeError):
            logger.warning(f"Invalid concurrency value provided ('{kwargs.get('concurrency')}'), defaulting to {DEFAULT_CONCURRENCY}.")
            self.concurrency = DEFAULT_CONCURRENCY

    def _get_tests(self) -> List[Dict[str, Any]]:
        """Gets all LookML data tests for the project.

        Returns:
            List of test dictionaries, each containing test metadata.

        Raises:
            LookerApiError: If fetching tests fails via the API.
            ValidatorError: For unexpected errors during processing.
        """
        logger.debug(f"Fetching LookML data tests for project '{self.project}'...")
        start_time = time.time()
        tests: List[Dict[str, Any]] = []
        try:
            # --- MODIFIED LINE ---
            # Removed the unsupported 'fields' argument from the SDK call.
            project_tests = self.sdk.all_lookml_tests(project_id=self.project)
            # --- END MODIFIED LINE ---

            if not project_tests:
                 logger.info(f"No LookML data tests found via API for project '{self.project}'.")
                 return []

            for test in project_tests:
                 # Ensure necessary attributes exist
                 model_name = getattr(test, 'model_name', None)
                 test_name = getattr(test, 'name', None)
                 if model_name and test_name:
                     test_id = f"{model_name}/{test_name}" # Consistent ID format
                     tests.append({
                         "model": model_name,
                         "explore": getattr(test, 'explore_name', None), # Explore might be optional
                         "name": test_name,
                         "test_id": test_id, # Include for easier reference
                         "file": getattr(test, 'file_path', None), # Use file_path if available
                         "line": getattr(test, 'line_number', None) # Use line_number if available
                     })
                 else:
                      logger.warning(f"Skipping test object due to missing model or name: {vars(test) if hasattr(test, '__dict__') else test}")

            logger.info(f"Found {len(tests)} LookML data tests for project '{self.project}'.")
            self.log_timing("Fetching data tests", start_time)
            return tests

        except SDKError as e:
            error_msg = f"API error fetching LookML tests for project '{self.project}': {e}"
            logger.error(error_msg, exc_info=True)
            raise LookerApiError(error_msg, status_code=e.status if hasattr(e, 'status') else None, original_exception=e) from e
        except Exception as e:
            # Catch potential TypeErrors if the SDK response format changes unexpectedly
            error_msg = f"Unexpected error processing LookML tests for project '{self.project}': {e}"
            logger.error(error_msg, exc_info=True)
            raise ValidatorError(error_msg) from e


    def _filter_tests(self, tests: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filters data tests based on model/explore selectors defined in BaseValidator.

        Args:
            tests: List of test dictionaries from _get_tests().

        Returns:
            Filtered list of test dictionaries.
        """
        if not self.explore_selectors:
            logger.debug("No explore selectors provided, running all found data tests.")
            return tests # Return all if no selectors

        start_time = time.time()
        # Resolve includes/excludes based on selectors
        includes, excludes = self.resolve_explores()
        logger.debug(f"Filtering data tests. Includes: {includes}, Excludes: {excludes}")

        filtered_tests: List[Dict[str, Any]] = []
        for test in tests:
            model = test.get("model")
            explore = test.get("explore") # Can be None

            if not model:
                 logger.warning(f"Skipping test due to missing model name: {test.get('name')}")
                 continue

            # Use matches_selector from BaseValidator.
            # Pass explore name or '*' if explore is None to match model-level wildcards.
            explore_pattern = explore if explore else "*"
            if self.matches_selector(model, explore_pattern, includes, excludes):
                filtered_tests.append(test)
            # else:
            #      if self.verbose: logger.debug(f"Excluding test '{test.get('test_id')}' based on selectors.")


        count_before = len(tests)
        count_after = len(filtered_tests)
        logger.info(f"Filtered data tests: {count_after} selected out of {count_before}.")
        self.log_timing("Filtering data tests", start_time)
        return filtered_tests


    def _run_single_test(self, test: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Runs a single LookML data test using the Looker API.

        Args:
            test: Test dictionary containing metadata ('model', 'name', 'test_id', etc.).

        Returns:
            A dictionary containing error details if the test fails or errors, otherwise None.
        """
        model_name = test["model"]
        test_name = test["name"]
        test_id = test["test_id"] # e.g., "model_name/test_name"

        logger.debug(f"Running data test: {test_id}")
        start_time = time.time()

        try:
            # Run the specific LookML test via SDK
            # Note: The API endpoint might still accept 'model_name' and 'test_name'
            # even if the SDK method signature doesn't explicitly list them for run_lookml_test.
            # The SDK likely uses them internally. If this fails, we might need to adjust.
            test_result = self.sdk.run_lookml_test(
                project_id=self.project,
                # Pass model and test name if required by the underlying API call structure
                # Check SDK documentation or API explorer if needed. Assuming they are used implicitly or via path.
                # model_name=model_name, # May not be needed as direct args here
                # test_name=test_name,   # May not be needed as direct args here
                fields="success,errors" # Request only needed fields in response payload
            )
            runtime = time.time() - start_time
            logger.debug(f"Test {test_id} API call completed in {runtime:.2f}s")

            # Process the result (LookmlTestResult object)
            success = getattr(test_result, "success", False)
            errors = getattr(test_result, "errors", []) # List of LookmlTestError objects

            if success:
                logger.debug(f"Test {test_id} passed.")
                # Use printer? print_success(f"Test {test_id}: PASSED ({runtime:.2f}s)")
                return None # Indicate success
            else:
                logger.error(f"Test {test_id} failed.")
                # Format error messages from the result
                error_messages = [str(getattr(e, 'message', 'Unknown test error')).strip() for e in errors if hasattr(e, 'message')]
                full_error_message = "; ".join(error_messages) if error_messages else "Test failed with no specific error message."
                # Use printer? print_fail(f"Test {test_id}: FAILED ({runtime:.2f}s)")

                # Return structured error dictionary
                return {
                    "validator": self.__class__.__name__,
                    "type": "Data Test Failure",
                    "severity": "error",
                    "model": model_name,
                    "explore": test.get("explore"),
                    "test_name": test_name,
                    "file_path": test.get("file"), # Use the key from _get_tests
                    "line": test.get("line"),     # Use the key from _get_tests
                    "message": full_error_message,
                    "runtime_seconds": round(runtime, 2),
                    # Add full error objects if needed for detailed logging?
                    # "raw_errors": [vars(e) for e in errors if hasattr(e, '__dict__')]
                }

        except SDKError as e:
            runtime = time.time() - start_time
            error_msg = f"API error running data test {test_id}: {e}"
            logger.error(error_msg, exc_info=True)
            # Use printer? print_fail(f"Test {test_id}: API ERROR ({runtime:.2f}s)")
            return {
                "validator": self.__class__.__name__,
                "type": "API Error",
                "severity": "error",
                "model": model_name,
                "explore": test.get("explore"),
                "test_name": test_name,
                "file_path": test.get("file"),
                "line": test.get("line"),
                "message": f"API Error: {getattr(e, 'message', str(e))}", # Get specific message if possible
                "status_code": e.status if hasattr(e, 'status') else None,
                "runtime_seconds": round(runtime, 2),
            }
        except Exception as e:
            runtime = time.time() - start_time
            error_msg = f"Unexpected internal error running data test {test_id}: {e}"
            logger.error(error_msg, exc_info=True)
            # Use printer? print_fail(f"Test {test_id}: INTERNAL ERROR ({runtime:.2f}s)")
            return {
                "validator": self.__class__.__name__,
                "type": "Internal Validator Error",
                "severity": "error",
                "model": model_name,
                "explore": test.get("explore"),
                "test_name": test_name,
                "file_path": test.get("file"),
                "line": test.get("line"),
                "message": f"Internal error: {e}",
                "runtime_seconds": round(runtime, 2),
            }

    def _execute_validation(self) -> List[Dict[str, Any]]:
        """Core assert validation logic, called by BaseValidator.validate()."""
        all_errors: List[Dict[str, Any]] = []
        start_time = time.time()

        try:
            # Find all LookML tests for the project
            logger.info(f"Finding LookML data tests for project '{self.project}'...")
            all_tests = self._get_tests() # Raises LookerApiError or ValidatorError on failure

            if not all_tests:
                # Already logged in _get_tests if none found via API
                # logger.warning(f"No LookML data tests found for project '{self.project}'.")
                return [] # Return empty list, nothing to validate

            # Filter tests based on model/explore selectors
            tests_to_run = self._filter_tests(all_tests)

            if not tests_to_run:
                # Already logged in _filter_tests
                # logger.warning(f"No LookML data tests match the provided selectors for project '{self.project}'.")
                return []

            logger.info(f"Running {len(tests_to_run)} LookML data tests (Concurrency: {self.concurrency})...")

            # Run tests concurrently using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
                future_to_test = {
                    executor.submit(self._run_single_test, test): test
                    for test in tests_to_run
                }

                test_count = len(future_to_test)
                completed_count = 0
                logger.info(f"Submitted {test_count} tests to thread pool.")

                # Process results as they complete
                for future in as_completed(future_to_test):
                    completed_count += 1
                    test = future_to_test[future]
                    test_id = test.get("test_id", "unknown_test")
                    logger.debug(f"Processing result for test {test_id} ({completed_count}/{test_count})")

                    try:
                        # Get result (error dict or None)
                        result_error = future.result()
                        if result_error:
                            all_errors.append(result_error)
                        else:
                            pass # Success
                    except Exception as exc:
                        # Catch unexpected errors from within the thread execution itself
                        logger.error(f"Unexpected internal error processing result for test {test_id}: {exc}", exc_info=True)
                        all_errors.append({
                            "validator": self.__class__.__name__,
                            "type": "Internal Validator Error",
                            "severity": "error",
                            "model": test.get("model"),
                            "explore": test.get("explore"),
                            "test_name": test.get("name"),
                            "file_path": test.get("file"),
                            "line": test.get("line"),
                            "message": f"An internal error occurred processing test result: {exc}",
                        })

            # Log timing
            self.log_timing("Assert validation execution", start_time)

            # Log summary
            passed_count = len(tests_to_run) - len(all_errors)
            if not all_errors:
                logger.info(f"Assert validation completed successfully: {passed_count} tests passed.")
            else:
                logger.error(f"Assert validation completed: {passed_count} tests passed, {len(all_errors)} tests failed or errored.")

            return all_errors

        except (LookerApiError, AssertValidationError, ValidatorError) as e:
             # Re-raise these specific exceptions to be caught by BaseValidator.validate()
             logger.error(f"Assert validation failed during setup or execution: {e}", exc_info=False) # Log the specific error
             raise e # Re-raise
        except Exception as e:
            # Catch any other unexpected errors during setup/filtering
            error_msg = f"Unexpected error during assert validation setup: {e}"
            logger.error(error_msg, exc_info=True)
            # Wrap it in ValidatorError for consistent handling in run_validator
            raise ValidatorError(error_msg, original_exception=e) from e
