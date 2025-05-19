# FILE: looker_validator/validators/lookml_validator.py
"""
LookML validator: Uses Looker's validate_project API endpoint.
Based on user's original code, updated for new BaseValidator and error handling.
"""

import logging
import time
from typing import List, Dict, Any, Optional

from looker_sdk.error import SDKError
# Import the specific model for the validation result if needed for type hinting
# from looker_sdk.sdk.api40.models import LookmlValidation

# Use BaseValidator from updated artifact base_py_updated_v1
from .base import BaseValidator
# Use central exceptions
from ..exceptions import LookerApiError, LookMLValidationError, ValidatorError

logger = logging.getLogger(__name__)

# Define severity levels
SEVERITY_LEVELS = {
    "success": 0,
    "info": 10,
    "warning": 20,
    "error": 30,
    "fatal": 40
}


class LookMLValidator(BaseValidator):
    """Validator for testing LookML syntax using the validate_project API endpoint."""

    def __init__(self, connection, project, **kwargs):
        """Initialize LookML validator.

        Args:
            connection: LookerConnection instance.
            project: Looker project name.
            **kwargs: Additional validator options, including:
                severity (str): Minimum severity to consider failure (info, warning, error).
                                Note: This is now typically handled by the caller/reporter (CLI).
        """
        super().__init__(connection, project, **kwargs)
        # Severity threshold is no longer used internally; validator returns all issues.
        self.severity = kwargs.get("severity", "warning")

    def _execute_validation(self) -> List[Dict[str, Any]]:
        """Run LookML validation using the Looker API.
        
        Implements the abstract method required by BaseValidator.

        Returns:
            A list of dictionaries, each representing a distinct LookML validation issue found.
            An empty list indicates successful validation with no errors or warnings found.
        """
        start_time = time.time()
        all_issues: List[Dict[str, Any]] = []

        try:
            # Run LookML validator API call
            logger.info(f"Running LookML validation for project '{self.project}'...")

            try:
                # Use the standard SDK method for LookML validation
                # This call might time out based on the global SDK timeout setting.
                validation_response = self.sdk.validate_project(project_id=self.project)
                logger.info("LookML validation API call completed.")

                # Process the response to extract structured issues
                all_issues = self._process_validation_response(validation_response)

            except SDKError as e:
                # Handle API errors during the validation call itself
                # Check if it's a timeout error (often 504 Gateway Timeout or similar)
                status_code = e.status if hasattr(e, 'status') else None
                if status_code in [504, 502, 503] or "timeout" in str(e).lower():
                     error_msg = f"LookML validation request timed out for project '{self.project}'. The project may be too large or the instance busy."
                     logger.error(error_msg)
                     # Return a specific error dictionary for timeout
                     return [{
                         "validator": self.__class__.__name__,
                         "type": "Timeout Error",
                         "severity": "error",
                         "message": error_msg,
                     }]
                else:
                     error_msg = f"Looker API error during LookML validation for project '{self.project}': {e}"
                     logger.error(error_msg, exc_info=True)
                     # Raise a specific error that can be caught by the CLI
                     raise LookerApiError(error_msg, status_code=status_code, original_exception=e)

            except Exception as e:
                 # Catch unexpected errors during validation call or processing
                 error_msg = f"Unexpected error during LookML validation for project '{self.project}': {e}"
                 logger.error(error_msg, exc_info=True)
                 raise LookMLValidationError(error_msg, original_exception=e) # Use LookML specific error

            # Log timing
            self.log_timing("LookML validation", start_time)

            # Log summary based on findings
            if not all_issues:
                logger.info("LookML validation completed successfully with no issues found.")
            else:
                # Count errors/warnings from the structured list
                error_count = sum(1 for issue in all_issues if issue.get("severity") == "error")
                warning_count = sum(1 for issue in all_issues if issue.get("severity") == "warning")
                info_count = sum(1 for issue in all_issues if issue.get("severity") == "info")
                log_summary = f"LookML validation completed: {error_count} errors, {warning_count} warnings, {info_count} info messages found."
                if error_count > 0:
                     logger.error(log_summary)
                elif warning_count > 0:
                     logger.warning(log_summary)
                else:
                     logger.info(log_summary)

            # Return the list of structured issues
            return all_issues

        except (LookerApiError, LookMLValidationError) as e:
            # Re-raise these specific exceptions to be caught by the validate method in BaseValidator
            raise e
        except Exception as e:
            # Catch any other unexpected errors
            error_msg = f"Unexpected error in LookML validation: {e}"
            logger.error(error_msg, exc_info=True)
            raise ValidatorError(error_msg, original_exception=e)


    def _process_validation_response(self, validation_response: Any) -> List[Dict[str, Any]]:
        """Processes the response from the validate_project API call.

        Extracts errors and warnings into a structured list based on the documented
        LookmlValidation object structure (primarily the 'errors' attribute).

        Args:
            validation_response: The response object from sdk.validate_project().
                                 Expected to be LookmlValidation or have an 'errors' list.

        Returns:
            A list of structured issue dictionaries. Returns empty list if no issues found
            or if the response format is unexpected/empty.
        """
        issues: List[Dict[str, Any]] = []
        logger.debug("Processing LookML validation response...")

        # The primary attribute containing issues is 'errors', a list of LookmlValidationError objects.
        if hasattr(validation_response, 'errors') and validation_response.errors:
            logger.debug(f"Found {len(validation_response.errors)} items in 'errors' attribute.")
            for error_obj in validation_response.errors:
                try:
                    # Safely extract attributes from the SDK's LookmlValidationError object
                    severity = getattr(error_obj, 'severity', 'error').lower() # Default to error if missing
                    message = getattr(error_obj, 'message', 'Unknown validation issue')
                    file_path = getattr(error_obj, 'file_path', None)
                    line_number = getattr(error_obj, 'line_number', None)
                    explore_name = getattr(error_obj, 'explore_name', None)
                    model_name = getattr(error_obj, 'model_name', None)
                    field_name = getattr(error_obj, 'field_name', None)

                    issue_dict = {
                        "validator": self.__class__.__name__,
                        "type": "LookML Issue", # General type for LookML problems
                        "severity": severity,
                        "message": message.strip() if message else message, # Clean whitespace
                        "file_path": file_path,
                        "line": line_number, # Use 'line' for consistency with printer
                        "model": model_name,
                        "explore": explore_name,
                        "field": field_name, # Use 'field' for consistency
                    }
                    # Add to list, removing None values for cleaner output
                    issues.append({k: v for k, v in issue_dict.items() if v is not None})
                except Exception as e:
                     logger.warning(f"Failed to process individual LookML validation error object: {e}", exc_info=True)
                     # Add a generic error indicating processing failure
                     issues.append({
                         "validator": self.__class__.__name__,
                         "type": "Processing Error",
                         "severity": "warning",
                         "message": f"Could not parse validation error detail: {e}",
                     })

        # Log if the response indicates validity but no errors were parsed (unlikely but possible)
        elif not issues:
             logger.info("LookML validation response indicates no errors or warnings.")

        else:
             # Response object didn't have 'errors' or it was empty.
             logger.warning("LookML validation response structure not recognized or 'errors' list was empty/missing.")

        logger.debug(f"Processed validation response, extracted {len(issues)} issues.")
        return issues