# looker_validator/exceptions.py

"""
Exceptions for the Looker Validator package.
Updated to include specific exceptions used by refactored modules.
"""
from typing import Any, Optional

# Base exception remains ValidatorError as in the original code
class ValidatorError(Exception):
    """Base exception for all Validator errors."""
    def __init__(self, message: str, original_exception: Optional[Exception] = None):
        """
        Initializes the base error.

        Args:
            message: The error message.
            original_exception: The original exception that caused this error, if any.
                                Used for exception chaining (raise NewError from original_exception).
        """
        super().__init__(message)
        self.original_exception = original_exception
        # Preserve original __cause__ if original_exception is provided
        if original_exception:
            self.__cause__ = original_exception

    def __str__(self) -> str:
        # Base string representation remains the message
        return super().__str__()

# --- Added/Renamed Exceptions ---

class LookerAuthenticationError(ValidatorError):
    """Raised for errors during Looker SDK authentication or initialization."""
    pass

class LookerConnectionError(ValidatorError):
    """Raised for errors connecting to the Looker instance after initial auth (e.g., test fails)."""
    pass

class LookerBranchError(ValidatorError):
    """Exception raised when there's an error managing Git branches (via Looker API)."""
    # Renamed from BranchError for consistency
    pass

# --- Existing Exceptions (Hierarchy kept from original) ---

class LookerApiError(ValidatorError):
    """Exception raised when there's an error communicating with the Looker API."""

    def __init__(self, message: str, status_code: Optional[int] = None, response: Optional[Any] = None, original_exception: Optional[Exception] = None):
        """
        Initializes the Looker API error.

        Args:
            message: The error message.
            status_code: The HTTP status code, if available.
            response: The API response body, if available.
            original_exception: The original exception (e.g., looker_sdk.error.SDKError).
        """
        super().__init__(message, original_exception)
        self.status_code = status_code
        self.response = response # Store the response for potential debugging

    def __str__(self) -> str:
        # String representation includes status code, as in the original code
        message = super().__str__()
        if self.status_code:
            return f"{message} (Status code: {self.status_code})"
        return message


class ConfigError(ValidatorError):
    """Exception raised when there's a configuration error."""
    # Inherits __init__ from ValidatorError, allowing original_exception
    pass


# --- Specific validation errors ---

class SQLValidationError(ValidatorError):
    """Exception raised when SQL validation fails."""
    # Consider adding context like model, explore if needed directly in exception
    pass


class ContentValidationError(ValidatorError):
    """Exception raised when content validation fails."""
    # Consider adding context like content_id, type if needed directly in exception
    pass


class AssertValidationError(ValidatorError):
    """Exception raised when assert validation (data test) fails."""
    # Consider adding context like test_name if needed directly in exception
    pass


class LookMLValidationError(ValidatorError):
    """Exception raised when LookML validation fails."""
     # Consider adding context like file_path, line if needed directly in exception
    pass

