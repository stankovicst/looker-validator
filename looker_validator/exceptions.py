"""
Exceptions for the Looker Validator package.
"""

class ValidatorError(Exception):
    """Base exception for all Validator errors."""
    pass


class LookerApiError(ValidatorError):
    """Exception raised when there's an error communicating with the Looker API."""
    
    def __init__(self, message: str, status_code: int = None, response = None):
        self.message = message
        self.status_code = status_code
        self.response = response
        super().__init__(message)
        
    def __str__(self):
        if self.status_code:
            return f"{self.message} (Status code: {self.status_code})"
        return self.message


class BranchError(ValidatorError):
    """Exception raised when there's an error managing Git branches."""
    pass


class ConfigError(ValidatorError):
    """Exception raised when there's a configuration error."""
    pass


class SQLValidationError(ValidatorError):
    """Exception raised when SQL validation fails."""
    pass


class ContentValidationError(ValidatorError):
    """Exception raised when content validation fails."""
    pass


class AssertValidationError(ValidatorError):
    """Exception raised when assert validation fails."""
    pass


class LookMLValidationError(ValidatorError):
    """Exception raised when LookML validation fails."""
    pass