"""
Enhanced exceptions for looker-validator package.
"""
from typing import Any, Dict, Optional


class LookerValidatorException(Exception):
    """Base exception for all looker-validator exceptions."""
    
    def __init__(self, title: str, detail: str, name: Optional[str] = None):
        """Initialize the exception.
        
        Args:
            title: Short, human-readable summary of the problem
            detail: Human-readable explanation with details
            name: Optional unique identifier for the error type
        """
        self.title = title
        self.detail = detail
        self.name = name or self.__class__.__name__
        self.exit_code = 1  # Default exit code
        
        # Create the message for the exception
        super().__init__(f"{title}: {detail}")
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(title={self.title!r})"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the exception to a dictionary.
        
        Returns:
            Dictionary representation of the exception
        """
        return {
            "name": self.name,
            "title": self.title,
            "detail": self.detail,
            "exit_code": self.exit_code
        }


class LookerApiError(LookerValidatorException):
    """Exception raised when an error is returned by the Looker API."""
    
    def __init__(
        self, 
        title: str, 
        detail: str, 
        status: int = 500, 
        response: Optional[Dict[str, Any]] = None,
        name: Optional[str] = None
    ):
        """Initialize the exception.
        
        Args:
            title: Short, human-readable summary of the problem
            detail: Human-readable explanation with details
            status: HTTP status code
            response: Optional response data from the Looker API
            name: Optional unique identifier for the error type
        """
        super().__init__(title, detail, name)
        self.status = status
        self.response = response
        self.exit_code = 2  # API errors get a specific exit code
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the exception to a dictionary.
        
        Returns:
            Dictionary representation of the exception
        """
        result = super().to_dict()
        result["status"] = self.status
        if self.response:
            result["response"] = self.response
        return result


class ConfigError(LookerValidatorException):
    """Exception raised for configuration errors."""
    
    def __init__(self, title: str, detail: str):
        super().__init__(title, detail, "config-error")
        self.exit_code = 3


class ValidatorError(LookerValidatorException):
    """Base class for validation errors."""
    
    def __init__(self, title: str, detail: str, name: Optional[str] = None):
        super().__init__(title, detail, name or "validation-error")
        self.exit_code = 10


class BranchError(ValidatorError):
    """Exception raised when there's an error managing Git branches."""
    
    def __init__(self, title: str, detail: str):
        super().__init__(title, detail, "branch-error")
        self.exit_code = 11


class SQLValidationError(ValidatorError):
    """Exception raised when SQL validation fails."""
    
    def __init__(self, title: str, detail: str):
        super().__init__(title, detail, "sql-validation-error")
        self.exit_code = 12


class ContentValidationError(ValidatorError):
    """Exception raised when content validation fails."""
    
    def __init__(self, title: str, detail: str):
        super().__init__(title, detail, "content-validation-error")
        self.exit_code = 13


class AssertValidationError(ValidatorError):
    """Exception raised when assert validation fails."""
    
    def __init__(self, title: str, detail: str):
        super().__init__(title, detail, "assert-validation-error")
        self.exit_code = 14


class LookMLValidationError(ValidatorError):
    """Exception raised when LookML validation fails."""
    
    def __init__(self, title: str, detail: str):
        super().__init__(title, detail, "lookml-validation-error")
        self.exit_code = 15