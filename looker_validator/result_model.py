"""
Standardized result models for validators.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union


class SkipReason(str, Enum):
    """Reasons why a validation might be skipped."""
    NO_DIMENSIONS = "no_dimensions"
    UNMODIFIED = "unmodified"
    EXCLUDED = "excluded"


@dataclass
class ValidationError:
    """Base class for validation errors."""
    model: str
    explore: str
    message: str
    metadata: Dict[str, Any] = field(default_factory=dict, init=False)  # Added init=False
    
    def __post_init__(self) -> None:
        """Post-initialization to set up metadata."""
        self.metadata = {}  # Initialize empty metadata
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the error to a dictionary.
        
        Returns:
            Dictionary representation of the error
        """
        return {
            "model": self.model,
            "explore": self.explore,
            "message": self.message,
            "metadata": self.metadata
        }


@dataclass
class SQLError(ValidationError):
    """SQL validation error."""
    dimension: Optional[str] = None
    sql: Optional[str] = None
    lookml_url: Optional[str] = None
    line_number: Optional[int] = None
    
    def __post_init__(self) -> None:
        """Post-initialization to set up metadata."""
        super().__post_init__()  # Initialize base metadata
        self.metadata = {
            "dimension": self.dimension,
            "sql": self.sql,
            "lookml_url": self.lookml_url,
            "line_number": self.line_number
        }


@dataclass
class ContentError(ValidationError):
    """Content validation error."""
    field_name: str
    content_type: str
    title: str
    url: str
    folder: Optional[str] = None
    tile_type: Optional[str] = None
    tile_title: Optional[str] = None
    
    def __post_init__(self) -> None:
        """Post-initialization to set up metadata."""
        super().__post_init__()  # Initialize base metadata
        self.metadata = {
            "field_name": self.field_name,
            "content_type": self.content_type,
            "title": self.title,
            "folder": self.folder,
            "url": self.url
        }
        
        if self.tile_type and self.tile_title:
            self.metadata["tile_type"] = self.tile_type
            self.metadata["tile_title"] = self.tile_title


@dataclass
class LookMLError(ValidationError):
    """LookML validation error."""
    field_name: str
    severity: str
    lookml_url: Optional[str] = None
    file_path: Optional[str] = None
    line_number: Optional[int] = None
    
    def __post_init__(self) -> None:
        """Post-initialization to set up metadata."""
        super().__post_init__()  # Initialize base metadata
        self.metadata = {
            "field_name": self.field_name,
            "severity": self.severity,
            "lookml_url": self.lookml_url,
            "file_path": self.file_path,
            "line_number": self.line_number
        }


@dataclass
class DataTestError(ValidationError):
    """Data test validation error."""
    test_name: str
    lookml_url: str
    explore_url: str
    
    def __post_init__(self) -> None:
        """Post-initialization to set up metadata."""
        super().__post_init__()  # Initialize base metadata
        self.metadata = {
            "test_name": self.test_name,
            "lookml_url": self.lookml_url,
            "explore_url": self.explore_url
        }


@dataclass
class TestResult:
    """Result for a single test."""
    model: str
    explore: str
    status: Literal["passed", "failed", "skipped"]
    skip_reason: Optional[SkipReason] = None
    

@dataclass
class ValidationResult:
    """Standardized result for validator runs."""
    validator: Literal["sql", "content", "assert", "lookml"]
    status: Literal["passed", "failed"]
    tested: List[TestResult] = field(default_factory=list)
    errors: List[ValidationError] = field(default_factory=list)
    successes: List[Dict[str, Any]] = field(default_factory=list)
    timing: Dict[str, float] = field(default_factory=dict)
    
    def add_error(self, error: ValidationError) -> None:
        """Add an error to the results.
        
        Args:
            error: The validation error to add
        """
        self.errors.append(error)
        if self.errors:
            self.status = "failed"
    
    def add_test_result(self, test_result: TestResult) -> None:
        """Add a test result.
        
        Args:
            test_result: The test result to add
        """
        self.tested.append(test_result)
        if test_result.status == "failed" and self.status == "passed":
            self.status = "failed"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the result to a dictionary.
        
        Returns:
            Dictionary representation of the result
        """
        return {
            "validator": self.validator,
            "status": self.status,
            "tested": [vars(t) for t in self.tested],
            "errors": [e.to_dict() for e in self.errors],
            "successes": self.successes,
            "timing": self.timing
        }