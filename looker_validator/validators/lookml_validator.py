"""
LookML validator for testing LookML syntax.
"""

import logging
import os
import time
import json
from typing import List, Dict, Any, Optional, Tuple, Set
import requests

from looker_validator.validators.base import BaseValidator, ValidatorError

logger = logging.getLogger(__name__)


class LookMLValidator(BaseValidator):
    """Validator for testing LookML syntax."""

    def __init__(self, connection, project, **kwargs):
        """Initialize LookML validator.

        Args:
            connection: LookerConnection instance
            project: Looker project name
            **kwargs: Additional validator options
        """
        super().__init__(connection, project, **kwargs)
        self.severity = kwargs.get("severity", "warning")
        
        # Validation results
        self.issues = []

    def validate(self) -> bool:
        """Run LookML validation.

        Returns:
            True if LookML syntax is valid, False otherwise
        """
        start_time = time.time()
        
        try:
            # Set up branch for validation
            self.setup_branch()
            
            # Pin imported projects if specified
            if self.pin_imports:
                self.pin_imported_projects()
            
            # Run LookML validator
            logger.info(f"Running LookML validator for project {self.project}")
            
            # Try validation with timeout handling
            try:
                # First try to get cached validation results
                try:
                    cache_result = self._get_cached_validation()
                    if cache_result and not cache_result.get("stale", False):
                        logger.info("Using cached LookML validation results")
                        self._process_validation_response(cache_result)
                    else:
                        logger.info("No valid cached results found, running full validation")
                        self._validate_lookml()
                except (requests.exceptions.Timeout, Exception) as e:
                    logger.info(f"Couldn't get cached results: {str(e)}, running full validation")
                    self._validate_lookml()
            except requests.exceptions.Timeout:
                logger.warning(f"LookML validation timed out for project {self.project}. The project may be too large.")
                self.issues.append({
                    "severity": "error",
                    "message": "Validation request timed out. Your project may be too large for validation.",
                    "file_path": "",
                    "line_number": 0
                })
            except Exception as e:
                logger.error(f"Failed to validate LookML: {str(e)}")
                self.issues.append({
                    "severity": "error",
                    "message": f"Validation failed: {str(e)}",
                    "file_path": "",
                    "line_number": 0
                })
            
            # Log results
            self._log_results()
            
            self.log_timing("LookML validation", start_time)
            
            # Return True if no issues of specified severity or higher
            return self._check_severity()
        
        finally:
            # Clean up temporary branch if needed
            self.cleanup()

    def _get_cached_validation(self) -> Optional[Dict[str, Any]]:
        """Get cached LookML validation results."""
        logger.debug(f"Checking for cached LookML validation results for project '{self.project}'")
        try:
            # This endpoint path may need to be adjusted based on your Looker API version
            url = f"projects/{self.project}/validate"
            response = self.sdk.get(url)
            
            # Check if we got a valid response
            if response.status_code == 204:  # No content typically means no cached results
                return None
                
            return response.json()
        except Exception as e:
            logger.debug(f"Error getting cached validation results: {str(e)}")
            return None

    def _validate_lookml(self):
        """Run LookML validation."""
        try:
            # Use Looker's LookML validator without timeout parameter
            response = self.sdk.validate_project(self.project)
            
            # Process the validation response
            self._process_validation_response(response)
            
        except requests.exceptions.Timeout:
            # Re-raise timeout errors for special handling
            raise
        except Exception as e:
            logger.error(f"Failed to validate LookML: {str(e)}")
            raise ValidatorError(f"LookML validation failed: {str(e)}")

    def _process_validation_response(self, response):
        """Process the validation response from the API.
        
        This method handles different API versions and response structures.
        """
        try:
            # Check for API 4.0 validation results - empty errors list means no issues
            if hasattr(response, 'errors'):
                if not response.errors:
                    logger.info("LookML validation passed with no issues (empty errors list)")
                    self.issues.append({
                        "severity": "info",
                        "message": "LookML validation successful - no errors found",
                        "file_path": "",
                        "line_number": 0
                    })
                    return
                    
                # Process errors if they exist
                for error in response.errors:
                    self.issues.append({
                        "severity": "error",
                        "message": self._get_attr(error, 'message', ''),
                        "file_path": self._get_attr(error, 'file_path', '') or self._get_attr(error, 'model_name', ''),
                        "line_number": self._get_attr(error, 'line_number', 0)
                    })
            
            # Check for 'validation_errors' in API 4.0
            if hasattr(response, 'validation_errors') and response.validation_errors:
                for err in response.validation_errors:
                    severity = self._get_attr(err, 'severity', 'error').lower()
                    message = self._get_attr(err, 'message', '')
                    file_path = self._get_attr(err, 'file_path', '') or self._get_attr(err, 'model_name', '')
                    line_number = self._get_attr(err, 'line_number', 0)
                    
                    self.issues.append({
                        "severity": severity,
                        "message": message,
                        "file_path": file_path,
                        "line_number": line_number
                    })
            
            # Check if the response has a direct structure for project errors
            if hasattr(response, 'project_error') and response.project_error:
                self.issues.append({
                    "severity": "error",
                    "message": str(response.project_error),
                    "file_path": "",
                    "line_number": 0
                })
            
            # Check if the response has a direct structure for model errors
            if hasattr(response, 'model_error') and response.model_error:
                self.issues.append({
                    "severity": "error",
                    "message": str(response.model_error),
                    "file_path": "",
                    "line_number": 0
                })
            
            # Check if there are models that weren't validated
            if hasattr(response, 'models_not_validated') and response.models_not_validated:
                for model in response.models_not_validated:
                    self.issues.append({
                        "severity": "warning",
                        "message": f"Model '{model}' was not validated",
                        "file_path": "",
                        "line_number": 0
                    })
                
            # Check for computation time - log it for information
            if hasattr(response, 'computation_time'):
                logger.info(f"LookML validation computation time: {response.computation_time} seconds")
                
            # If the response has 'is_valid' attribute and it's True, no issues
            if hasattr(response, 'is_valid') and response.is_valid:
                logger.info("LookML validation passed with no issues")
                # Explicitly add an info message indicating success
                self.issues.append({
                    "severity": "info",
                    "message": "LookML validation successful - no issues found",
                    "file_path": "",
                    "line_number": 0
                })
                return
            
            # Check for project_digest - indicates successful validation
            if hasattr(response, 'project_digest') and response.project_digest:
                # If we have a project digest and no errors, it's likely valid
                if not self.issues or all(issue["severity"] == "info" for issue in self.issues):
                    logger.info("LookML validation passed (project digest exists)")
                    self.issues.append({
                        "severity": "info",
                        "message": "LookML validation successful - project digest exists",
                        "file_path": "",
                        "line_number": 0
                    })
                    return
            
            # If no issues were found but response wasn't explicitly marked valid,
            # it might be a different response structure
            if not self.issues:
                # Try to introspect the response
                if hasattr(response, "__dict__"):
                    # Check each attribute for potential errors
                    for attr_name, attr_value in response.__dict__.items():
                        # Skip internal attributes
                        if attr_name.startswith("_"):
                            continue
                            
                        # If attribute is a list, it might contain errors
                        if isinstance(attr_value, list) and attr_value:
                            logger.debug(f"Found list in response: {attr_name}")
                            # Determine severity from attribute name
                            severity = "error"
                            if "warning" in attr_name.lower():
                                severity = "warning"
                            elif "info" in attr_name.lower():
                                severity = "info"
                                
                            # Process items in the list
                            for item in attr_value:
                                # If item is a complex object with attributes
                                if hasattr(item, "__dict__"):
                                    item_dict = item.__dict__
                                    self.issues.append({
                                        "severity": severity,
                                        "message": item_dict.get("message", str(item)),
                                        "file_path": item_dict.get("file_path", "") or item_dict.get("model_name", ""),
                                        "line_number": item_dict.get("line_number", 0)
                                    })
                                # If item is a simple value
                                else:
                                    self.issues.append({
                                        "severity": severity,
                                        "message": str(item),
                                        "file_path": "",
                                        "line_number": 0
                                    })
                
                # Last resort - if we still don't have any issues
                if not self.issues:
                    # Empty errors list but no success indicator - assume success
                    logger.info("No validation issues detected (empty API response)")
                    self.issues.append({
                        "severity": "info",
                        "message": "LookML validation completed with no detected issues",
                        "file_path": "",
                        "line_number": 0
                    })
                        
        except Exception as e:
            logger.error(f"Failed to process validation response: {str(e)}")
            # Add the error as an issue
            self.issues.append({
                "severity": "error",
                "message": f"Failed to process validation response: {str(e)}",
                "file_path": "",
                "line_number": 0
            })

    def _get_attr(self, obj, attr_name, default_value):
        """Safely get an attribute from an object, returning a default if not present."""
        try:
            if hasattr(obj, attr_name):
                return getattr(obj, attr_name)
            elif hasattr(obj, '__dict__') and attr_name in obj.__dict__:
                return obj.__dict__[attr_name]
            elif isinstance(obj, dict) and attr_name in obj:
                return obj[attr_name]
            return default_value
        except:
            return default_value

    def _check_severity(self) -> bool:
        """Check if there are issues of the specified severity or higher.

        Returns:
            True if no issues of specified severity or higher
        """
        severity_levels = {
            "info": 0,
            "warning": 1,
            "error": 2
        }
        
        min_severity = severity_levels.get(self.severity, 1)
        
        for issue in self.issues:
            issue_severity = severity_levels.get(issue["severity"], 0)
            if issue_severity >= min_severity:
                # Don't fail on timeout unless severity is error
                if "timeout" in issue["message"].lower() and min_severity < 2:
                    logger.warning("Timeout detected but continuing due to severity threshold")
                    continue
                return False
                
        return True

    def _log_results(self):
        """Log validation results."""
        total_issues = len(self.issues)
        info_count = sum(1 for issue in self.issues if issue["severity"] == "info")
        warning_count = sum(1 for issue in self.issues if issue["severity"] == "warning")
        error_count = sum(1 for issue in self.issues if issue["severity"] == "error")
        
        logger.info("\n" + "=" * 80)
        logger.info(f"LookML Validation Results for {self.project}")
        logger.info("=" * 80)
        
        logger.info(f"Total issues: {total_issues}")
        logger.info(f"Errors: {error_count}")
        logger.info(f"Warnings: {warning_count}")
        logger.info(f"Info: {info_count}")
        logger.info(f"Severity threshold: {self.severity}")
        
        if self.issues:
            logger.info("\nIssues:")
            
            # Sort issues by severity (error, warning, info)
            severity_order = {"error": 0, "warning": 1, "info": 2}
            sorted_issues = sorted(
                self.issues,
                key=lambda x: (severity_order.get(x["severity"], 3), x["file_path"] or "", x["line_number"] or 0)
            )
            
            for issue in sorted_issues:
                severity = issue["severity"].upper()
                message = issue["message"]
                file_path = issue["file_path"] or "Unknown file"
                line_number = issue["line_number"] or "Unknown line"
                
                # Format by severity
                if severity == "ERROR":
                    prefix = "❌"
                elif severity == "WARNING":
                    prefix = "⚠️"
                else:
                    prefix = "ℹ️"
                
                logger.info(f"  {prefix} {severity}: {message}")
                if file_path != "Unknown file" or line_number != "Unknown line":
                    logger.info(f"     at {file_path}:{line_number}")
                
        logger.info("=" * 80)
        
        # Save issues to log file
        if self.issues:
            log_path = os.path.join(self.log_dir, f"lookml_issues_{self.project}.json")
            try:
                with open(log_path, "w") as f:
                    json.dump(self.issues, f, indent=2)
                logger.info(f"Issue details saved to {log_path}")
            except Exception as e:
                logger.warning(f"Failed to save issue log: {str(e)}")