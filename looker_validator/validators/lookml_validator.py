"""
Asynchronous LookML validator for syntax checking.
"""

import logging
import time
from typing import Dict, List, Optional, Any, Set, cast

from looker_validator.async_client import AsyncLookerClient, LOOKML_VALIDATION_TIMEOUT
from looker_validator.exceptions import LookMLValidationError
from looker_validator.result_model import LookMLError, ValidationResult, TestResult, SkipReason
from looker_validator.validators.base_validator import AsyncBaseValidator

logger = logging.getLogger(__name__)

# Define severity levels
SEVERITY_LEVELS = {
    "success": 0,
    "info": 10,
    "warning": 20,
    "error": 30,
    "fatal": 40
}


class AsyncLookMLValidator(AsyncBaseValidator):
    """Validator for testing LookML syntax."""
    
    def __init__(
        self,
        client: AsyncLookerClient,
        project: str,
        branch: Optional[str] = None,
        commit_ref: Optional[str] = None,
        remote_reset: bool = False,
        severity: str = "warning",
        log_dir: str = "logs",
        pin_imports: Optional[Dict[str, str]] = None,
        use_personal_branch: bool = False,
        timeout: int = LOOKML_VALIDATION_TIMEOUT,
        incremental: bool = False,
        target: Optional[str] = None,
    ):
        """Initialize the LookML validator.
        
        Args:
            client: AsyncLookerClient instance
            project: Looker project name
            branch: Git branch name
            commit_ref: Git reference to base branch on
            remote_reset: Whether to reset to remote branch state
            severity: Minimum severity level to trigger failure
            log_dir: Directory for logs
            pin_imports: Dictionary of project:ref pairs for imports
            use_personal_branch: Whether to use personal branch
            timeout: Timeout for validation in seconds
            incremental: Whether to perform incremental validation
            target: Target branch for incremental comparison
        """
        super().__init__(
            client, 
            project, 
            branch,
            commit_ref,
            remote_reset,
            None,  # explore selectors not used for LookML validation
            log_dir,
            pin_imports,
            use_personal_branch
        )
        
        self.severity = severity
        self.timeout = timeout
        self.incremental = incremental
        self.target = target
    
    async def validate(self) -> ValidationResult:
        """Run LookML validation.
        
        Returns:
            ValidationResult with the validation results
        """
        start_time = time.time()
        result = ValidationResult(validator="lookml", status="passed")
        
        try:
            # Set up branch for validation
            await self.setup_branch()
            
            # Skip cache check when validating a specific branch
            if self.branch:
                logger.debug(f"Running fresh validation on branch '{self.branch}'")
                validation_results = await self.client.lookml_validation(
                    self.project, 
                    self.timeout
                )
            else:
                # Check for cached validation results only for production
                logger.debug("Checking for cached LookML validation results")
                validation_results = await self.client.cached_lookml_validation(self.project)
                
                # Run validation if no cached results or cache is stale
                if not validation_results or validation_results.get("stale", False):
                    logger.debug("No valid cached results, running full validation")
                    validation_results = await self.client.lookml_validation(
                        self.project, 
                        self.timeout
                    )
                else:
                    logger.debug("Using cached LookML validation results")
            
            # Get all models and explores in the project for tracking what was tested
            models_explores = await self._get_lookml_models_explores()
            
            # If incremental mode, filter to only modified explores
            if self.incremental and (self.branch or self.commit_ref):
                logger.info(f"Running in incremental mode, comparing to {self.target or 'production'}")
                target_models_explores = await self._get_target_models_explores()
                modified_models_explores = self._get_modified_explores(models_explores, target_models_explores)
                
                logger.info(f"Found {self._count_explores(modified_models_explores)} explores modified from target")
                models_explores = modified_models_explores
            
            # Process validation results
            await self._process_validation_results(validation_results, result, models_explores)
            
            # Add timing information
            result.timing["total"] = time.time() - start_time
            
            return result
            
        except Exception as e:
            logger.error(f"LookML validation failed: {str(e)}")
            raise LookMLValidationError(
                title="LookML validation failed",
                detail=f"Failed to validate LookML: {str(e)}"
            )
            
        finally:
            # Clean up branch manager
            await self.cleanup()
    
    def _count_explores(self, models_explores: Dict[str, List[str]]) -> int:
        """Count the total number of explores in the dictionary.
        
        Args:
            models_explores: Dictionary mapping model names to lists of explore names
            
        Returns:
            Total number of explores
        """
        return sum(len(explores) for explores in models_explores.values())
    
    async def _get_lookml_models_explores(self) -> Dict[str, List[str]]:
        """Get all models and explores in the project.
        
        Returns:
            Dictionary mapping model names to lists of explore names
        """
        logger.debug(f"Getting LookML models and explores for project '{self.project}'")
        
        # Get all LookML models
        models = await self.client.get_lookml_models(fields=["name", "project_name", "explores"])
        
        # Filter to models in this project
        project_models = [model for model in models if model.get("project_name") == self.project]
        
        # Create model->explores mapping
        models_explores = {}
        for model in project_models:
            model_name = model["name"]
            explores = []
            
            for explore_data in model.get("explores", []):
                explore_name = explore_data["name"]
                explores.append(explore_name)
            
            if explores:
                models_explores[model_name] = explores
        
        logger.debug(f"Found {len(models_explores)} models with {self._count_explores(models_explores)} explores")
        return models_explores
    
    async def _get_target_models_explores(self) -> Dict[str, List[str]]:
        """Get models and explores from the target branch/commit.
        
        Returns:
            Dictionary mapping model names to lists of explore names
        """
        if not self.target and not self.branch and not self.commit_ref:
            raise ValueError("Target, branch, or commit_ref must be specified for incremental validation")
        
        # Save current branch/commit
        current_branch = self.branch
        current_commit = self.commit_ref
        
        try:
            # Set target branch (or production if not specified)
            self.branch = self.target
            self.commit_ref = None
            
            # Set up branch
            await self.setup_branch()
            
            # Get models and explores on target branch
            return await self._get_lookml_models_explores()
            
        finally:
            # Restore original branch/commit
            self.branch = current_branch
            self.commit_ref = current_commit
            
            # Switch back
            await self.setup_branch()
    
    def _get_modified_explores(
        self, 
        current_models_explores: Dict[str, List[str]], 
        target_models_explores: Dict[str, List[str]]
    ) -> Dict[str, List[str]]:
        """Identify modified explores by comparing current and target branches.
        
        Args:
            current_models_explores: Models and explores in current branch
            target_models_explores: Models and explores in target branch
            
        Returns:
            Dictionary containing only modified or new explores
        """
        modified_models_explores = {}
        
        # Check for new or modified models and explores
        for model, explores in current_models_explores.items():
            # If model doesn't exist in target, all explores are new
            if model not in target_models_explores:
                modified_models_explores[model] = explores
                continue
            
            # Find explores that are new in this model
            target_explores = set(target_models_explores[model])
            modified_explores = [e for e in explores if e not in target_explores]
            
            if modified_explores:
                modified_models_explores[model] = modified_explores
        
        return modified_models_explores
    
    async def _process_validation_results(
        self, 
        validation_results: Dict[str, Any], 
        result: ValidationResult,
        models_explores: Dict[str, List[str]]
    ) -> None:
        """Process LookML validation results.
        
        Args:
            validation_results: Raw validation results from Looker API
            result: ValidationResult object to update
            models_explores: Dictionary mapping model names to lists of explore names
        """
        # Check for empty results
        if not validation_results:
            return
        
        # Get severity threshold
        severity_threshold = SEVERITY_LEVELS.get(self.severity, SEVERITY_LEVELS["warning"])
        
        # Process errors
        errors = validation_results.get("errors", [])
        
        # No errors means validation was successful
        if not errors:
            logger.info("LookML validation passed with no issues")
        
        # Track models/explores with errors for test results
        model_explore_errors = {}
        
        # Process each error
        has_error_over_threshold = False
        for error in errors:
            # Get error details
            model_id = error.get("model_id", "")
            explore = error.get("explore", "")
            
            # Skip errors for explores not in our filtered list
            if (
                models_explores and 
                (model_id not in models_explores or 
                 (explore and explore not in models_explores.get(model_id, [])))
            ):
                continue
                
            message = error.get("message", "Unknown error")
            file_path = error.get("file_path", "")
            line_number = error.get("line_number")
            field_name = error.get("field_name", "")
            severity = error.get("severity", "error").lower()
            
            # Create key for tracking errored explores
            model_explore_key = f"{model_id}/{explore}"
            if model_explore_key not in model_explore_errors:
                model_explore_errors[model_explore_key] = []
            
            # Check for ignore tags
            if self._has_ignore_tag(error):
                logger.debug(f"Ignoring LookML error due to ignore tag: {message}")
                continue
            
            # Create LookML URL if file_path is provided
            lookml_url = None
            if file_path:
                lookml_url = f"{self.client.base_url}/projects/{self.project}/files/{'/'.join(file_path.split('/')[1:])}"
                if line_number:
                    lookml_url += f"?line={line_number}"
            
            # Create error object
            lookml_error = LookMLError(
                model=model_id,
                explore=explore,
                message=message,
                field_name=field_name,
                severity=severity,
                file_path=file_path,
                line_number=line_number,
                lookml_url=lookml_url
            )
            
            # Track error by model/explore
            model_explore_errors[model_explore_key].append(lookml_error)
            
            # Add error to result
            result.add_error(lookml_error)
            
            # Check if error is over threshold
            error_severity = SEVERITY_LEVELS.get(severity, SEVERITY_LEVELS["error"])
            if error_severity >= severity_threshold:
                has_error_over_threshold = True
        
        # Add test results for all models and explores
        test_results_added = set()
        
        # First add results for models/explores with errors
        for model_explore, errors in model_explore_errors.items():
            if "/" in model_explore:
                model, explore = model_explore.split("/", 1)
            else:
                model = model_explore
                explore = "unknown"
            
            test_result = TestResult(
                model=model,
                explore=explore,
                status="failed" if has_error_over_threshold else "passed"
            )
            result.add_test_result(test_result)
            test_results_added.add(model_explore)
        
        # Then add results for all other models/explores that were checked
        for model, explores in models_explores.items():
            for explore in explores:
                model_explore = f"{model}/{explore}"
                if model_explore not in test_results_added:
                    test_result = TestResult(
                        model=model,
                        explore=explore,
                        status="passed"
                    )
                    result.add_test_result(test_result)
        
        # Update overall status based on severity threshold
        if has_error_over_threshold:
            result.status = "failed"
        else:
            result.status = "passed"
            
    def _has_ignore_tag(self, error: Dict[str, Any]) -> bool:
        """Check if an error has an ignore tag.
        
        Args:
            error: Error data from the API
            
        Returns:
            True if the error has an ignore tag
        """
        # Check for tags in error data
        tags = error.get("tags", [])
        for tag in tags:
            tag_lower = tag.lower()
            if "spectacles: ignore" in tag_lower or "looker-validator: ignore" in tag_lower:
                return True
                
        # Check for comments in error message
        message = error.get("message", "").lower()
        if "-- spectacles: ignore" in message or "-- looker-validator: ignore" in message:
            return True
            
        # Check field name for ignore suffix (some developers add it to field names)
        field_name = error.get("field_name", "").lower()
        if "spectacles_ignore" in field_name or "looker_validator_ignore" in field_name:
            return True
            
        return False