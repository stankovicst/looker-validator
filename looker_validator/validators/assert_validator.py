"""
Asynchronous assert validator for running Looker data tests.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Set, Tuple

from looker_validator.async_client import AsyncLookerClient
from looker_validator.exceptions import AssertValidationError
from looker_validator.result_model import DataTestError, ValidationResult, TestResult
from looker_validator.validators.base_validator import AsyncBaseValidator

logger = logging.getLogger(__name__)

# Default concurrency for data tests
DEFAULT_CONCURRENCY = 15  # Matches per-user query limit in most Looker instances


@dataclass
class DataTest:
    """Representation of a LookML data test."""
    name: str
    model: str
    explore: str
    file: str
    line: int
    query_url_params: str
    project: str
    base_url: str
    success: Optional[bool] = None
    runtime: Optional[float] = None
    
    @property
    def lookml_url(self) -> str:
        """Get the URL to the test in the LookML IDE."""
        file_path = self.file.split("/", 1)[1] if "/" in self.file else self.file
        return f"{self.base_url}/projects/{self.project}/files/{file_path}?line={self.line}"
    
    @property
    def explore_url(self) -> str:
        """Get the URL to the explore with the test query."""
        return f"{self.base_url}/explore/{self.model}/{self.explore}?{self.query_url_params}"


class AsyncAssertValidator(AsyncBaseValidator):
    """Validator for running LookML data tests."""
    
    def __init__(
        self,
        client: AsyncLookerClient,
        project: str,
        branch: Optional[str] = None,
        commit_ref: Optional[str] = None,
        remote_reset: bool = False,
        explores: Optional[List[str]] = None,
        concurrency: int = DEFAULT_CONCURRENCY,
        log_dir: str = "logs",
        pin_imports: Optional[Dict[str, str]] = None,
        use_personal_branch: bool = False,
    ):
        """Initialize the assert validator.
        
        Args:
            client: AsyncLookerClient instance
            project: Looker project name
            branch: Git branch name
            commit_ref: Git commit reference
            remote_reset: Whether to reset to remote branch state
            explores: List of explores to validate in format "model/explore"
            concurrency: Number of concurrent tests to run
            log_dir: Directory for logs
            pin_imports: Dictionary of project:ref pairs for imports
            use_personal_branch: Whether to use personal branch
        """
        super().__init__(
            client, 
            project, 
            branch,
            commit_ref,
            remote_reset,
            explores,
            log_dir,
            pin_imports,
            use_personal_branch
        )
        
        self.concurrency = concurrency
    
    async def validate(self) -> ValidationResult:
        """Run LookML data tests.
        
        Returns:
            ValidationResult with the validation results
        """
        start_time = time.time()
        result = ValidationResult(validator="assert", status="passed")
        
        try:
            # Set up branch for validation
            await self.setup_branch()
            
            # Get all data tests in the project
            tests = await self._get_tests()
            
            # Filter tests based on explore selectors
            if self.explore_selectors and self.explore_selectors != ["*/*"]:
                tests = self._filter_tests(tests)
            
            if not tests:
                logger.warning(f"No data tests found for project {self.project}")
                return result
            
            # Run the tests
            logger.info(f"Running {len(tests)} LookML data tests with concurrency={self.concurrency}")
            errors = await self._run_tests(tests)
            
            # Add errors to result
            for error in errors:
                result.add_error(error)
            
            # Add test results
            self._add_test_results(result, tests)
            
            # Add timing information
            result.timing["total"] = time.time() - start_time
            
            return result
            
        except Exception as e:
            logger.error(f"Data test validation failed: {str(e)}")
            raise AssertValidationError(
                title="Data test validation failed",
                detail=f"Failed to run data tests: {str(e)}"
            )
            
        finally:
            # Clean up branch manager
            await self.cleanup()
    
    async def _get_tests(self) -> List[DataTest]:
        """Get all data tests for the project.
        
        Returns:
            List of DataTest objects
        """
        # Get all tests for the project
        tests_data = await self.client.all_lookml_tests(self.project)
        
        # Create DataTest objects
        tests = []
        for test_data in tests_data:
            # Check for ignore tags in test name or file
            if self._has_ignore_tag(test_data):
                logger.debug(f"Ignoring test '{test_data['name']}' due to ignore tag")
                continue
                
            test = DataTest(
                name=test_data["name"],
                model=test_data["model_name"],
                explore=test_data["explore"],
                file=test_data["file"],
                line=test_data["line"],
                query_url_params=test_data["query_url_params"],
                project=self.project,
                base_url=self.client.base_url
            )
            tests.append(test)
        
        return tests
        
    def _has_ignore_tag(self, test_data: Dict[str, Any]) -> bool:
        """Check if a test has an ignore tag.
        
        Args:
            test_data: Test data from the API
            
        Returns:
            True if the test has an ignore tag
        """
        # If test has tags directly
        tags = test_data.get("tags", [])
        for tag in tags:
            tag_lower = tag.lower()
            if "spectacles: ignore" in tag_lower or "looker-validator: ignore" in tag_lower:
                return True
                
        # Check test name for ignore pattern (some LookML developers add it to the name)
        test_name = test_data.get("name", "").lower()
        if "spectacles: ignore" in test_name or "looker-validator: ignore" in test_name:
            return True
            
        return False
    
    def _filter_tests(self, tests: List[DataTest]) -> List[DataTest]:
        """Filter tests based on model/explore selectors.
        
        Args:
            tests: List of tests to filter
            
        Returns:
            Filtered list of tests
        """
        filtered_tests = []
        
        for test in tests:
            # Always include tests without explores
            if not test.explore:
                filtered_tests.append(test)
                continue
            
            # Check if test matches explore selectors
            if self._is_explore_selected(test.model, test.explore):
                filtered_tests.append(test)
        
        return filtered_tests
    
    async def _run_tests(self, tests: List[DataTest]) -> List[DataTestError]:
        """Run data tests with concurrency limits.
        
        Args:
            tests: List of tests to run
            
        Returns:
            List of DataTestError objects
        """
        # Create a semaphore to limit concurrent test runs
        semaphore = asyncio.Semaphore(self.concurrency)
        
        # Run tests concurrently
        tasks = [self._run_test(test, semaphore) for test in tests]
        errors = await asyncio.gather(*tasks)
        
        # Flatten the list of errors
        return [error for error_list in errors for error in error_list]
    
    async def _run_test(
        self, 
        test: DataTest, 
        semaphore: asyncio.Semaphore
    ) -> List[DataTestError]:
        """Run a single data test.
        
        Args:
            test: Test to run
            semaphore: Semaphore for concurrency control
            
        Returns:
            List of DataTestError objects
        """
        errors = []
        
        try:
            async with semaphore:
                # Run the test
                results = await self.client.run_lookml_test(
                    project=self.project,
                    model=test.model,
                    test=test.name
                )
                
                # Get the test result (should be a single item)
                if not results:
                    logger.warning(f"No results returned for test: {test.name}")
                    return errors
                
                # Process the result
                result = results[0]
                
                # Store success/failure
                test.success = result.get("success", False)
                test.runtime = result.get("runtime", 0)
                
                # If test failed, create errors
                if not test.success:
                    for error_data in result.get("errors", []):
                        error = DataTestError(
                            model=error_data.get("model_id", test.model),
                            explore=error_data.get("explore", test.explore),
                            message=error_data.get("message", "Unknown error"),
                            test_name=result.get("test_name", test.name),
                            lookml_url=test.lookml_url,
                            explore_url=test.explore_url
                        )
                        errors.append(error)
        
        except Exception as e:
            logger.error(f"Error running test {test.name}: {str(e)}")
            errors.append(DataTestError(
                model=test.model,
                explore=test.explore,
                message=f"Error running test: {str(e)}",
                test_name=test.name,
                lookml_url=test.lookml_url,
                explore_url=test.explore_url
            ))
        
        return errors
    
    def _add_test_results(self, result: ValidationResult, tests: List[DataTest]) -> None:
        """Add test results for all models and explores.
        
        Args:
            result: ValidationResult to update
            tests: List of data tests that were run
        """
        # Create a mapping of model/explore to test status
        model_explore_status: Dict[Tuple[str, str], bool] = {}
        
        # Process each test
        for test in tests:
            key = (test.model, test.explore)
            
            # If we haven't seen this model/explore before or if any test fails,
            # update the status
            current_status = model_explore_status.get(key, True)
            model_explore_status[key] = current_status and (test.success or False)
        
        # Add test results
        for (model, explore), success in model_explore_status.items():
            test_result = TestResult(
                model=model,
                explore=explore,
                status="passed" if success else "failed"
            )
            result.add_test_result(test_result)