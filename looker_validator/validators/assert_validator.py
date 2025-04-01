"""
Enhanced data test validator with better concurrency.
"""

import logging
import os
import json
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Optional, Tuple, Set, Callable

import looker_sdk
from tqdm import tqdm

from looker_validator.validators.base import BaseValidator, ValidatorError

logger = logging.getLogger(__name__)

# Default concurrent test limit
DEFAULT_CONCURRENCY = 15  # Matches the per-user query limit in most Looker instances


class AssertValidator(BaseValidator):
    """Validator for running LookML data tests."""

    def __init__(self, connection, project, **kwargs):
        """Initialize Assert validator.

        Args:
            connection: LookerConnection instance
            project: Looker project name
            **kwargs: Additional validator options
        """
        super().__init__(connection, project, **kwargs)
        self.concurrency = kwargs.get("concurrency", DEFAULT_CONCURRENCY)
        
        # Validation results
        self.test_results = {}
        self.passing_tests = set()
        self.failing_tests = set()
        self.test_runtime = {}  # Track test runtimes

    def validate(self) -> bool:
        """Run LookML data tests.

        Returns:
            True if all tests pass, False otherwise
        """
        start_time = time.time()
        
        try:
            # Set up branch for validation
            self.setup_branch()
            
            # Pin imported projects if specified
            if self.pin_imports:
                self.pin_imported_projects()
                
            # Find all LookML tests
            logger.info(f"Finding LookML tests for project {self.project}")
            tests = self._get_tests()
            
            if not tests:
                logger.warning(f"No LookML tests found for project {self.project}")
                return True
                
            # Filter tests based on model/explore selectors
            tests = self._filter_tests(tests)
            
            if not tests:
                logger.warning(f"No tests match the provided selectors")
                return True
                
            # Run tests
            logger.info(f"Running {len(tests)} LookML tests with concurrency={self.concurrency}")
            self._run_tests(tests)
            
            # Log results
            self._log_results()
            
            self.log_timing("Assert validation", start_time)
            
            # Return True if all tests pass
            return len(self.failing_tests) == 0
        
        finally:
            # Clean up temporary branch if needed
            self.cleanup()

    def _get_tests(self) -> List[Dict[str, Any]]:
        """Get all LookML tests for the project.

        Returns:
            List of test dictionaries
        """
        try:
            # Get all models in the project
            models_response = self.sdk.all_lookml_models()
            
            # Find models for this project
            project_models = [
                model for model in models_response
                if model.project_name == self.project
            ]
            
            tests = []
            
            # Get all tests for each model
            for model in project_models:
                try:
                    # Get tests for model
                    model_tests = self.sdk.all_lookml_tests(model.name)
                    
                    for test in model_tests:
                        # Create test dictionary with metadata
                        test_dict = {
                            "model": model.name,
                            "explore": test.explore,
                            "name": test.name,
                            "test_id": f"{model.name}/{test.name}",
                            "file": getattr(test, 'file', None),
                            "line": getattr(test, 'line', None)
                        }
                        tests.append(test_dict)
                except Exception as model_error:
                    logger.error(f"Error getting tests for model {model.name}: {str(model_error)}")
            
            return tests
        
        except Exception as e:
            logger.error(f"Failed to get LookML tests: {str(e)}")
            raise ValidatorError(f"Failed to get LookML tests: {str(e)}")

    def _filter_tests(self, tests: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter tests based on model/explore selectors.

        Args:
            tests: List of test dictionaries

        Returns:
            Filtered list of test dictionaries
        """
        if not self.explore_selectors:
            return tests
            
        includes, excludes = self.resolve_explores()
        
        filtered_tests = []
        for test in tests:
            model = test["model"]
            explore = test["explore"]
            
            # If explore is None or empty, include by default
            if not explore:
                filtered_tests.append(test)
                continue
                
            if self.matches_selector(model, explore, includes, excludes):
                filtered_tests.append(test)
                
        return filtered_tests

    def _run_tests(self, tests: List[Dict[str, Any]]):
        """Run all LookML tests with improved concurrency.

        Args:
            tests: List of test dictionaries
        """
        # Create a semaphore to limit concurrent test execution
        test_queue = asyncio.Queue()
        for test in tests:
            test_queue.put_nowait(test)
            
        # Use ThreadPoolExecutor for parallel execution
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = []
            for _ in range(min(self.concurrency, len(tests))):
                futures.append(
                    executor.submit(self._process_test_queue, test_queue)
                )
                
            # Wait for all tests to complete
            for future in tqdm(
                futures, 
                total=len(futures), 
                desc="Running test workers"
            ):
                future.result()  # This will raise any exceptions from the worker

    def _process_test_queue(self, test_queue: asyncio.Queue):
        """Process tests from the queue until empty.

        Args:
            test_queue: Queue of tests to process
        """
        while not test_queue.empty():
            try:
                test = test_queue.get_nowait()
                self._run_single_test(test)
                test_queue.task_done()
            except asyncio.QueueEmpty:
                break  # Queue is empty
            except Exception as e:
                # Log error but continue processing other tests
                logger.error(f"Error processing test: {str(e)}")
                test_queue.task_done()

    def _run_single_test(self, test: Dict[str, Any]):
        """Run a single LookML test.

        Args:
            test: Test dictionary
        """
        model = test["model"]
        name = test["name"]
        test_id = test["test_id"]
        
        logger.debug(f"Running test {test_id}")
        
        start_time = time.time()
        
        try:
            # Run test
            test_result = self.sdk.run_lookml_test(model, name)
            
            # Record runtime
            runtime = time.time() - start_time
            self.test_runtime[test_id] = runtime
            
            # Process test result
            success = getattr(test_result, "success", False)
            errors = getattr(test_result, "errors", [])
            warnings = getattr(test_result, "warnings", [])
            
            if success and not errors:
                logger.debug(f"Test {test_id} passed ✓ ({runtime:.2f}s)")
                self.passing_tests.add(test_id)
            else:
                logger.debug(f"Test {test_id} failed ❌ ({runtime:.2f}s)")
                self.failing_tests.add(test_id)
                
                # Record errors
                self.test_results[test_id] = {
                    "success": success,
                    "errors": [error.message for error in errors],
                    "warnings": [warning.message for warning in warnings],
                    "runtime": runtime,
                    "file": test.get("file"),
                    "line": test.get("line")
                }
        
        except Exception as e:
            logger.error(f"Error running test {test_id}: {str(e)}")
            self.failing_tests.add(test_id)
            self.test_results[test_id] = {
                "success": False,
                "errors": [f"Exception: {str(e)}"],
                "warnings": [],
                "runtime": time.time() - start_time,
                "file": test.get("file"),
                "line": test.get("line")
            }

    def _log_results(self):
        """Log validation results."""
        total_tests = len(self.passing_tests) + len(self.failing_tests)
        
        logger.info("\n" + "=" * 80)
        logger.info(f"Assert Validation Results for {self.project}")
        logger.info("=" * 80)
        
        logger.info(f"Total tests: {total_tests}")
        logger.info(f"Passing: {len(self.passing_tests)}")
        logger.info(f"Failing: {len(self.failing_tests)}")
        
        if self.failing_tests:
            logger.info("\nFailed tests:")
            
            for test_id in sorted(self.failing_tests):
                logger.info(f"  ❌ {test_id}")
                
                if test_id in self.test_results:
                    result = self.test_results[test_id]
                    runtime = result.get("runtime", 0)
                    
                    # Log file and line if available
                    file_info = ""
                    if result.get("file") and result.get("line"):
                        file_info = f" ({result['file']}:{result['line']})"
                    
                    logger.info(f"    Runtime: {runtime:.2f}s{file_info}")
                    
                    for error in result.get("errors", []):
                        logger.info(f"    Error: {error}")
                        
                    for warning in result.get("warnings", []):
                        logger.info(f"    Warning: {warning}")
        
        if self.passing_tests:
            logger.info("\nPassing tests:")
            for test_id in sorted(self.passing_tests):
                runtime = self.test_runtime.get(test_id, 0)
                logger.info(f"  ✓ {test_id} ({runtime:.2f}s)")
                
        logger.info("=" * 80)
        
        # Save results to log file
        if self.test_results:
            log_path = os.path.join(self.log_dir, f"assert_results_{self.project}.json")
            try:
                with open(log_path, "w") as f:
                    json.dump(self.test_results, f, indent=2)
                logger.info(f"Test results saved to {log_path}")
            except Exception as e:
                logger.warning(f"Failed to save test results: {str(e)}")