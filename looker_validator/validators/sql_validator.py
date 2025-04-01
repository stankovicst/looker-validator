"""
SQL validator for testing the SQL in Looker dimensions with improved binary search.
"""

import logging
import os
import re
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple, Set

import looker_sdk
from looker_sdk.sdk.api40 import models as models40
from tqdm import tqdm

from looker_validator.validators.base import BaseValidator, ValidatorError

logger = logging.getLogger(__name__)


class SQLValidator(BaseValidator):
    """Validator for testing SQL in Looker dimensions with improved error isolation."""

    def __init__(self, connection, project, **kwargs):
        """Initialize SQL validator.

        Args:
            connection: LookerConnection instance
            project: Looker project name
            **kwargs: Additional validator options
        """
        super().__init__(connection, project, **kwargs)
        self.concurrency = kwargs.get("concurrency", 10)
        self.fail_fast = kwargs.get("fail_fast", False)
        self.profile = kwargs.get("profile", False)
        self.runtime_threshold = kwargs.get("runtime_threshold", 5)
        self.incremental = kwargs.get("incremental", False)
        self.target = kwargs.get("target")
        self.ignore_hidden = kwargs.get("ignore_hidden", False)
        self.chunk_size = kwargs.get("chunk_size", 500)
        self.max_retries = kwargs.get("max_retries", 2)
        
        # Query tracking
        self.profile_results = []
        
        # Validation results
        self.errors = {}
        self.passing_explores = set()
        self.failing_explores = set()
        self.skipped_explores = set()

    def validate(self) -> bool:
        """Run SQL validation.

        Returns:
            True if all SQL is valid, False otherwise
        """
        start_time = time.time()
        
        try:
            # Set up branch for validation
            self.setup_branch()
            
            # Get all models and explores in the project
            logger.info(f"Finding explores for project {self.project}")
            explores = self._get_all_explores()
            
            if not explores:
                logger.warning(f"No explores found for project {self.project}")
                return True
                
            # Filter explores based on selectors
            explores = self._filter_explores(explores)
            
            if not explores:
                logger.warning(f"No explores match the provided selectors")
                return True
                
            # Get explores to test if using incremental mode
            if self.incremental:
                logger.info("Running in incremental mode, checking for changed SQL")
                explores = self._get_changed_explores(explores)
                
                if not explores:
                    logger.info("No SQL changes detected, skipping validation")
                    return True
            
            # Run SQL validation on all explores
            logger.info(f"Testing {len(explores)} explores [concurrency = {self.concurrency}]")
            self._test_explores(explores)
            
            # Log results
            self._log_results()
            
            # Show profile results if enabled
            if self.profile and self.profile_results:
                self._show_profile_results()
            
            self.log_timing("SQL validation", start_time)
            
            # Return True if no errors
            return len(self.failing_explores) == 0
        
        finally:
            # Clean up temporary branch if needed
            self.cleanup()

    def _get_all_explores(self) -> List[Dict[str, str]]:
        """Get all explores in the project.

        Returns:
            List of explore dictionaries with 'model' and 'name' keys
        """
        try:
            # Get all models in the project
            models_response = self.sdk.all_lookml_models()
            
            # Find models for this project
            project_models = [
                model for model in models_response
                if model.project_name == self.project
            ]
            
            explores = []
            
            # Get all explores for each model
            for model in project_models:
                model_detail = self.sdk.lookml_model(model.name)
                
                for explore in model_detail.explores:
                    explores.append({
                        "model": model.name,
                        "name": explore.name
                    })
            
            return explores
        
        except Exception as e:
            logger.error(f"Failed to get explores: {str(e)}")
            raise ValidatorError(f"Failed to get explores: {str(e)}")

    def _filter_explores(self, explores: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Filter explores based on selectors.

        Args:
            explores: List of explore dictionaries

        Returns:
            Filtered list of explore dictionaries
        """
        if not self.explore_selectors:
            return explores
            
        includes, excludes = self.resolve_explores()
        
        filtered_explores = [
            explore for explore in explores
            if self.matches_selector(
                explore["model"], 
                explore["name"],
                includes, 
                excludes
            )
        ]
        
        return filtered_explores

    def _get_changed_explores(self, explores: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Get explores with changed SQL between branches.

        Args:
            explores: List of explore dictionaries

        Returns:
            List of explores with changed SQL
        """
        # Remember current branch
        current_branch = self.branch
        
        try:
            # Get SQL dimensions on current branch
            current_dimensions = {}
            
            for explore in explores:
                model = explore["model"]
                name = explore["name"]
                dimensions = self._get_explore_dimensions(model, name)
                current_dimensions[f"{model}/{name}"] = dimensions
            
            # Switch to target branch or production
            target = self.target or "production"
            logger.info(f"Checking for SQL changes against {target} branch")
            
            # Store current branch info
            temp_branch = self.branch
            temp_commit = self.commit_ref
            temp_reset = self.remote_reset
            
            # Set branch to target
            self.branch = target if target != "production" else None
            self.commit_ref = None
            self.remote_reset = False
            
            # Set up target branch
            self.setup_branch()
            
            # Get SQL dimensions on target branch
            target_dimensions = {}
            
            for explore in explores:
                model = explore["model"]
                name = explore["name"]
                dimensions = self._get_explore_dimensions(model, name)
                target_dimensions[f"{model}/{name}"] = dimensions
            
            # Reset back to original branch
            self.branch = temp_branch
            self.commit_ref = temp_commit
            self.remote_reset = temp_reset
            self.setup_branch()
            
            # Find explores with changed SQL
            changed_explores = []
            
            for explore in explores:
                key = f"{explore['model']}/{explore['name']}"
                
                # Check if dimensions changed
                if key not in target_dimensions:
                    logger.info(f"New explore found: {key}")
                    changed_explores.append(explore)
                    continue
                
                current = current_dimensions.get(key, {})
                target = target_dimensions.get(key, {})
                
                # Check for changed dimensions
                if self._has_sql_changes(current, target):
                    logger.info(f"SQL changes detected in explore: {key}")
                    changed_explores.append(explore)
                else:
                    logger.info(f"No SQL changes in explore: {key}")
                    self.skipped_explores.add(key)
            
            return changed_explores
            
        except Exception as e:
            logger.error(f"Failed to check for SQL changes: {str(e)}")
            # Fall back to testing all explores
            logger.warning("Falling back to testing all explores")
            return explores

    def _has_sql_changes(self, current_dims: Dict, target_dims: Dict) -> bool:
        """Check if SQL has changed between branches.

        Args:
            current_dims: Dictionary of dimensions from current branch
            target_dims: Dictionary of dimensions from target branch

        Returns:
            True if SQL has changed
        """
        # Check for new or removed dimensions
        current_keys = set(current_dims.keys())
        target_keys = set(target_dims.keys())
        
        if current_keys != target_keys:
            return True
        
        # Check for SQL changes in existing dimensions
        for name, dim in current_dims.items():
            if name not in target_dims:
                return True
                
            current_sql = dim.get("sql", "")
            target_sql = target_dims[name].get("sql", "")
            
            if current_sql != target_sql:
                return True
                
        return False

    def _get_explore_dimensions(self, model: str, explore: str) -> Dict[str, Dict]:
        """Get all dimensions for an explore.

        Args:
            model: Model name
            explore: Explore name

        Returns:
            Dictionary of dimensions with name as key
        """
        try:
            # Get explore metadata
            explore_obj = self.sdk.lookml_model_explore(model, explore)
            
            dimensions = {}
            
            # Process all dimensions
            if hasattr(explore_obj, "fields") and hasattr(explore_obj.fields, "dimensions"):
                for dim in explore_obj.fields.dimensions:
                    # Skip if ignoring hidden dimensions
                    if self.ignore_hidden and dim.hidden:
                        continue
                        
                    # Check for spectacles:ignore tag
                    tags = dim.tags or []
                    if "spectacles: ignore" in tags:
                        continue
                        
                    dimensions[dim.name] = {
                        "sql": dim.sql,
                        "type": dim.type,
                        "hidden": dim.hidden,
                    }
            
            return dimensions
            
        except Exception as e:
            logger.error(f"Failed to get dimensions for {model}/{explore}: {str(e)}")
            return {}

    def _test_explores(self, explores: List[Dict[str, str]]):
        """Test SQL for all explores.

        Args:
            explores: List of explore dictionaries
        """
        # First test all explores with explore-level queries
        self._run_explore_queries(explores)
        
        # If not in fail-fast mode, test failing explores dimension-by-dimension
        if not self.fail_fast and self.failing_explores:
            self._run_dimension_queries()

    def _run_explore_queries(self, explores: List[Dict[str, str]]):
        """Run explore-level queries.

        Args:
            explores: List of explore dictionaries
        """
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = []
            
            for explore in explores:
                futures.append(
                    executor.submit(
                        self._test_explore,
                        explore["model"],
                        explore["name"]
                    )
                )
            
            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc="Testing explores"
            ):
                # Process result if needed
                pass

    def _test_explore(self, model: str, explore: str) -> Tuple[bool, Optional[str]]:
        """Test an explore by running a query with all dimensions.

        Args:
            model: Model name
            explore: Explore name

        Returns:
            Tuple of (success, error_message)
        """
        try:
            # Get dimensions for the explore
            dimensions = self._get_explore_dimensions(model, explore)
            
            if not dimensions:
                logger.warning(f"No dimensions found for {model}/{explore}")
                self.passing_explores.add(f"{model}/{explore}")
                return True, None
            
            # Split dimensions into chunks to avoid large queries
            dimension_names = list(dimensions.keys())
            chunks = [
                dimension_names[i:i + self.chunk_size]
                for i in range(0, len(dimension_names), self.chunk_size)
            ]
            
            for chunk_index, chunk in enumerate(chunks):
                # Test chunk of dimensions
                success, error = self._test_dimension_chunk(model, explore, chunk)
                
                if not success:
                    # Record failing explore
                    explore_key = f"{model}/{explore}"
                    self.failing_explores.add(explore_key)
                    
                    if self.fail_fast:
                        # In fail-fast mode, record the error at explore level
                        self.errors[explore_key] = {
                            "error": error,
                            "dimensions": []
                        }
                    
                    return False, error
            
            # All chunks passed
            self.passing_explores.add(f"{model}/{explore}")
            return True, None
            
        except Exception as e:
            logger.error(f"Error testing {model}/{explore}: {str(e)}")
            self.failing_explores.add(f"{model}/{explore}")
            return False, str(e)

    def _test_dimension_chunk(self, model: str, explore: str, dimension_names: List[str]) -> Tuple[bool, Optional[str]]:
        """Test a chunk of dimensions in an explore.

        Args:
            model: Model name
            explore: Explore name
            dimension_names: List of dimension names to test

        Returns:
            Tuple of (success, error_message)
        """
        if not dimension_names:
            return True, None
            
        # Create query with all dimensions - use API 4.0 models
        query = models40.WriteQuery(
            model=model,
            view=explore,
            fields=dimension_names,
            filters={"1": "2"},  # WHERE 1=2 to prevent data processing
            limit=0  # LIMIT 0 to prevent data fetching
        )
        
        start_time = time.time()
        retries = 0
        
        while retries <= self.max_retries:
            try:
                # Execute query
                response = self.sdk.run_inline_query("sql", query)
                
                # Profile query if enabled
                if self.profile:
                    elapsed = time.time() - start_time
                    if elapsed >= self.runtime_threshold:
                        self._add_profile_result(
                            "explore",
                            f"{model}.{explore}",
                            elapsed,
                            None
                        )
                
                return True, None
                
            except Exception as e:
                retries += 1
                error_message = str(e)
                
                # Profile query if enabled
                if self.profile:
                    elapsed = time.time() - start_time
                    if elapsed >= self.runtime_threshold:
                        self._add_profile_result(
                            "explore",
                            f"{model}.{explore}",
                            elapsed,
                            None
                        )
                
                # Check for SQL error
                if "SQL ERROR" in error_message:
                    return False, error_message
                elif retries <= self.max_retries:
                    # If it's a timeout or temporary error, retry
                    if "timeout" in error_message.lower() or "502" in error_message or "504" in error_message:
                        logger.info(f"Retry {retries}/{self.max_retries} for {model}/{explore} due to: {error_message}")
                        time.sleep(2 ** retries)  # Exponential backoff
                        continue
                    else:
                        logger.error(f"Non-SQL error in {model}/{explore}: {error_message}")
                        return False, error_message
                else:
                    logger.error(f"Max retries exceeded for {model}/{explore}: {error_message}")
                    return False, f"Max retries exceeded: {error_message}"

    def _run_dimension_queries(self):
        """Run dimension-level queries for failing explores using binary search."""
        logger.info("Performing dimension-level testing with binary search")
        
        for explore_key in self.failing_explores:
            model, explore = explore_key.split("/")
            
            # Skip if already in fail-fast mode
            if explore_key in self.errors and self.fail_fast:
                continue
            
            # Get dimensions
            dimensions = self._get_explore_dimensions(model, explore)
            dimension_names = list(dimensions.keys())
            
            # Initialize the error object if needed
            if explore_key not in self.errors:
                self.errors[explore_key] = {
                    "error": None,
                    "dimensions": []
                }
            
            # Use binary search to find errors
            self._binary_search_dimensions(model, explore, dimension_names)

    def _binary_search_dimensions(self, model: str, explore: str, dimension_names: List[str]):
        """Use binary search to identify problematic dimensions.
        
        Args:
            model: Model name
            explore: Explore name
            dimension_names: List of dimension names to test
        """
        # Base case: single dimension
        if len(dimension_names) == 1:
            self._test_single_dimension(model, explore, dimension_names[0])
            return
            
        # Base case: empty list
        if not dimension_names:
            return
            
        # If the list is small enough, test dimensions individually
        if len(dimension_names) <= 5:
            for dim_name in dimension_names:
                self._test_single_dimension(model, explore, dim_name)
            return
            
        # Otherwise, split the list and test each half
        midpoint = len(dimension_names) // 2
        left_chunk = dimension_names[:midpoint]
        right_chunk = dimension_names[midpoint:]
        
        # Test left chunk
        left_success, _ = self._test_dimension_chunk(model, explore, left_chunk)
        if not left_success:
            # Recursively search the left chunk
            self._binary_search_dimensions(model, explore, left_chunk)
        
        # Test right chunk
        right_success, _ = self._test_dimension_chunk(model, explore, right_chunk)
        if not right_success:
            # Recursively search the right chunk
            self._binary_search_dimensions(model, explore, right_chunk)

    def _test_single_dimension(self, model: str, explore: str, dimension: str):
        """Test a single dimension.
        
        Args:
            model: Model name
            explore: Explore name
            dimension: Dimension name
        """
        success, error_message = self._test_dimension_chunk(model, explore, [dimension])
        
        if not success:
            explore_key = f"{model}/{explore}"
            
            # Extract SQL from error if possible
            sql = ""
            try:
                # Try to create a query to extract SQL
                query = models40.WriteQuery(
                    model=model,
                    view=explore,
                    fields=[dimension],
                    filters={"1": "2"},
                    limit=0
                )
                
                # Get SQL without executing
                sql = self.sdk.run_inline_query("sql", query)
            except Exception as e:
                logger.debug(f"Could not extract SQL for dimension {dimension}: {str(e)}")
            
            # Extract query ID if present
            query_id = None
            match = re.search(r"query_id=(\d+)", error_message)
            if match:
                query_id = match.group(1)
                
            # Simplify error message
            if "SQL ERROR" in error_message:
                # Extract SQL error part
                sql_error = re.search(r"SQL ERROR: (.*?)(\n|$)", error_message)
                if sql_error:
                    error_message = sql_error.group(1)
            
            # Record the error
            self.errors[explore_key]["dimensions"].append({
                "name": dimension,
                "error": error_message,
                "query_id": query_id,
                "sql": sql
            })

    def _add_profile_result(self, type_: str, name: str, runtime: float, query_id: Optional[str]):
        """Add a profile result for a long-running query.

        Args:
            type_: Type of query ('explore' or 'dimension')
            name: Name of explore or dimension
            runtime: Runtime in seconds
            query_id: Query ID if available
        """
        self.profile_results.append({
            "type": type_,
            "name": name,
            "runtime": runtime,
            "query_id": query_id
        })

    def _show_profile_results(self):
        """Show profiler results for long-running queries."""
        if not self.profile_results:
            return
            
        # Sort by runtime descending
        sorted_results = sorted(
            self.profile_results,
            key=lambda x: x["runtime"],
            reverse=True
        )
        
        logger.info("\n" + "=" * 80)
        logger.info("Query profiler results")
        logger.info("=" * 80)
        
        # Format as table
        logger.info(f"{'Type':<10} {'Name':<30} {'Runtime (s)':<12} {'Query ID':<10} {'Explore From Here':<50}")
        logger.info(f"{'-'*10} {'-'*30} {'-'*12} {'-'*10} {'-'*50}")
        
        for result in sorted_results:
            type_ = result["type"]
            name = result["name"]
            runtime = f"{result['runtime']:.1f}"
            query_id = result["query_id"] or ""
            
            # Generate explore URL if query ID available
            explore_url = ""
            if query_id:
                explore_url = f"{self.connection.base_url}/x/{query_id}"
            
            logger.info(f"{type_:<10} {name[:30]:<30} {runtime:<12} {query_id:<10} {explore_url:<50}")
        
        logger.info("=" * 80)

    def _log_results(self):
        """Log validation results."""
        total_explores = len(self.passing_explores) + len(self.failing_explores) + len(self.skipped_explores)
        
        logger.info("\n" + "=" * 80)
        logger.info(f"SQL Validation Results for {self.project}")
        logger.info("=" * 80)
        
        if self.incremental:
            logger.info(f"Incremental mode: comparing to {self.target or 'production'}")
        
        logger.info(f"Total explores: {total_explores}")
        logger.info(f"Passing: {len(self.passing_explores)}")
        logger.info(f"Failing: {len(self.failing_explores)}")
        logger.info(f"Skipped: {len(self.skipped_explores)}")
        
        if self.failing_explores:
            logger.info("\nFailed explores:")
            
            for explore_key in sorted(self.failing_explores):
                logger.info(f"  ❌ {explore_key}")
                
                # Log dimension errors if available
                if explore_key in self.errors:
                    error_data = self.errors[explore_key]
                    
                    # Log explore-level error in fail-fast mode
                    if error_data["error"] and self.fail_fast:
                        logger.info(f"    Error: {error_data['error']}")
                    
                    # Log dimension errors
                    for dim_error in error_data.get("dimensions", []):
                        dim_name = dim_error["name"]
                        error_msg = dim_error["error"]
                        
                        logger.info(f"    Dimension: {dim_name}")
                        logger.info(f"      Error: {error_msg}")
        
        if self.passing_explores:
            logger.info("\nPassing explores:")
            for explore_key in sorted(self.passing_explores):
                logger.info(f"  ✓ {explore_key}")
                
        if self.skipped_explores:
            logger.info("\nSkipped explores (no SQL changes):")
            for explore_key in sorted(self.skipped_explores):
                logger.info(f"  ⏭️ {explore_key}")
                
        logger.info("=" * 80)
        
        # Save errors to log file
        if self.errors:
            log_path = os.path.join(self.log_dir, f"sql_errors_{self.project}.json")
            try:
                with open(log_path, "w") as f:
                    json.dump(self.errors, f, indent=2)
                logger.info(f"Error details saved to {log_path}")
            except Exception as e:
                logger.warning(f"Failed to save error log: {str(e)}")