"""
Enhanced asynchronous SQL validator.
"""

import asyncio
import logging
import re  # Added re import
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, Any, cast

# Assuming these imports are correct based on the context
from looker_validator.async_client import AsyncLookerClient
from looker_validator.exceptions import SQLValidationError, LookerValidatorException, LookerApiError
from looker_validator.result_model import SQLError, ValidationResult, TestResult, SkipReason
from looker_validator.validators.base_validator import AsyncBaseValidator

logger = logging.getLogger(__name__)

# Constants
DEFAULT_QUERY_CONCURRENCY = 10
DEFAULT_CHUNK_SIZE = 500
DEFAULT_RUNTIME_THRESHOLD = 5
QUERY_TASK_LIMIT = 250


@dataclass
class LookMLDimension:
    """Representation of a LookML dimension."""
    name: str
    model_name: str
    explore_name: str
    type: str
    tags: List[str]
    sql: str
    is_hidden: bool
    url: Optional[str] = None
    _queried: bool = False
    # Use default_factory for mutable defaults like lists
    errors: List[SQLError] = field(default_factory=list)
    ignore: bool = False  # Initialize ignore attribute

    def __post_init__(self):
        """Post-initialization setup."""
        # Check if dimension should be ignored based on tags or SQL comments
        # Support both 'spectacles: ignore' and 'looker-validator: ignore' patterns
        ignore_tags = {"spectacles: ignore", "looker-validator: ignore"}
        ignore_sql_comments = {"-- spectacles: ignore", "-- looker-validator: ignore"}

        # Check tags (case-insensitive comparison)
        if any(tag.lower() in ignore_tags for tag in self.tags):
            self.ignore = True
        # Check SQL comments (simple substring check)
        elif any(comment in self.sql for comment in ignore_sql_comments):
            self.ignore = True
        else:
            self.ignore = False # Explicitly set to False if no ignore conditions met

    @property
    def queried(self) -> bool:
        """Whether the dimension has been queried."""
        return self._queried

    @queried.setter
    def queried(self, value: bool) -> None:
        """Set the queried status."""
        self._queried = value

    @property
    def errored(self) -> Optional[bool]:
        """Whether the dimension has errors."""
        # Return True if queried and has errors, False if queried and no errors, None if not queried
        return bool(self.errors) if self.queried else None

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any], model_name: str, explore_name: str) -> "LookMLDimension":
        """Create a dimension from JSON data.

        Args:
            json_dict: JSON dictionary from Looker API
            model_name: Name of the model
            explore_name: Name of the explore

        Returns:
            LookMLDimension instance
        """
        return cls(
            name=json_dict["name"],
            model_name=model_name,
            explore_name=explore_name,
            type=json_dict["type"],
            tags=json_dict.get("tags", []),
            sql=json_dict["sql"],
            is_hidden=json_dict.get("hidden", False),
            url=json_dict.get("lookml_link")
        )


@dataclass
class LookMLExplore:
    """Representation of a LookML explore."""
    name: str
    model_name: str
    # Use default_factory for mutable defaults
    dimensions: List[LookMLDimension] = field(default_factory=list)
    errors: List[SQLError] = field(default_factory=list)
    successes: List[Dict[str, Any]] = field(default_factory=list)
    skipped: Optional[SkipReason] = None
    _queried: bool = False

    # No __post_init__ needed if just initializing lists via default_factory

    @property
    def queried(self) -> bool:
        """Whether the explore has been queried (at least one dimension queried)."""
        if self.dimensions:
            return any(dimension.queried for dimension in self.dimensions)
        return self._queried

    @queried.setter
    def queried(self, value: bool) -> None:
        """Set the queried status for the explore and all its dimensions."""
        if self.dimensions:
            for dimension in self.dimensions:
                dimension.queried = value
        else:
            self._queried = value

    @property
    def errored(self) -> Optional[bool]:
        """Whether the explore has errors (explore-level or any dimension-level)."""
        if not self.queried:
            return None
        # Check for explore-level errors or any dimension errors
        return bool(self.errors) or any(dim.errored for dim in self.dimensions if dim.errored is not None)


@dataclass
class Query:
    """SQL query to run."""
    explore: LookMLExplore
    dimensions: Tuple[LookMLDimension, ...]
    query_id: Optional[str] = None
    explore_url: Optional[str] = None
    errored: Optional[bool] = None
    runtime: Optional[float] = None

    def __post_init__(self):
        """Validate the query."""
        # Check that all dimensions are from the same model/explore as the Query's explore
        if not self.dimensions:
            return # Nothing to validate if no dimensions

        first_dim = self.dimensions[0]
        # Check if all dimensions belong to the same model/explore
        if not all(d.model_name == first_dim.model_name and d.explore_name == first_dim.explore_name for d in self.dimensions):
             raise ValueError("All dimensions in a Query must belong to the same model and explore.")

        # Check if the dimensions' model/explore matches the Query's explore
        if (first_dim.model_name != self.explore.model_name or
                first_dim.explore_name != self.explore.name):
            raise ValueError("Dimensions must match the model and explore of the Query.")

    def divide(self) -> List["Query"]:
        """Divide the query into two smaller queries for binary search.

        Returns:
            List of two queries with roughly half the dimensions each.
        """
        if not self.errored:
            raise ValueError("Can only divide errored queries")

        if len(self.dimensions) < 2:
            raise ValueError("Cannot divide query with less than 2 dimensions")

        midpoint = len(self.dimensions) // 2
        return [
            Query(self.explore, self.dimensions[:midpoint]),
            Query(self.explore, self.dimensions[midpoint:])
        ]


class AsyncSQLValidator(AsyncBaseValidator):
    """Validator for testing SQL generation in Looker explores using asynchronous operations."""

    # __init__ method remains the same as the previous version
    def __init__(
        self,
        client: AsyncLookerClient,
        project: str,
        branch: Optional[str] = None,
        commit_ref: Optional[str] = None,
        remote_reset: bool = False,
        explores: Optional[List[str]] = None,
        concurrency: int = DEFAULT_QUERY_CONCURRENCY,
        fail_fast: bool = False,
        profile: bool = False,
        runtime_threshold: int = DEFAULT_RUNTIME_THRESHOLD,
        incremental: bool = False,
        target: Optional[str] = None,
        max_depth: int = 5, # Note: max_depth seems unused in the provided code
        ignore_hidden: bool = False,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        log_dir: str = "logs",
        pin_imports: Optional[Dict[str, str]] = None,
        use_personal_branch: bool = False,
    ):
        """Initialize the SQL validator. (Content identical to previous version)"""
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
        self.fail_fast = fail_fast
        self.profile = profile
        self.runtime_threshold = runtime_threshold
        self.incremental = incremental
        self.target = target
        self.max_depth = max_depth # Store but note it's unused
        self.ignore_hidden = ignore_hidden
        self.chunk_size = chunk_size

        # State for query tracking
        self._task_to_query: Dict[str, Query] = {}
        self._long_running_queries: List[Query] = []

    # validate method remains the same as the previous version
    async def validate(self) -> ValidationResult:
        """Run SQL validation on the specified explores. (Content identical to previous version)"""
        start_time = time.time()
        result = ValidationResult(validator="sql", status="passed") # Assume passed initially
        explores_to_validate: List[LookMLExplore] = []

        try:
            # Set up the Git branch for validation
            await self.setup_branch()

            # Get all explores defined in the project for the current branch/commit
            logger.info("Fetching explores for the current branch/commit...")
            current_explores = await self._get_explores()
            logger.info(f"Found {len(current_explores)} explores in total.")

            # Handle incremental mode: filter explores based on changes
            if self.incremental:
                if not self.target:
                    raise LookerValidatorException("Incremental mode requires a target branch/commit.")
                if not (self.branch or self.commit_ref):
                     raise LookerValidatorException("Incremental mode requires the current state to be a branch or commit.")

                logger.info(f"Incremental mode enabled. Comparing against target '{self.target}'.")
                target_explores = await self._get_target_explores()
                logger.info(f"Found {len(target_explores)} explores in target '{self.target}'.")

                modified_count = 0
                skipped_count = 0
                for explore in current_explores:
                    if self._is_explore_modified(explore, target_explores):
                        explores_to_validate.append(explore)
                        modified_count += 1
                    else:
                        explore.skipped = SkipReason.UNMODIFIED
                        result.add_test_result(TestResult(
                            model=explore.model_name,
                            explore=explore.name,
                            status="skipped",
                            skip_reason=SkipReason.UNMODIFIED
                        ))
                        skipped_count += 1
                logger.info(f"Identified {modified_count} modified explores to validate, {skipped_count} skipped.")
            else:
                # Non-incremental mode: validate all selected explores
                explores_to_validate = current_explores
                logger.info(f"Validating {len(explores_to_validate)} selected explores.")


            # Run the core SQL validation process if there are explores to validate
            if explores_to_validate:
                 await self._validate_explores(explores_to_validate)
            else:
                logger.info("No explores require validation.")


            # Process results after validation runs (or if skipped)
            for explore in explores_to_validate: # Iterate over only those potentially validated
                test_result = TestResult(
                    model=explore.model_name,
                    explore=explore.name,
                    status="passed" # Default assumption
                )

                if explore.skipped: # Should only be NO_DIMENSIONS here
                    test_result.status = "skipped"
                    test_result.skip_reason = explore.skipped
                elif explore.errored:
                    test_result.status = "failed"
                    # Add all collected errors (explore and dimension level) to the main result
                    all_errors = explore.errors + [
                        err for dim in explore.dimensions for err in dim.errors
                    ]
                    for error in all_errors:
                        result.add_error(error)
                        # If fail_fast, we only care about the first error encountered overall
                        if self.fail_fast:
                            break
                    if self.fail_fast and result.errors:
                         # If fail_fast triggered, update overall status immediately
                         result.status = "failed"

                result.add_test_result(test_result)

            # If any test failed (and not fail_fast), update overall status
            if any(tr.status == "failed" for tr in result.tested):
                 result.status = "failed"

            # Add timing information
            result.timing["total"] = time.time() - start_time

            # Output profiling information if requested and relevant
            if self.profile and self._long_running_queries:
                self._print_profile_results()

            return result

        except LookerValidatorException as e:
             logger.error(f"Spectacles Error: {e}")
             result.status = "failed"
             result.add_error(SQLError(model="unknown", explore="unknown", message=str(e)))
             return result
        except Exception as e:
             logger.exception(f"An unexpected error occurred during validation: {e}")
             result.status = "failed"
             result.add_error(SQLError(model="unknown", explore="unknown", message=f"Unexpected error: {e}"))
             return result
        finally:
            await self.cleanup()

    # _get_explores method remains the same as the previous version
    async def _get_explores(self) -> List[LookMLExplore]:
        """Get all explores and their dimensions.
    
        Returns:
            List of LookMLExplore objects
        """
        # Verify we're in dev mode with correct branch before fetching
        if self.branch:
            current_workspace = await self.client.get_workspace()
            if current_workspace != "dev":
                logger.debug(f"Switching to dev workspace for model fetching")
                await self.client.update_workspace("dev")
            
            current_branch = await self.client.get_active_branch_name(self.project)
            if current_branch != self.branch:
                logger.warning(f"Explicitly checking out branch '{self.branch}' for model fetching")
                await self.client.checkout_branch(self.project, self.branch)
    
        logger.debug(f"Getting LookML models for project '{self.project}'...")
        try:
            all_models = await self.client.get_lookml_models(fields=["name", "project_name", "explores"])
            logger.debug(f"Retrieved {len(all_models)} models")
            if all_models and len(all_models) > 0:
                sample_model = all_models[0]
                logger.debug(f"Sample model keys: {list(sample_model.keys())}")
        except Exception as e:
            raise LookerValidatorException(f"Failed to retrieve LookML models: {e}")

        # Handle different API formats
        project_models = []
        for model in all_models:
            # Try different possible key names for project name
            model_project = None
            for key in ["project_name", "project", "projectName"]:
                if key in model:
                    model_project = model[key]
                    break
            # If we have a project name and it matches, include this model
            if model_project and model_project == self.project:
                project_models.append(model)
        if not project_models:
            logger.warning(f"No LookML models found for project '{self.project}'.")
            return []

        logger.debug(f"Found {len(project_models)} models in project '{self.project}'.")
        explores_list: List[LookMLExplore] = []
        explore_fetch_tasks = []

        for model in project_models:
            model_name = model["name"]
            if not model.get("explores"):
                continue

            for explore_data in model["explores"]:
                explore_name = explore_data["name"]

                if self.explore_selectors and not self._is_explore_selected(model_name, explore_name):
                    logger.debug(f"Skipping explore '{model_name}.{explore_name}' due to selectors.")
                    continue

                explore = LookMLExplore(name=explore_name, model_name=model_name)
                explores_list.append(explore)
                explore_fetch_tasks.append(self._fetch_explore_dimensions(explore))

        logger.info(f"Fetching dimensions for {len(explore_fetch_tasks)} selected explores...")
        await asyncio.gather(*explore_fetch_tasks)

        final_explores = []
        for explore in explores_list:
            if explore.errors:
                logger.error(f"Skipping explore '{explore.model_name}.{explore.name}' due to errors fetching dimensions.")
                final_explores.append(explore)
            elif not explore.dimensions and not explore.skipped:
                logger.warning(f"Explore '{explore.model_name}.{explore.name}' has no valid dimensions after filtering, skipping.")
                explore.skipped = SkipReason.NO_DIMENSIONS
                final_explores.append(explore)
            elif explore.dimensions:
                final_explores.append(explore)

        logger.info(f"Prepared {len(final_explores)} explores for validation process.")
        return final_explores

    # _fetch_explore_dimensions method remains the same as the previous version
    async def _fetch_explore_dimensions(self, explore: LookMLExplore):
        """Fetch and process dimensions for a single explore. (Content identical to previous version)"""
        model_name = explore.model_name
        explore_name = explore.name
        logger.debug(f"Fetching dimensions for explore '{model_name}.{explore_name}'...")
        try:
            dimensions_data = await self.client.get_lookml_dimensions(model_name, explore_name)
            valid_dimensions = []
            for dim_data in dimensions_data:
                dimension = LookMLDimension.from_json(dim_data, model_name, explore_name)

                if dimension.url:
                    dimension.url = self.client.base_url + dimension.url

                if dimension.ignore:
                    logger.debug(f"Ignoring dimension '{dimension.name}' due to ignore tag/comment.")
                    continue
                if self.ignore_hidden and dimension.is_hidden:
                    logger.debug(f"Ignoring hidden dimension '{dimension.name}'.")
                    continue

                valid_dimensions.append(dimension)

            explore.dimensions = valid_dimensions
            logger.debug(f"Found {len(valid_dimensions)} valid dimensions for '{model_name}.{explore_name}'.")

        except Exception as e:
            logger.error(f"Error fetching dimensions for explore '{model_name}.{explore_name}': {e}")
            explore.errors.append(SQLError(
                model=model_name,
                explore=explore_name,
                dimension=None,
                message=f"Failed to get dimensions: {e}",
                sql=None,
            ))
            explore.dimensions = []

    # _get_target_explores method remains the same as the previous version
    async def _get_target_explores(self) -> List[LookMLExplore]:
        """Get explores from the target branch/commit. (Content identical to previous version)"""
        if not self.target:
             raise LookerValidatorException("Target must be specified for _get_target_explores")

        logger.info(f"Switching to target '{self.target}' to fetch explores for comparison...")
        original_branch = self.branch
        original_commit = self.commit_ref
        original_target = self.target

        try:
            self.branch = self.target if "/" in self.target else self.target
            self.commit_ref = self.target if "/" not in self.target else None
            self.target = None

            await self.setup_branch(force_checkout=True)

            target_explores = await self._get_explores()
            logger.info(f"Successfully fetched {len(target_explores)} explores from target '{self.branch or self.commit_ref}'.")
            return target_explores

        except Exception as e:
             logger.exception(f"Failed to get explores from target '{original_target}': {e}")
             raise LookerValidatorException(
                title="Target branch error",
                detail=f"Could not retrieve explores from target state '{original_target}'."
            )

        finally:
            logger.info("Switching back to the original branch/commit...")
            self.branch = original_branch
            self.commit_ref = original_commit
            self.target = original_target

            try:
                 await self.setup_branch(force_checkout=True)
                 logger.info("Successfully switched back to the original workspace.")
            except Exception as e:
                 logger.error(f"CRITICAL: Failed to switch back to original branch/commit after checking target: {e}")
                 raise LookerValidatorException("Failed to restore original workspace state after checking target.")

    # _is_explore_modified method remains the same as the previous version
    def _is_explore_modified(self, explore: LookMLExplore, target_explores: List[LookMLExplore]) -> bool:
        """Check if an explore's dimensions have changed. (Content identical to previous version)"""
        target_explore = next((te for te in target_explores if te.model_name == explore.model_name and te.name == explore.name), None)

        if target_explore is None:
            logger.debug(f"Explore '{explore.model_name}.{explore.name}' not found in target, considered modified.")
            return True

        current_dims_sql = {d.name: d.sql for d in explore.dimensions}
        target_dims_sql = {d.name: d.sql for d in target_explore.dimensions}

        if current_dims_sql != target_dims_sql:
            logger.debug(f"Explore '{explore.model_name}.{explore.name}' dimensions/SQL differ from target, considered modified.")
            return True

        logger.debug(f"Explore '{explore.model_name}.{explore.name}' is unchanged compared to target.")
        return False

    # _validate_explores method remains the same as the previous version
    async def _validate_explores(self, explores: List[LookMLExplore]) -> None:
        """Manages the asynchronous validation of explores. (Content identical to previous version)"""
        queries_to_run: asyncio.Queue[Query] = asyncio.Queue()
        running_tasks: asyncio.Queue[str] = asyncio.Queue()
        query_slot = asyncio.Semaphore(self.concurrency)
        total_queries_created = 0

        for explore in explores:
            if explore.skipped or explore.errors:
                continue

            dimensions = tuple(explore.dimensions)
            if not dimensions:
                 logger.warning(f"Explore '{explore.model_name}.{explore.name}' has no dimensions to validate.")
                 continue

            if len(dimensions) <= self.chunk_size:
                await queries_to_run.put(Query(explore, dimensions))
                total_queries_created += 1
            else:
                logger.debug(f"Chunking {len(dimensions)} dimensions for explore '{explore.model_name}.{explore.name}' into sizes of {self.chunk_size}.")
                for i in range(0, len(dimensions), self.chunk_size):
                    chunk = dimensions[i:i + self.chunk_size]
                    await queries_to_run.put(Query(explore, tuple(chunk)))
                    total_queries_created += 1

        if total_queries_created == 0:
             logger.info("No queries generated for validation.")
             return

        logger.info(f"Queued {total_queries_created} initial validation queries.")

        workers = [
            asyncio.create_task(
                self._run_query_creator(queries_to_run, running_tasks, query_slot),
                name="query_creator"
            ),
            asyncio.create_task(
                self._get_query_results_poller(queries_to_run, running_tasks, query_slot),
                name="query_results_poller"
            )
        ]

        try:
            await queries_to_run.join()
            logger.debug("Initial query queue processed.")
            await running_tasks.join()
            logger.debug("All running query tasks completed or errored.")
        except KeyboardInterrupt:
            logger.warning("KeyboardInterrupt received. Attempting to cancel running queries...")
            tasks_to_cancel = []
            while not running_tasks.empty():
                try:
                    task_id = running_tasks.get_nowait()
                    tasks_to_cancel.append(self.client.cancel_query_task(task_id))
                    running_tasks.task_done()
                except asyncio.QueueEmpty: break
                except Exception as e: logger.error(f"Error during cancellation: {e}")
            if tasks_to_cancel:
                 await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
                 logger.info("Sent cancellation requests.")
            raise
        except Exception as e:
             logger.exception(f"Unexpected error during explore validation loop: {e}")
             raise
        finally:
            logger.debug("Cancelling worker tasks...")
            for worker in workers:
                if not worker.done(): worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            logger.debug("Worker tasks finished.")
            self._task_to_query.clear()

    # _run_query_creator method remains the same as the previous version
    async def _run_query_creator(
        self,
        queries_to_run: asyncio.Queue,
        running_tasks: asyncio.Queue,
        query_slot: asyncio.Semaphore
    ) -> None:
        """Worker that creates Looker query tasks."""
        while True:
            try:
                query = await queries_to_run.get()
                await query_slot.acquire()
                try:
                    # Add retry logic for query creation
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            dimension_names = [d.name for d in query.dimensions]
                            logger.debug(f"Creating query for {query.explore.model_name}.{query.explore.name} with {len(dimension_names)} dims.")
                            created_query = await self.client.create_query(
                                model=query.explore.model_name, explore=query.explore.name,
                                dimensions=dimension_names, fields=["id", "share_url"]
                            )
                            query.query_id = created_query["id"]
                            query.explore_url = created_query["share_url"]
                        
                            # Try to create the query task
                            task_id = await self.client.create_query_task(query.query_id)
                            logger.debug(f"Created query task {task_id} for query {query.query_id}.")
                            self._task_to_query[task_id] = query
                            await running_tasks.put(task_id)
                            break  # Success, exit retry loop
                        except LookerApiError as e:
                            if "OAuth" in str(e) or "log in" in str(e) or e.status in (401, 403):
                                # Auth error, retry if we have attempts left
                                if attempt < max_retries - 1:
                                    logger.warning(f"Auth error in query creation, retrying ({attempt+1}/{max_retries})...")
                                    await asyncio.sleep(1)  # Brief pause before retry
                                    continue
                            # Either not an auth error or we've exhausted retries
                            raise
                    else:
                        # We exhausted retries without success
                        raise LookerApiError(
                            title="Authentication failed",
                            detail="Failed to authenticate after multiple retries",
                            status=401
                        )
                    
                except Exception as e:
                    logger.error(f"Error creating query/task for {query.explore.model_name}.{query.explore.name}: {e}")
                    query.errored = True
                    error_msg = f"Failed to create or run query task: {e}"
                    sql_error = SQLError(
                        model=query.explore.model_name, explore=query.explore.name,
                        dimension=query.dimensions[0].name if len(query.dimensions) == 1 else None,
                        message=error_msg, sql=None,
                        lookml_url=query.dimensions[0].url if len(query.dimensions) == 1 else None
                    )
                    if len(query.dimensions) == 1:
                        query.dimensions[0].errors.append(sql_error)
                        query.dimensions[0].queried = True
                    else:
                        query.explore.errors.append(sql_error)
                    query.explore.queried = True
                    query_slot.release()
                    queries_to_run.task_done()
                    if self.fail_fast: logger.warning("Fail fast triggered during query creation.")
            except asyncio.CancelledError:
                logger.debug("Query creator task cancelled.")
                break
            except Exception as e:
                logger.exception(f"Unexpected error in query creator worker: {e}")
                break

    # _get_query_results_poller method remains the same as the previous version
    async def _get_query_results_poller(
        self,
        queries_to_run: asyncio.Queue,
        running_tasks: asyncio.Queue,
        query_slot: asyncio.Semaphore
    ) -> None:
        # Add this helper method inside the function
        async def handle_oauth_error(task_id: str) -> None:
            """Handle OAuth error for a query task."""
            logger.warning(f"OAuth error for task {task_id}, attempting to refresh authentication...")
            try:
                # Force authentication refresh
                await self.client.authenticate()
                # Re-queue the task to try again
                await running_tasks.put(task_id)
                running_tasks.task_done()
                logger.debug(f"Re-queued task {task_id} after authentication refresh")
            except Exception as auth_error:
                logger.error(f"Authentication refresh failed: {auth_error}")
                # Handle the task failure
                if task_id in self._task_to_query:
                    query = self._task_to_query[task_id]
                    query.errored = True
                    error_message = f"Authentication failed: {auth_error}"
                    self._handle_query_error(query, error_message, task_id)
                    del self._task_to_query[task_id]
                    running_tasks.task_done()
                    queries_to_run.task_done()
                    query_slot.release()
        """Worker that polls for results of running query tasks. (Content identical to previous version)"""
        while True:
            task_ids_to_check = []
            try:
                if running_tasks.empty():
                    await asyncio.sleep(0.2)
                    if running_tasks.empty(): continue

                while len(task_ids_to_check) < QUERY_TASK_LIMIT:
                    try: task_ids_to_check.append(running_tasks.get_nowait())
                    except asyncio.QueueEmpty: break

                if not task_ids_to_check: continue

                logger.debug(f"Polling results for {len(task_ids_to_check)} task IDs.")
                results = await self.client.get_query_task_multi_results(tuple(task_ids_to_check))

                for task_id, result_data in results.items():
                    query = self._task_to_query.get(task_id)
                    if not query:
                        logger.warning(f"No Query object found for task ID {task_id}, skipping.")
                        running_tasks.task_done(); continue

                    status = result_data.get("status")

                    if status == "complete":
                        logger.debug(f"Task {task_id} completed successfully.")
                        query.errored = False
                        query.runtime = result_data.get("data", {}).get("runtime")
                        query.explore.queried = True
                        for dim in query.dimensions: dim.queried = True
                        query.explore.successes.append({"dimensions": [d.name for d in query.dimensions], "runtime": query.runtime})
                        if self.profile and query.runtime is not None and query.runtime > self.runtime_threshold:
                            logger.info(f"Query {query.query_id} (Task {task_id}) exceeded runtime threshold: {query.runtime:.2f}s")
                            self._long_running_queries.append(query)
                        del self._task_to_query[task_id]
                        running_tasks.task_done(); queries_to_run.task_done(); query_slot.release()

                    elif status == "error":
                        logger.warning(f"Task {task_id} failed with error.")
                        query.errored = True
                        query.runtime = result_data.get("data", {}).get("runtime")
                        query.explore.queried = True
    
                        # Define error_payload BEFORE trying to access it
                        error_payload = result_data.get("data", {})
    
                        # Now you can get error_message from it
                        error_message = error_payload.get("error", "Unknown error")
                        if isinstance(error_message, str) and ("OAuth" in error_message or "log in required" in error_message):
                            logger.warning(f"OAuth error detected for task {task_id}")
                            await handle_oauth_error(task_id)
                            continue
    
                        for dim in query.dimensions: dim.queried = True

                        sql = error_payload.get("sql")
                        errors = error_payload.get("errors", [])
                        # Handle error message extraction more safely
                        if errors:
                            error_parts = []
                            for e in errors:
                                # Get message details or message, with a fallback
                                message_part = e.get("message_details")
                                if message_part is None:
                                    message_part = e.get("message")
                                if message_part is None:
                                    message_part = "Unknown error detail"
                
                                # Make sure we have a string
                                if not isinstance(message_part, str):
                                    message_part = str(message_part)
                
                                error_parts.append(message_part)
            
                            error_message = "; ".join(error_parts)
                        # We already handled this case earlier, so we can remove this else block
                        # else:
                        #     error_message = error_payload.get("error", "Unknown error")
                        #     if not isinstance(error_message, str):
                        #         error_message = str(error_message)
    
                        logger.debug(f"Error for task {task_id}: {error_message}")


                        if self.fail_fast:
                             error = SQLError(model=query.explore.model_name, explore=query.explore.name, dimension=None, message=error_message, sql=sql, lookml_url=query.explore_url)
                             query.explore.errors.append(error)
                             logger.warning("Fail fast triggered by query error.")
                             del self._task_to_query[task_id]; running_tasks.task_done(); queries_to_run.task_done(); query_slot.release()
                             continue

                        if len(query.dimensions) > 1:
                             logger.debug(f"Error on multi-dimension query ({len(query.dimensions)} dims), dividing.")
                             try:
                                 child_queries = query.divide()
                                 for child_query in child_queries: await queries_to_run.put(child_query)
                                 logger.debug(f"Added {len(child_queries)} child queries.")
                             except ValueError as e:
                                 logger.error(f"Error dividing query for task {task_id}: {e}")
                                 error = SQLError(model=query.explore.model_name, explore=query.explore.name, dimension=None, message=f"Original Error: {error_message}. Failed to divide query.", sql=sql, lookml_url=query.explore_url)
                                 query.explore.errors.append(error)
                             del self._task_to_query[task_id]; running_tasks.task_done(); queries_to_run.task_done(); query_slot.release()
                        else:
                             dimension = query.dimensions[0]
                             logger.warning(f"Error found for dimension: {dimension.model_name}.{dimension.explore_name}.{dimension.name}")
                             error = SQLError(model=dimension.model_name, explore=dimension.explore_name, dimension=dimension.name, message=error_message, sql=sql, lookml_url=dimension.url)
                             dimension.errors.append(error)
                             del self._task_to_query[task_id]; running_tasks.task_done(); queries_to_run.task_done(); query_slot.release()

                    elif status in ("running", "added", "expired"):
                        logger.debug(f"Task {task_id} status is '{status}', re-queuing.")
                        await running_tasks.put(task_id); running_tasks.task_done()

                    elif status == "killed":
                         logger.warning(f"Task {task_id} was killed.")
                         query.errored = True; query.explore.queried = True
                         for dim in query.dimensions: dim.queried = True
                         error_message = "Query task was killed (database timeout or manual cancellation)"
                         sql_error = SQLError(model=query.explore.model_name, explore=query.explore.name, dimension=query.dimensions[0].name if len(query.dimensions) == 1 else None, message=error_message, sql=None, lookml_url=query.dimensions[0].url if len(query.dimensions) == 1 else query.explore_url)
                         if len(query.dimensions) == 1: query.dimensions[0].errors.append(sql_error)
                         else: query.explore.errors.append(sql_error)
                         del self._task_to_query[task_id]; running_tasks.task_done(); queries_to_run.task_done(); query_slot.release()

                    else:
                        logger.error(f"Task {task_id} has unexpected status: '{status}'. Re-queuing.")
                        await running_tasks.put(task_id); running_tasks.task_done()

            except asyncio.CancelledError:
                logger.debug("Query results poller task cancelled.")
                for task_id in task_ids_to_check:
                     try: running_tasks.task_done()
                     except ValueError: pass
                break
            except Exception as e:
                logger.exception(f"Unexpected error in query results poller worker: {e}")
                for task_id in task_ids_to_check:
                    try: await running_tasks.put(task_id); running_tasks.task_done()
                    except Exception as re_queue_error: logger.error(f"Failed to re-queue task {task_id}: {re_queue_error}")
                    try: running_tasks.task_done()
                    except ValueError: pass
                await asyncio.sleep(1)

    # _print_profile_results method remains the same as the previous version
    def _print_profile_results(self) -> None:
        """Prints a formatted table of long-running queries. (Content identical to previous version)"""
        if not self._long_running_queries:
            logger.info("No queries exceeded the runtime threshold.")
            return

        logger.info("\n--- Query Profiler Results ---")
        logger.info(f"Queries exceeding {self.runtime_threshold} seconds:")

        sorted_queries = sorted(self._long_running_queries, key=lambda q: q.runtime or 0, reverse=True)

        headers = ["Explore", "Dimension(s)", "Runtime (s)", "Query URL"]
        header_line = f"{headers[0]:<40} {headers[1]:<40} {headers[2]:<15} {headers[3]}"
        separator = "-" * len(header_line)

        logger.info(separator)
        logger.info(header_line)
        logger.info(separator)

        for query in sorted_queries:
            explore_name = f"{query.explore.model_name}.{query.explore.name}"
            dimension_info = query.dimensions[0].name if len(query.dimensions) == 1 else f"({len(query.dimensions)} dimensions)"
            runtime_str = f"{query.runtime:.2f}" if query.runtime is not None else "N/A"
            url = query.explore_url or "N/A"
            logger.info(f"{explore_name:<40} {dimension_info:<40} {runtime_str:<15} {url}")

        logger.info(separator)

