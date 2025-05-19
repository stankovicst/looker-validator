"""
Asynchronous Looker API client for looker-validator.
"""
import asyncio
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple, cast
import logging

import aiohttp
import backoff
from aiohttp import ClientResponseError, ClientConnectorError, ClientTimeout
from urllib.parse import urljoin

from looker_validator.exceptions import LookerApiError, LookerValidatorException

DEFAULT_API_VERSION = "4.0"
DEFAULT_TIMEOUT = 600  # 10 minutes
LOOKML_VALIDATION_TIMEOUT = 7200  # 2 hours for validation

# Exceptions for backoff retry
NETWORK_EXCEPTIONS = (
    ClientConnectorError,
    asyncio.TimeoutError,
)
# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class AccessToken:
    """Immutable access token with expiration tracking."""
    
    def __init__(self, access_token: str, token_type: str, expires_in: int):
        self.access_token = access_token
        self.token_type = token_type
        self.expires_in = expires_in
        self.expires_at = time.time() + expires_in - 60  # 1 minute buffer
        
    def __str__(self) -> str:
        return self.access_token
        
    @property
    def expired(self) -> bool:
        """Check if token is expired."""
        return time.time() >= self.expires_at


class AsyncLookerClient:
    """Asynchronous client for the Looker API."""
    
    def __init__(
        self,
        base_url: str,
        client_id: str,
        client_secret: str,
        port: Optional[int] = None,
        api_version: str = DEFAULT_API_VERSION,
        timeout: int = DEFAULT_TIMEOUT,
    ):
        """Initialize AsyncLookerClient."""
        self.base_url = base_url.rstrip("/")
        self.client_id = client_id
        self.client_secret = client_secret
        self.api_version = api_version
        self.timeout = timeout
        self.access_token: Optional[AccessToken] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.workspace: str = "production"
        self._auth_lock = asyncio.Lock()  # Lock for thread-safe authentication

    async def __aenter__(self) -> "AsyncLookerClient":
        """Set up client session."""
        self.session = aiohttp.ClientSession(
            timeout=ClientTimeout(
                total=self.timeout,
                connect=60,  # Connection timeout
                sock_connect=60,  # Socket connection timeout
                sock_read=self.timeout  # Socket read timeout
            ),
            connector=aiohttp.TCPConnector(keepalive_timeout=120)
        )
        await self.authenticate()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Clean up client session."""
        if self.session:
            await self.session.close()
            self.session = None
    
    async def authenticate(self) -> None:
        """Authenticate with Looker API."""
        # Only one thread should authenticate at a time
        async with self._auth_lock:
            # Check if another thread already authenticated while we were waiting
            if self.access_token and not self.access_token.expired:
                return
                
            if not self.session:
                self.session = aiohttp.ClientSession(
                    timeout=ClientTimeout(
                        total=self.timeout,
                        connect=60,  # Connection timeout
                        sock_connect=60,  # Socket connection timeout
                        sock_read=self.timeout  # Socket read timeout
                    ),
                    connector=aiohttp.TCPConnector(keepalive_timeout=120)
                )
        
            # Direct URL construction
            url = f"{self.base_url}/api/{self.api_version}/login"
            body = {"client_id": self.client_id, "client_secret": self.client_secret}
        
            try:
                # Send auth request - NOT using self.request to avoid recursion
                async with self.session.post(url, data=body, timeout=60) as response:
                    response.raise_for_status()
                    result = await response.json()
                    self.access_token = AccessToken(
                        result["access_token"],
                        result["token_type"],
                        result["expires_in"]
                    )
                
                    # Update session headers with token
                    self.session.headers.update({
                        "Authorization": f"token {self.access_token.access_token}"
                    })
                
                    # Verify connection with version check - direct call, not self.get
                    version_url = f"{self.base_url}/api/{self.api_version}/versions"
                    async with self.session.get(version_url, timeout=30) as version_response:
                        version_response.raise_for_status()
                        version_data = await version_response.json()
                        version = version_data["looker_release_version"]
                        logger.debug(f"Connected to Looker {version} using API {self.api_version}")
                        return
            except aiohttp.ClientError as e:
                raise LookerApiError(
                    title="Authentication failed",
                    detail=f"Failed to authenticate with Looker API: {str(e)}",
                    status=getattr(e, "status", 500),
                )
    
    async def _ensure_authenticated(self) -> None:
        """Ensure client is authenticated, refreshing token if needed."""
        if not self.session:
            await self.__aenter__()
        elif not self.access_token:
            await self.authenticate()
        elif self.access_token.expired:
            logger.debug("Access token expired, refreshing...")
            await self.authenticate()

    async def request(self, method: str, path: str, **kwargs: Any) -> Any:
        """Make an API request with authentication handling."""
        # Ensure we have valid authentication
        await self._ensure_authenticated()
        
        if not self.session:
            raise LookerValidatorException(
                title="No active session",
                detail="Client session is not initialized"
            )
        
        # Construct URL directly
        url = f"{self.base_url}/api/{self.api_version}/{path.lstrip('/')}"
        
        # Create new headers for this request
        headers = dict(self.session.headers)  # Start with session headers
        
        # Update with request-specific headers
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))
        
        # Always explicitly include the auth token in each request
        if self.access_token:
            headers["Authorization"] = f"token {self.access_token.access_token}"
        
        # First attempt - default retry behavior
        try:
            async with self.session.request(method, url, headers=headers, **kwargs) as response:
                response.raise_for_status()
                
                # Handle different response types
                if response.status == 204:  # No content
                    return None
                elif response.content_type and 'application/json' in response.content_type:
                    return await response.json()
                else:
                    return await response.text()
                    
        except ClientResponseError as e:
            # If auth error, try once more with fresh authentication
            if e.status in (401, 403):
                logger.warning(f"Auth error ({e.status}), attempting to re-authenticate...")
                await self.authenticate()
                
                # Try request again with new token
                headers["Authorization"] = f"token {self.access_token.access_token}"
                try:
                    async with self.session.request(method, url, headers=headers, **kwargs) as response:
                        response.raise_for_status()
                        
                        if response.status == 204:  # No content
                            return None
                        elif response.content_type and 'application/json' in response.content_type:
                            return await response.json()
                        else:
                            return await response.text()
                except ClientResponseError as retry_e:
                    # Failed even after re-auth, raise detailed error
                    error_detail = str(retry_e)
                    try:
                        error_text = await retry_e.response.text()
                        error_json = json.loads(error_text)
                        if "message" in error_json:
                            error_detail = error_json["message"]
                    except (json.JSONDecodeError, KeyError, AttributeError):
                        pass
                        
                    raise LookerApiError(
                        title=f"Looker API error ({retry_e.status}) after re-auth",
                        detail=error_detail,
                        status=retry_e.status,
                    )
            
            # Original non-auth error handling
            error_detail = str(e)
            try:
                error_text = await e.response.text()
                error_json = json.loads(error_text)
                if "message" in error_json:
                    error_detail = error_json["message"]
            except (json.JSONDecodeError, KeyError, AttributeError):
                pass
                
            raise LookerApiError(
                title=f"Looker API error ({e.status})",
                detail=error_detail,
                status=e.status,
            )
        except Exception as e:
            raise LookerApiError(
                title="Request failed",
                detail=f"Request to {url} failed: {str(e)}",
                status=500,
            )
    
    async def get(self, path: str, **kwargs: Any) -> Any:
        """Make a GET request to the API."""
        return await self.request("GET", path, **kwargs)
    
    async def post(self, path: str, **kwargs: Any) -> Any:
        """Make a POST request to the API."""
        return await self.request("POST", path, **kwargs)
    
    async def put(self, path: str, **kwargs: Any) -> Any:
        """Make a PUT request to the API."""
        return await self.request("PUT", path, **kwargs)
    
    async def delete(self, path: str, **kwargs: Any) -> Any:
        """Make a DELETE request to the API."""
        return await self.request("DELETE", path, **kwargs)
    
    async def patch(self, path: str, **kwargs: Any) -> Any:
        """Make a PATCH request to the API."""
        return await self.request("PATCH", path, **kwargs)
    
    async def get_looker_release_version(self) -> str:
        """Get the Looker instance release version."""
        result = await self.get("versions")
        return cast(str, result["looker_release_version"])
    
    async def get_workspace(self) -> str:
        """Get the current workspace."""
        result = await self.get("session")
        return cast(str, result["workspace_id"])
    
    async def update_workspace(self, workspace: str) -> None:
        """Update the current workspace.
        
        Args:
            workspace: Either 'production' or 'dev'
        """
        if workspace not in ("production", "dev"):
            raise ValueError("Workspace must be 'production' or 'dev'")
            
        await self.patch("session", json={"workspace_id": workspace})
        self.workspace = workspace
    
    async def get_all_branches(self, project: str) -> List[Dict[str, Any]]:
        """Get all Git branches for a project."""
        return await self.get(f"projects/{project}/git_branches")
    
    async def checkout_branch(self, project: str, branch: str) -> None:
        """Checkout a Git branch."""
        await self.put(
            f"projects/{project}/git_branch",
            json={"name": branch}
        )
    
    async def reset_to_remote(self, project: str) -> None:
        """Reset branch to remote state."""
        await self.post(f"projects/{project}/reset_to_remote")
    
    async def get_manifest(self, project: str) -> Dict[str, Any]:
        """Get project manifest with imported projects."""
        return await self.get(f"projects/{project}/manifest")
    
    async def get_active_branch(self, project: str) -> Dict[str, Any]:
        """Get active branch information."""
        return await self.get(f"projects/{project}/git_branch")
    
    async def get_active_branch_name(self, project: str) -> str:
        """Get name of the active branch."""
        branch_info = await self.get_active_branch(project)
        return cast(str, branch_info["name"])
    
    async def create_branch(
        self, 
        project: str, 
        branch: str, 
        ref: Optional[str] = None
    ) -> None:
        """Create a new Git branch."""
        body = {"name": branch}
        if ref:
            body["ref"] = ref
            
        await self.post(
            f"projects/{project}/git_branch",
            json=body
        )
    
    async def hard_reset_branch(
        self, 
        project: str, 
        branch: str, 
        ref: str
    ) -> None:
        """Hard reset a branch to the specified ref."""
        await self.put(
            f"projects/{project}/git_branch",
            json={"name": branch, "ref": ref}
        )
    
    async def delete_branch(self, project: str, branch: str) -> None:
        """Delete a Git branch."""
        await self.delete(f"projects/{project}/git_branch/{branch}")
    
    async def all_lookml_tests(self, project: str) -> List[Dict[str, Any]]:
        """Get all LookML tests for a project."""
        return await self.get(f"projects/{project}/lookml_tests")
    
    async def run_lookml_test(
        self, 
        project: str, 
        model: Optional[str] = None, 
        test: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Run LookML tests."""
        params = {}
        if model:
            params["model"] = model
        if test:
            params["test"] = test
            
        return await self.get(
            f"projects/{project}/lookml_tests/run",
            params=params
        )
    
    async def get_lookml_models(
        self, 
        fields: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get all LookML models."""
        params = {}
        if fields:
            params["fields"] = ",".join(fields)
            
        # Always disable cache in dev mode
        if self.workspace == "dev":
            params["cache"] = "false"
            
        return await self.get("lookml_models", params=params)
    
    async def get_lookml_dimensions(
        self, 
        model: str, 
        explore: str
    ) -> List[Dict[str, Any]]:
        """Get dimensions for a LookML explore."""
        result = await self.get(
            f"lookml_models/{model}/explores/{explore}",
            params={"fields": "fields"}
        )
        return result["fields"]["dimensions"]
    
    async def create_query(
        self,
        model: str,
        explore: str,
        dimensions: List[str],
        fields: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Create a Looker query."""
        # Always ensure we have fresh authentication
        await self._ensure_authenticated()
        
        body = {
            "model": model,
            "view": explore,
            "fields": dimensions,
            "limit": 0,
            "filter_expression": "1=2",  # Don't return data
        }
        
        params = {}
        if fields:
            params["fields"] = ",".join(fields)
        
        # Use no-cache for explore queries
        params["cache"] = "false"
            
        return await self.post("queries", json=body, params=params)
    
    async def create_query_task(self, query_id: str) -> str:
        """Run a query asynchronously."""
        # Always ensure we have fresh authentication
        await self._ensure_authenticated()
        
        body = {"query_id": query_id, "result_format": "json_detail"}
        params = {"fields": "id", "cache": "false"}
        
        result = await self.post(
            "query_tasks",
            json=body,
            params=params
        )
        return cast(str, result["id"])
    
    async def get_query_task_multi_results(
        self, 
        query_task_ids: Tuple[str, ...]
    ) -> Dict[str, Any]:
        """Get results for multiple query tasks."""
        # Always ensure we have fresh authentication
        await self._ensure_authenticated()
        
        return await self.get(
            "query_tasks/multi_results",
            params={"query_task_ids": ",".join(query_task_ids)}
        )
    
    async def cancel_query_task(self, query_task_id: str) -> None:
        """Cancel a running query task."""
        try:
            await self.delete(f"running_queries/{query_task_id}")
        except LookerApiError:
            # Ignore errors when canceling - query might be finished already
            pass
    
    async def content_validation(self) -> Dict[str, Any]:
        """Run content validation."""
        # Use extended timeout for content validation
        return await self.get("content_validation")
    
    async def lookml_validation(
        self, 
        project: str,
        timeout: int = LOOKML_VALIDATION_TIMEOUT
    ) -> Dict[str, Any]:
        """Run LookML validation."""
        # Save current timeout
        old_timeout = self.timeout
        try:
            # Use extended timeout for LookML validation
            self.timeout = timeout
            if self.session:
                self.session._timeout = ClientTimeout(total=timeout)
            
            params = {}
            # Disable caching for dev mode on non-master branches
            if self.workspace == "dev":
                try:
                    branch = await self.get_active_branch_name(project)
                    if branch != "master" and branch != "production":
                        params["cache"] = "false"
                except Exception:
                    # If we can't get branch, err on side of disabling cache
                    params["cache"] = "false"
                    
            return await self.post(f"projects/{project}/validate", params=params)
        finally:
            # Restore original timeout
            self.timeout = old_timeout
            if self.session:
                self.session._timeout = ClientTimeout(total=old_timeout)
    
    async def cached_lookml_validation(self, project: str) -> Optional[Dict[str, Any]]:
        """Get cached LookML validation results."""
        try:
            return await self.get(f"projects/{project}/validate")
        except LookerApiError as e:
            if e.status == 204:  # No content
                return None
            raise
    
    async def all_folders(self) -> List[Dict[str, Any]]:
        """Get all folders in the Looker instance."""
        return await self.get("folders")
    
    async def run_query(
        self, 
        query_id: str, 
        model: str, 
        explore: str, 
        dimension: Optional[str] = None
    ) -> str:
        """Get compiled SQL for a query."""
        try:
            result = await self.get(f"queries/{query_id}/run/sql")
            return cast(str, result)
        except LookerApiError as e:
            if e.status == 404 or e.status == 400:
                return "-- SQL could not be generated because of errors with this query."
            raise