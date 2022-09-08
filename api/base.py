# Based on Microsoft ADL library (simplification)

from requests.sessions import Session
from .exceptions import (
    HttpNotFoundException,
    HttpPermisionDeniedException,
    HttpRestException,
)
from .retry import ExponentialRetryPolicy, RetryPolicy
from typing import Dict, Optional, Union
from .model import RestOperator
from requests.auth import AuthBase
from requests.models import Response
import threading
import requests


class RestApiBase:
    """REST API function factory base class

    Simplifies and unify calls for REST APIs

    Args:
        auth_key (AuthBase, optional): Authentication defined ny requests library. Defaults to None.
        req_timeout_s (int, optional): HTTP call timeout. Defaults to 60.
    """

    _OPERATIONS: Optional[Dict[str, RestOperator]] = None
    __USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"
    __MAX_POOL_CONNECTIONS = 1024

    def __init__(self, auth_key: AuthBase = None, req_timeout_s=60):
        self.local = threading.local()
        self.auth_key = auth_key
        self.req_timeout_s = req_timeout_s

    def _call_operation(
        self,
        operation: str,
        retry_policy: RetryPolicy = None,
        headers: Dict[str, str] = {},
        **kwargs,
    ) -> Response:
        """HTTP REST call from private function factory __call_once

        Creates a call given the operation configuration at dictionary ``_OPERATIONS`` and
        the name of the key at argument ``operation``

        Args:
            operation (str): name of the operation defined on dictionary _OPERATIONS (class instance variable).
                All operations stored in this dictionary are of type RestOperator
            retry_policy (RetryPolicy, optional): If no input, an exponential policy is applied, .
                Defaults to None.
            headers (Dict[str, str], optional): HTTP headers of requests call. Defaults to {}.

        Raises:
            NotImplementedError: Operation is not defined
            ValueError: Problems on configurations or iput arguments
            HttpRestException: Generic server error in REST call
            HttpPermisionDeniedException: Permision denied HTTP error
            HttpNotFoundException: Not found HTTP error

        Returns:
            Response: request HTTP Response model
        """

        retry_policy = (
            ExponentialRetryPolicy() if retry_policy is None else retry_policy
        )

        if self._OPERATIONS is None:
            raise NotImplementedError("class does not have any operator defined")

        if not self._OPERATIONS.get(operation):
            raise NotImplementedError(f"No definition for operation {operation}")

        op = self._OPERATIONS.get(operation)
        url = op.url

        # get reserved values from kwargs
        data = kwargs.pop("data", None)
        stream = kwargs.pop("stream", False)

        keys = set(kwargs)

        # check path variables
        path_vars = keys.intersection(op.path_vars)
        if len(path_vars) < len(op.path_vars):
            raise ValueError("Path variables missing: %s", op.path_vars - keys)
        # replace path variables in url
        url = url.format(
            **dict((key, value) for key, value in kwargs.items() if key in path_vars)
        )
        # remove path variables from keys
        keys = keys - path_vars

        # check required_params and allowed params
        if op.required_params > keys:
            raise ValueError(
                "Required parameters missing: %s", op.required_params - keys
            )
        if keys - op.allowed_params > set():
            raise ValueError("Extra parameters given: %s", keys - op.allowed_params)

        # check headers
        headers_keys = set(headers.keys())
        if op.required_headers > headers_keys:
            raise ValueError(
                "Required header missing: %s", op.required_headers - headers_keys
            )
        if headers_keys - op.allowed_headers > set():
            raise ValueError(
                "Extra header given: %s", headers_keys - op.allowed_headers
            )

        # set params for call
        params = dict((key, value) for key, value in kwargs.items() if key in keys)

        retry_count = -1
        while True:
            retry_count += 1
            last_exception = None
            try:
                response = self.__call_once(
                    method=op.method,
                    url=url,
                    params=params,
                    data=data,
                    stream=stream,
                    headers=headers,
                )
            except requests.exceptions.RequestException as e:
                last_exception = e
                response = None

            request_successful = self._is_successful_response(response, last_exception)
            if request_successful or not retry_policy.should_retry(
                response, last_exception, retry_count
            ):
                break

        if not request_successful and last_exception is not None:
            raise HttpRestException("HTTP error: " + repr(last_exception))

        if response.status_code == 403:
            raise HttpPermisionDeniedException()
        elif response.status_code == 404:
            raise HttpNotFoundException(f"{url} not found")
        elif response.status_code >= 400:
            raise HttpRestException("Operation error from server")

        return response

    def _is_successful_response(self, response: Response, exception: Exception):
        if exception is not None:
            return False
        if 100 <= response.status_code < 300:
            return True
        return False

    @property
    def session(self) -> Session:
        try:
            s = self.local.session
        except AttributeError:
            s = None
        if not s:
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=self.__MAX_POOL_CONNECTIONS,
                pool_maxsize=self.__MAX_POOL_CONNECTIONS,
            )
            s = requests.Session()
            s.mount("default", adapter)
            self.local.session = s
        return s

    def __call_once(
        self,
        method: str,
        url: str,
        params: Dict[str, str],
        data: Union[str, bytes, dict],
        stream: bool,
        headers: Dict[str, str] = {},
    ) -> Response:
        func = getattr(self.session, method)
        req_headers = dict()
        req_headers["User-Agent"] = self.__USER_AGENT
        req_headers["Content-Length"] = str(len(data) if data is not None else 0)
        req_headers.update(headers)
        return func(
            url,
            params=params,
            headers=req_headers,
            data=data,
            auth=self.auth_key,
            stream=stream,
            timeout=self.req_timeout_s,
        )
