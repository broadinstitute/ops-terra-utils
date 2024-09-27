from typing import Any, Optional
import requests
import backoff

GET = "GET"
POST = "POST"
DELETE = "DELETE"
PATCH = "PATCH"
PUT = "PUT"


class RunRequest:
    def __init__(self, token: Any, max_retries: int = 5, max_backoff_time: int = 5 * 60):
        self.max_retries = max_retries
        self.max_backoff_time = max_backoff_time
        self.token = token

    @staticmethod
    def _create_backoff_decorator(max_tries: int, factor: int, max_time: int) -> Any:
        """Create backoff decorator so we can pass in max_tries."""
        return backoff.on_exception(
            backoff.expo,
            requests.exceptions.RequestException,
            max_tries=max_tries,
            factor=factor,
            max_time=max_time
        )

    def run_request(self, uri: str, method: str, data: Any = None, params: Optional[dict] = None,
                    factor: int = 15, content_type: Optional[str] = None,) -> requests.Response:
        """Run request."""
        # Create a custom backoff decorator with the provided parameters
        backoff_decorator = self._create_backoff_decorator(
            max_tries=self.max_retries,
            factor=factor,
            max_time=self.max_backoff_time
        )

        # Apply the backoff decorator to the actual request execution
        @backoff_decorator
        def _make_request() -> requests.Response:
            if method == GET:
                response = requests.get(
                    uri,
                    headers=self.create_headers(content_type=content_type),
                    params=params
                )
            elif method == POST:
                response = requests.post(
                    uri,
                    headers=self.create_headers(content_type=content_type),
                    data=data
                )
            elif method == DELETE:
                response = requests.delete(
                    uri,
                    headers=self.create_headers(content_type=content_type)
                )
            elif method == PATCH:
                response = requests.patch(
                    uri,
                    headers=self.create_headers(content_type=content_type),
                    data=data
                )
            elif method == PUT:
                response = requests.put(
                    uri,
                    headers=self.create_headers(content_type=content_type)
                )
            else:
                raise ValueError(f"Method {method} is not supported")
            if 300 <= response.status_code or response.status_code < 200:
                print(response.text)
                response.raise_for_status()  # Raise an exception for non-200 status codes
            return response

        return _make_request()

    def create_headers(self, content_type: Optional[str] = None, accept: Optional[str] = "application/json") -> dict:
        """Create headers for API calls."""
        self.token.get_token()
        headers = {"Authorization": f"Bearer {self.token.token_string}",
                   "accept": accept}
        if content_type:
            headers["Content-Type"] = content_type
        if accept:
            headers["accept"] = accept
        return headers

    def upload_file(self, uri: str, data: dict) -> str:
        """Run Post request with files parameter."""
        headers = self.create_headers(accept=None)
        response = requests.post(uri, headers=headers, files=data)
        if 300 <= response.status_code or response.status_code < 200:
            print(response.text)
            response.raise_for_status()  # Raise an exception for non-200 status codes
        return response.text
