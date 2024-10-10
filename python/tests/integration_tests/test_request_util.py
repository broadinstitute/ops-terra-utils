import pytest
import json
from unittest import TestCase
from unittest.mock import MagicMock
from requests import HTTPError

from python.utils.request_util import RunRequest


class TestRunRequest(TestCase):
    """
    The instance of the Token class here is mocked here, so we can simply test the functionality of the interaction
    with URLs and that expected response codes are getting returned. httpbin (https://httpbin.org) is a testing service
    that provides endpoints to test various HTTP methods and responses
    """

    base_url = "https://httpbin.org"

    def setUp(self):
        self.mock_token = MagicMock()
        self.request_util = RunRequest(token=self.mock_token)

    def test_create_headers(self):
        headers = self.request_util.create_headers()
        expected_res = {
            "Authorization": f"Bearer {self.mock_token.token_string}",
            "accept": "application/json"
        }
        self.assertEqual(headers, expected_res)

    def test_create_headers_content_type(self):
        headers = self.request_util.create_headers(content_type="text/tab-separated-values")
        expected_res = {
            "Authorization": f"Bearer {self.mock_token.token_string}",
            "accept": "application/json",
            "Content-Type": "text/tab-separated-values",
        }
        self.assertEqual(headers, expected_res)

    def test_run_request_get(self):
        param = {"foo": "bar"}
        response = self.request_util.run_request(
            uri=f"{self.base_url}/get",
            method="GET",
            params=param,
        )
        self.assertEqual(response.json()["args"], param)
        self.assertEqual(response.status_code, 200)

    def test_run_request_post(self):
        payload = {"foo": "bar"}
        response = self.request_util.run_request(
            uri=f"{self.base_url}/post",
            method="POST",
            data={"foo": "bar"},
        )
        self.assertEqual(response.json()["form"], payload)
        self.assertEqual(response.status_code, 200)

    def test_run_request_delete(self):
        response = self.request_util.run_request(
            uri=f"{self.base_url}/delete",
            method="DELETE",
        )
        self.assertEqual(response.status_code, 200)

    def test_run_request_patch(self):
        payload = {"foo": "bar"}
        response = self.request_util.run_request(
            uri=f"{self.base_url}/patch",
            method="PATCH",
            data=payload,
        )
        self.assertEqual(response.json()["form"], payload)
        self.assertEqual(response.status_code, 200)

    def test_run_request_put(self):
        payload = {"foo": "bar"}
        response = self.request_util.run_request(
            uri=f"{self.base_url}/put",
            method="PUT",
            data=payload,
        )
        self.assertEqual(response.json()["form"], payload)
        self.assertEqual(response.status_code, 200)

    def test_run_request_unsupported_method(self):
        with pytest.raises(ValueError, match="Method PLURT is not supported"):
            self.request_util.run_request(
                uri=f"{self.base_url}/plurt",
                method="PLURT",
            )

    def test_run_request_bad_response_code(self):
        with pytest.raises(HTTPError):
            self.request_util.run_request(
                uri=f"{self.base_url}/status/{500}",
                method="PUT",
            )

    def test_upload_file(self):
        file_name = "some_file.tsv"
        response = self.request_util.upload_file(
            uri=f"{self.base_url}/post",
            data={"file": file_name},
        )
        res_json = json.loads(response)
        self.assertEqual(res_json["files"]["file"], file_name)
