import pytest
import json
from unittest.mock import MagicMock
from requests import HTTPError

from python.utils.requests_utils.request_util import RunRequest


"""The instance of the Token class here is mocked, so we can simply test the functionality of the interaction
with URLs and that expected response codes are getting returned. httpbin (https://httpbin.org) is a testing service
that provides endpoints to test various HTTP methods and responses."""

mock_token = MagicMock()
request_util = RunRequest(token=mock_token)
base_url = "https://httpbin.org"


def test_create_headers():
    headers = request_util.create_headers()
    expected_res = {
        "Authorization": f"Bearer {mock_token.token_string}",
        "accept": "application/json"
    }
    assert headers == expected_res


def test_create_headers_content_type():
    headers = request_util.create_headers(content_type="text/tab-separated-values")
    expected_res = {
        "Authorization": f"Bearer {mock_token.token_string}",
        "accept": "application/json",
        "Content-Type": "text/tab-separated-values",
    }
    assert headers == expected_res


def test_run_request_get():
    param = {"foo": "bar"}
    response = request_util.run_request(
        uri=f"{base_url}/get",
        method="GET",
        params=param,
    )
    assert response.json()["args"] == param
    assert response.status_code == 200


def test_run_request_post():
    payload = {"foo": "bar"}
    response = request_util.run_request(
        uri=f"{base_url}/post",
        method="POST",
        data={"foo": "bar"},
    )
    assert response.json()["form"] == payload
    assert response.status_code == 200


def test_run_request_delete():
    response = request_util.run_request(
        uri=f"{base_url}/delete",
        method="DELETE",
    )
    assert response.status_code == 200


def test_run_request_patch():
    payload = {"foo": "bar"}
    response = request_util.run_request(
        uri=f"{base_url}/patch",
        method="PATCH",
        data=payload,
    )
    assert response.json()["form"] == payload
    assert response.status_code == 200


def test_run_request_put():
    payload = {"foo": "bar"}
    response = request_util.run_request(
        uri=f"{base_url}/put",
        method="PUT",
        data=payload,
    )
    assert response.json()["form"] == payload
    assert response.status_code == 200


def test_run_request_unsupported_method():
    with pytest.raises(ValueError, match="Method PLURT is not supported"):
        request_util.run_request(
            uri=f"{base_url}/plurt",
            method="PLURT",
        )


def test_run_request_bad_response_code():
    with pytest.raises(HTTPError):
        request_util.run_request(
            uri=f"{base_url}/status/{500}",
            method="PUT",
        )


def test_upload_file():
    file_name = "some_file.tsv"
    response = request_util.upload_file(
        uri=f"{base_url}/post",
        data={"file": file_name},
    )
    res_json = json.loads(response)
    assert res_json["files"]["file"] == file_name
