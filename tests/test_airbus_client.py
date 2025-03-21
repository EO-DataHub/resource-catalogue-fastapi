import base64
from unittest import mock

import pytest
from fastapi import Request
from requests.models import Response

from resource_catalogue_fastapi.airbus_client import AirbusClient


@pytest.fixture
def airbus_client():
    return AirbusClient(airbus_env="test")


@pytest.fixture
def mock_request():
    return mock.Mock(spec=Request)


@pytest.fixture
def mock_user_details():
    with mock.patch("resource_catalogue_fastapi.utils.get_user_details") as mock_get_user_details:
        mock_get_user_details.return_value = ("test_user", "test_email")
        yield mock_get_user_details


@pytest.fixture
def mock_k8s_client():
    with mock.patch("kubernetes.client.CustomObjectsApi") as mock_api_instance:
        with mock.patch("kubernetes.config.load_incluster_config") as mock_load_incluster_config:
            mock_load_incluster_config.return_value = None
            mock_instance = mock.Mock()
            mock_instance.get_namespaced_custom_object.return_value = {
                "spec": {"namespace": "test-namespace"}
            }
            mock_api_instance.return_value = mock_instance
            yield mock_api_instance


@pytest.fixture
def mock_k8s_core_v1_api():
    with mock.patch("kubernetes.client.CoreV1Api") as mock_core_v1_api:
        mock_instance = mock.Mock()
        mock_instance.read_namespaced_secret.return_value = mock.Mock(
            data={"airbus-key": base64.b64encode(b"test_api_key").decode("utf-8")}
        )
        mock_core_v1_api.return_value = mock_instance
        yield mock_core_v1_api


@pytest.fixture
def mock_requests_post():
    with mock.patch("requests.post") as mock_post:
        mock_response = mock.Mock(spec=Response)
        mock_response.json.return_value = {"access_token": "test_access_token"}
        mock_post.return_value = mock_response
        yield mock_post


def test_get_secret_airbus_api_key(airbus_client, mock_k8s_client, mock_k8s_core_v1_api):
    api_key = airbus_client.get_secret_airbus_api_key("test_user")
    assert api_key == "test_api_key"


def test_generate_access_token(
    airbus_client,
    mock_request,
    mock_user_details,
    mock_k8s_client,
    mock_k8s_core_v1_api,
    mock_requests_post,
):
    token = airbus_client.generate_access_token("test_user")
    assert token == "test_access_token"


def test_get_quote_from_airbus(airbus_client, mock_requests_post):
    url = "https://test-airbus-api.com"
    body = {"acquisitionId": "test_id"}
    headers = {"Authorization": "Bearer test_access_token"}

    mock_response = mock.Mock(spec=Response)
    mock_response.json.return_value = {"quote": "test_quote"}
    mock_requests_post.return_value = mock_response

    quote = airbus_client.get_quote_from_airbus(url, body, headers)
    assert quote == {"quote": "test_quote"}
