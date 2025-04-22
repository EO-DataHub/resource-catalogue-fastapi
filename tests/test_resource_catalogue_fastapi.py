import base64
import json
import os
from unittest.mock import MagicMock, patch

import pytest
import requests
from fastapi.testclient import TestClient

from resource_catalogue_fastapi import (
    OrderableCatalogue,
    OrderablePlanetCollection,
    QuoteRequest,
    app,
    quote,
)

client = TestClient(app)

# Fake Successful Contracts
fake_contracts = {
    "sar": True,
    "optical": {"C12345": "My LEGACY Contract", "C67890": "My PNEO Contract"},
}
fake_contracts_b64 = base64.b64encode(json.dumps(fake_contracts).encode()).decode()


@pytest.fixture(autouse=True)
def setenvvar(monkeypatch):
    with patch.dict(os.environ, clear=True):
        envvars = {
            "PLANET_BASE_URL": "https://url.com/planet",
        }
        for k, v in envvars.items():
            monkeypatch.setenv(k, v)
        yield


@patch("resource_catalogue_fastapi.upload_file_s3")
@patch("resource_catalogue_fastapi.get_file_from_url")
@patch("resource_catalogue_fastapi.pulsar_client.create_producer")
def test_create_item_success(mock_create_producer, mock_get_file_from_url, mock_upload_file_s3):
    # Mock the dependencies
    mock_get_file_from_url.return_value = b"file content"
    mock_producer = MagicMock()
    mock_create_producer.return_value = mock_producer

    # Define the request payload
    payload = {"url": "http://example.com/file.json"}

    # Send the request
    response = client.post("/manage/catalogs/user-datasets/test-workspace", json=payload)

    # Assertions
    assert response.status_code == 200
    assert response.json() == {"message": "Item created successfully"}

    # Verify interactions with mocks
    mock_get_file_from_url.assert_called_once_with("http://example.com/file.json")
    mock_upload_file_s3.assert_called_once_with(
        b"file content", "test-bucket", "test-workspace/saved-data/file.json"
    )
    mock_create_producer.assert_called_once_with(
        topic="transformed", producer_name="resource_catalogue_fastapi"
    )
    mock_producer.send.assert_called_once()


@patch("resource_catalogue_fastapi.delete_file_s3")
def test_delete_item_success(mock_delete_file_s3):
    # Define the request payload
    payload = {"url": "http://example.com/file.json"}

    # Send the request
    response = client.request(
        "DELETE", "/manage/catalogs/user-datasets/test-workspace", json=payload
    )

    # Assertions
    assert response.status_code == 200
    assert response.json() == {"message": "Item deleted successfully"}

    # Verify interactions with mocks
    mock_delete_file_s3.assert_called_once_with(
        "test-bucket", "test-workspace/saved-data/file.json"
    )


@patch("resource_catalogue_fastapi.upload_file_s3")
@patch("resource_catalogue_fastapi.get_file_from_url")
def test_update_item_success(mock_get_file_from_url, mock_upload_file_s3):
    # Mock the dependencies
    mock_get_file_from_url.return_value = b"file content"

    # Define the request payload
    payload = {"url": "http://example.com/file.json"}

    # Send the request
    response = client.put("/manage/catalogs/user-datasets/test-workspace", json=payload)

    # Assertions
    assert response.status_code == 200
    assert response.json() == {"message": "Item updated successfully"}

    # Verify interactions with mocks
    mock_get_file_from_url.assert_called_once_with("http://example.com/file.json")
    mock_upload_file_s3.assert_called_once_with(
        b"file content", "test-bucket", "test-workspace/saved-data/file.json"
    )


@patch("resource_catalogue_fastapi.utils.upload_file_s3")
@patch("resource_catalogue_fastapi.get_file_from_url")
@patch("resource_catalogue_fastapi.utils.requests.post")
@patch("resource_catalogue_fastapi.utils.requests.get")
@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.get_contract_id")
@patch("resource_catalogue_fastapi.get_api_key")
@patch("kubernetes.config.load_incluster_config")
@patch("resource_catalogue_fastapi.get_user_details")
@patch("resource_catalogue_fastapi.pulsar_client.create_producer")
def test_order_item_success_airbus_sar(
    mock_create_producer,
    mock_get_user_details,
    mock_load_incluster_config,
    mock_get_api_key,
    mock_get_contract_id,
    mock_get_request,
    mock_post_request,
    mock_get_file_from_url,
    mock_upload_file_s3,
):

    # Mock the dependencies
    mock_load_incluster_config.return_value = None
    mock_get_api_key.return_value = "test-api-key"
    mock_get_contract_id.return_value = ("my-contract-id", None)
    mock_get_file_from_url.return_value = b'{"stac_item": "data"}'
    mock_producer = MagicMock()
    mock_create_producer.return_value = mock_producer
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "links": [
            {"href": "http://example.com/collection", "rel": "collection"},
            {"href": "http://example.com/catalog", "rel": "parent"},
        ]
    }
    mock_response.raise_for_status = MagicMock()
    mock_post_request.return_value = mock_response
    mock_get_request.return_value = mock_response
    mock_get_user_details.return_value = ("test_user", ["test_workspace"])

    # Define the request payload
    payload = {
        "productBundle": "SSC",
        "licence": "Single User Licence",
        "radarOptions": {"orbit": "rapid"},
    }

    # Send the request
    response = client.post(
        "/stac/catalogs/commercial/catalogs/airbus/collections/airbus_sar_data/items/file/order",
        json=payload,
    )

    # Assertions
    assert response.status_code == 201
    assert response.json().get("properties").get("order:status") == "pending"

    # Check that one of the calls has the first argument's dict with 'order:status' set to 'pending'
    found = False
    for call_args in mock_upload_file_s3.call_args_list:
        data = json.loads(call_args[0][0])
        if data.get("properties", {}).get("order:status") == "pending":
            properties = data.get("properties", {})
            assert properties.get("created")
            assert properties.get("updated")
            order_options = properties.get("order_options")
            assert order_options.get("endUser").get("endUserName") == "test_user"
            assert order_options.get("licence") == "Single User License"
            assert order_options.get("productBundle") == "SSC"
            assert order_options.get("radarOptions").get("orbit") == "rapid"
            found = True
            break

    assert found, "No call to upload_file_s3 with 'order:status' set to 'pending' found"
    mock_post_request.assert_called_once()


@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.get_contract_id")
@patch("resource_catalogue_fastapi.get_api_key")
@patch("kubernetes.config.load_incluster_config")
@patch("resource_catalogue_fastapi.utils.upload_file_s3")
@patch("resource_catalogue_fastapi.get_file_from_url")
@patch("resource_catalogue_fastapi.utils.requests.post")
@patch("resource_catalogue_fastapi.utils.requests.get")
@patch("resource_catalogue_fastapi.get_user_details")
@patch("resource_catalogue_fastapi.pulsar_client.create_producer")
def test_order_item_success_airbus_phr(
    mock_create_producer,
    mock_get_user_details,
    mock_get_request,
    mock_post_request,
    mock_get_file_from_url,
    mock_upload_file_s3,
    mock_load_incluster_config,
    mock_get_api_key,
    mock_get_contract_id,
):
    # Mock the dependencies
    mock_get_file_from_url.return_value = b'{"stac_item": "data"}'
    mock_producer = MagicMock()
    mock_create_producer.return_value = mock_producer
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "links": [
            {"href": "http://example.com/collection", "rel": "collection"},
            {"href": "http://example.com/catalog", "rel": "parent"},
        ]
    }
    mock_response.raise_for_status = MagicMock()
    mock_post_request.return_value = mock_response
    mock_get_request.return_value = mock_response
    mock_get_user_details.return_value = ("test_user", ["test_workspace"])

    mock_load_incluster_config.return_value = None
    mock_get_api_key.return_value = "test-api-key"
    mock_get_contract_id.return_value = ("test-contract-id", None)

    # Define the request payload
    payload = {
        "productBundle": "General use",
        "licence": "Standard",
    }

    # Send the request
    response = client.post(
        "/stac/catalogs/commercial/catalogs/airbus/collections/airbus_phr_data/items/file/order",
        json=payload,
    )

    # Assertions
    assert response.status_code == 201
    assert response.json().get("properties").get("order:status") == "pending"

    # Check that one of the calls has the first argument's dict with 'order:status' set to 'pending'
    found = False
    for call_args in mock_upload_file_s3.call_args_list:
        data = json.loads(call_args[0][0])
        if data.get("properties", {}).get("order:status") == "pending":
            properties = data.get("properties", {})
            assert properties.get("created")
            assert properties.get("updated")
            order_options = properties.get("order_options")
            assert order_options.get("endUser").get("endUserName") == "test_user"
            assert order_options.get("licence") == "standard"
            assert order_options.get("productBundle") == "General use"
            found = True
            break

    assert found, "No call to upload_file_s3 with 'order:status' set to 'pending' found"
    mock_post_request.assert_called_once()


@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.get_contract_id")
@patch("resource_catalogue_fastapi.get_api_key")
@patch("kubernetes.config.load_incluster_config")
@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.validate_country_code")
@patch("resource_catalogue_fastapi.utils.upload_file_s3")
@patch("resource_catalogue_fastapi.get_file_from_url")
@patch("resource_catalogue_fastapi.utils.requests.post")
@patch("resource_catalogue_fastapi.utils.requests.get")
@patch("resource_catalogue_fastapi.get_user_details")
@patch("resource_catalogue_fastapi.pulsar_client.create_producer")
def test_order_item_success_airbus_pneo(
    mock_create_producer,
    mock_get_user_details,
    mock_get_request,
    mock_post_request,
    mock_get_file_from_url,
    mock_upload_file_s3,
    mock_validate_country_code,
    mock_load_incluster_config,
    mock_get_api_key,
    mock_get_contract_id,
):
    # Mock the dependencies
    mock_get_file_from_url.return_value = b'{"stac_item": "data"}'
    mock_producer = MagicMock()
    mock_create_producer.return_value = mock_producer
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "links": [
            {"href": "http://example.com/collection", "rel": "collection"},
            {"href": "http://example.com/catalog", "rel": "parent"},
        ]
    }
    mock_response.raise_for_status = MagicMock()
    mock_post_request.return_value = mock_response
    mock_get_request.return_value = mock_response
    mock_get_user_details.return_value = ("test_user", ["test_workspace"])
    mock_validate_country_code.return_value = None

    mock_load_incluster_config.return_value = None
    mock_get_api_key.return_value = "test-api-key"
    mock_get_contract_id.return_value = ("test-contract-id", None)

    # Define the request payload
    payload = {
        "productBundle": "Visual",
        "licence": "Standard Multi End-Users (>30)",
        "endUserCountry": "GB",
    }

    # Send the request
    response = client.post(
        "/stac/catalogs/commercial/catalogs/airbus/collections/airbus_pneo_data/items/file/order",
        json=payload,
    )

    # Assertions
    assert response.status_code == 201
    assert response.json().get("properties").get("order:status") == "pending"

    # Check that one of the calls has the first argument's dict with 'order:status' set to 'pending'
    found = False
    for call_args in mock_upload_file_s3.call_args_list:
        data = json.loads(call_args[0][0])
        if data.get("properties", {}).get("order:status") == "pending":
            properties = data.get("properties", {})
            assert properties.get("created")
            assert properties.get("updated")
            order_options = properties.get("order_options")
            assert order_options.get("endUser").get("endUserName") == "test_user"
            assert order_options.get("endUser").get("country") == "GB"
            assert order_options.get("licence") == "standard_up_30"
            assert order_options.get("productBundle") == "Visual"
            found = True
            break

    assert found, "No call to upload_file_s3 with 'order:status' set to 'pending' found"
    mock_post_request.assert_called_once()


@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.get_contract_id")
@patch("resource_catalogue_fastapi.get_api_key")
@patch("kubernetes.config.load_incluster_config")
@patch("resource_catalogue_fastapi.utils.upload_file_s3")
@patch("resource_catalogue_fastapi.get_file_from_url")
@patch("resource_catalogue_fastapi.utils.requests.post")
@patch("resource_catalogue_fastapi.utils.requests.get")
@patch("resource_catalogue_fastapi.get_user_details")
@patch("resource_catalogue_fastapi.pulsar_client.create_producer")
def test_order_item_success_airbus_spot(
    mock_create_producer,
    mock_get_user_details,
    mock_get_request,
    mock_post_request,
    mock_get_file_from_url,
    mock_upload_file_s3,
    mock_load_incluster_config,
    mock_get_api_key,
    mock_get_contract_id,
):
    # Mock the dependencies
    mock_get_file_from_url.return_value = b'{"stac_item": "data"}'
    mock_producer = MagicMock()
    mock_create_producer.return_value = mock_producer
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "links": [
            {"href": "http://example.com/collection", "rel": "collection"},
            {"href": "http://example.com/catalog", "rel": "parent"},
        ]
    }
    mock_response.raise_for_status = MagicMock()
    mock_post_request.return_value = mock_response
    mock_get_request.return_value = mock_response
    mock_get_user_details.return_value = ("test_user", ["test_workspace"])

    mock_load_incluster_config.return_value = None
    mock_get_api_key.return_value = "test-api-key"
    mock_get_contract_id.return_value = ("test-contract-id", None)

    # Define the request payload
    payload = {
        "productBundle": "Analytic",
        "licence": "Academic",
        "coordinates": [[[4, 5], [5, 6], [6, 7]]],
    }

    # Send the request
    response = client.post(
        "/stac/catalogs/commercial/catalogs/airbus/collections/airbus_spot_data/items/file/order",
        json=payload,
    )

    # Assertions
    assert response.status_code == 201
    assert response.json().get("properties").get("order:status") == "pending"

    # Check that one of the calls has the first argument's dict with 'order:status' set to 'pending'
    found = False
    for call_args in mock_upload_file_s3.call_args_list:
        data = json.loads(call_args[0][0])
        if data.get("properties", {}).get("order:status") == "pending":
            properties = data.get("properties", {})
            assert properties.get("created")
            assert properties.get("updated")
            order_options = properties.get("order_options")
            assert order_options.get("endUser").get("endUserName") == "test_user"
            assert order_options.get("licence") == "educ"
            assert order_options.get("productBundle") == "Analytic"
            found = True
            break

    assert found, "No call to upload_file_s3 with 'order:status' set to 'pending' found"
    mock_post_request.assert_called_once()


@patch("resource_catalogue_fastapi.get_api_key")
@patch("kubernetes.config.load_incluster_config")
@patch("resource_catalogue_fastapi.utils.upload_file_s3")
@patch("resource_catalogue_fastapi.get_file_from_url")
@patch("resource_catalogue_fastapi.utils.requests.post")
@patch("resource_catalogue_fastapi.utils.requests.get")
@patch("resource_catalogue_fastapi.get_user_details")
@patch("resource_catalogue_fastapi.pulsar_client.create_producer")
def test_order_item_success_planet(
    mock_create_producer,
    mock_get_user_details,
    mock_get_request,
    mock_post_request,
    mock_get_file_from_url,
    mock_upload_file_s3,
    mock_load_incluster_config,
    mock_get_api_key,
):
    # Mock the dependencies
    mock_get_file_from_url.return_value = b'{"stac_item": "data"}'
    mock_producer = MagicMock()
    mock_create_producer.return_value = mock_producer
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "links": [
            {"href": "http://example.com/collection", "rel": "collection"},
            {"href": "http://example.com/catalog", "rel": "parent"},
        ]
    }
    mock_response.raise_for_status = MagicMock()
    mock_post_request.return_value = mock_response
    mock_get_request.return_value = mock_response
    mock_get_user_details.return_value = ("test_user", ["test_workspace"])

    mock_load_incluster_config.return_value = None
    mock_get_api_key.return_value = "test-api-key"

    # Define the request payload
    payload = {
        "productBundle": "Basic",
    }

    # Send the request
    response = client.post(
        "/stac/catalogs/commercial/catalogs/planet/collections/PSScene/items/file/order",
        json=payload,
    )

    # Assertions
    assert response.status_code == 201
    assert response.json().get("properties").get("order:status") == "pending"

    # Check that one of the calls has the first argument's dict with 'order:status' set to 'pending'
    found = False
    for call_args in mock_upload_file_s3.call_args_list:
        data = json.loads(call_args[0][0])
        if data.get("properties", {}).get("order:status") == "pending":
            properties = data.get("properties", {})
            assert properties.get("created")
            assert properties.get("updated")
            order_options = properties.get("order_options")
            assert order_options.get("endUser").get("endUserName") == "test_user"
            assert order_options.get("productBundle") == "Basic"
            found = True
            break

    assert found, "No call to upload_file_s3 with 'order:status' set to 'pending' found"
    mock_post_request.assert_called_once()


@patch("resource_catalogue_fastapi.get_user_details")
@patch("resource_catalogue_fastapi.pulsar_client.create_producer")
def test_order_item_invalid(
    mock_create_producer,
    mock_get_user_details,
):
    # Mock the dependencies
    mock_producer = MagicMock()
    mock_create_producer.return_value = mock_producer
    mock_get_user_details.return_value = ("test_user", ["test_workspace"])

    # Define the request payload
    payload = {
        "productBundle": "General use",
        "licence": "Invalid licence",
    }

    # Send the request
    response = client.post(
        "/stac/catalogs/commercial/catalogs/airbus/collections/airbus_phr_data/items/file/order",
        json=payload,
    )

    # Assertions
    assert response.status_code == 422


@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.get_contract_id")
@patch("resource_catalogue_fastapi.get_api_key")
@patch("kubernetes.config.load_incluster_config")
@patch("resource_catalogue_fastapi.upload_file_s3")
@patch("resource_catalogue_fastapi.utils.upload_file_s3")
@patch("resource_catalogue_fastapi.utils.requests.post")
@patch("resource_catalogue_fastapi.utils.requests.get")
@patch("resource_catalogue_fastapi.get_user_details")
@patch("resource_catalogue_fastapi.pulsar_client.create_producer")
def test_order_item_failure(
    mock_create_producer,
    mock_get_user_details,
    mock_get_request,
    mock_post_request,
    mock_utils_upload_file_s3,
    mock_upload_file_s3,
    mock_load_incluster_config,
    mock_get_api_key,
    mock_get_contract_id,
):
    # Mock the dependencies
    mock_producer = MagicMock()
    mock_create_producer.return_value = mock_producer
    mock_post_response = MagicMock()
    mock_post_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Mocked error")
    mock_post_request.return_value = mock_post_response
    mock_get_response = MagicMock()
    mock_get_response.json.return_value = {
        "links": [
            {"href": "http://example.com/collection", "rel": "collection"},
            {"href": "http://example.com/catalog", "rel": "parent"},
        ]
    }
    mock_get_response.raise_for_status = MagicMock()
    mock_get_request.return_value = mock_get_response
    mock_get_user_details.return_value = ("test_user", ["test_workspace"])

    mock_load_incluster_config.return_value = None
    mock_get_api_key.return_value = "test-api-key"
    mock_get_contract_id.return_value = ("test-contract-id", None)

    # Define the request payload
    payload = {
        "productBundle": "SSC",
        "licence": "Single User Licence",
        "radarOptions": {"orbit": "rapid"},
    }

    # Send the request
    response = client.post(
        "/stac/catalogs/commercial/catalogs/airbus/collections/airbus_sar_data/items/file/order",
        json=payload,
    )

    # Assertions
    assert response.status_code == 500
    assert response.json() == {"detail": "Error executing order workflow"}

    # Verify interactions with mocks
    # Check that one of the calls has the first argument's dict with 'order:status' set to 'pending'
    found = False
    for call_args in mock_upload_file_s3.call_args_list:
        data = json.loads(call_args[0][0])
        if data.get("properties", {}).get("order:status") == "failed":
            properties = data.get("properties", {})
            assert properties.get("created")
            assert properties.get("updated")
            order_options = properties.get("order_options")
            assert order_options.get("endUser").get("endUserName") == "test_user"
            assert order_options.get("licence") == "Single User License"
            assert order_options.get("productBundle") == "SSC"
            assert order_options.get("radarOptions").get("orbit") == "rapid"
            found = True
            break
    assert found, "No call to upload_file_s3 with 'order:status' set to 'pending' found"
    mock_post_request.assert_called_once()


@patch("resource_catalogue_fastapi.requests.get")
@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.generate_access_token")
def test_fetch_airbus_asset_success(mock_generate_token, mock_requests_get):
    mock_generate_token.return_value = "mocked_access_token"
    mock_item_response = MagicMock()
    mock_item_response.json.return_value = {
        "assets": {"external_thumbnail": {"href": "https://example.com/thumbnail"}}
    }
    mock_item_response.raise_for_status = MagicMock()
    mock_requests_get.side_effect = [
        mock_item_response,
        MagicMock(content=b"image data", headers={"Content-Type": "image/jpeg"}),
    ]

    response = client.get(
        "/stac/catalogs/supported-datasets/airbus/collections/collection/items/item/thumbnail"
    )

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "image/jpeg"
    assert response.content == b"image data"

    # Verify interactions with mocks
    mock_generate_token.assert_called_once_with()
    mock_requests_get.assert_any_call(
        "https://example.com/thumbnail", headers={"Authorization": "Bearer mocked_access_token"}
    )


@patch("resource_catalogue_fastapi.requests.get")
@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.generate_access_token")
def test_fetch_airbus_asset_not_found(mock_generate_token, mock_requests_get):
    mock_generate_token.return_value = "mocked_access_token"
    mock_item_response = MagicMock()
    mock_item_response.json.return_value = {"assets": {}}
    mock_item_response.raise_for_status = MagicMock()
    mock_requests_get.side_effect = [mock_item_response]

    response = client.get(
        "/stac/catalogs/supported-datasets/airbus/collections/collection/items/item/thumbnail"
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "External thumbnail link not found in item"}

    # Verify interactions with mocks
    mock_requests_get.assert_called_once_with(
        "https://dev.eodatahub.org.uk/api/catalogue/stac/catalogs/supported-datasets/catalogs/airbus/collections/collection/items/item"
    )


@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.get_contract_id")
@patch("resource_catalogue_fastapi.get_api_key")
@patch("kubernetes.config.load_incluster_config")
@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.get_quote_from_airbus")
@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.generate_access_token")
@patch("resource_catalogue_fastapi.get_user_details")
def test_quote__airbus_sar(
    mock_get_user_details,
    mock_generate_airbus_access_token,
    mock_get_quote_from_airbus,
    mock_load_incluster_config,
    mock_get_api_key,
    mock_get_contract_id,
):
    result = {"units": "EUR", "value": 100}
    mock_get_quote_from_airbus.return_value = [
        {"acquisitionId": "otherId", "price": 200},
        {"acquisitionId": "12345", "price": {"currency": "EUR", "total": 100}},
    ]
    mock_generate_airbus_access_token.return_value = "valid_token"
    mock_get_user_details.return_value = ("test_user", ["test_workspace"])

    mock_load_incluster_config.return_value = None
    mock_get_api_key.return_value = "test-api-key"
    mock_get_contract_id.return_value = ("test-contract-id", None)

    headers = {"authorization": "Bearer valid_token", "accept": "application/json"}
    response = client.post(
        "stac/catalogs/commercial/catalogs/airbus/collections/airbus_sar_data/items/12345/quote",
        headers=headers,
        json={"licence": "Single User Licence"},
    )

    assert response.status_code == 200
    assert response.json() == result


@patch("resource_catalogue_fastapi.requests.get")
@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.get_quote_from_airbus")
@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.generate_access_token")
@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.get_contract_id")
@patch("resource_catalogue_fastapi.get_user_details")
@patch("resource_catalogue_fastapi.get_api_key")
@patch("kubernetes.config.load_incluster_config")
def test_quote__airbus_optical(
    mock_load_incluster_config,
    mock_get_api_key,
    mock_get_user_details,
    mock_get_contract_id,
    mock_generate_airbus_access_token,
    mock_get_quote_from_airbus,
    mock_get_request,
):
    result = {"units": "EUR", "value": 100}

    mock_load_incluster_config.return_value = None
    mock_get_api_key.return_value = "test-api-key"
    mock_get_contract_id.return_value = ("my-contract-id", None)

    mock_get_quote_from_airbus.return_value = {"currency": "EUR", "totalAmount": 100}
    mock_generate_airbus_access_token.return_value = "valid_token"

    mock_get_user_details.return_value = ("test_user", ["test_workspace"])

    mock_response = MagicMock()
    mock_response.json.return_value = {"geometry": {"coordinates": [[[4, 5], [5, 6], [6, 7]]]}}
    mock_response.raise_for_status = MagicMock()
    mock_get_request.return_value = mock_response
    headers = {"authorization": "Bearer valid_token", "accept": "application/json"}
    response = client.post(
        "stac/catalogs/commercial/catalogs/airbus/collections/airbus_spot_data/items/12345/quote",
        headers=headers,
        json={"licence": "Standard"},
    )
    assert response.status_code == 200
    assert response.json() == result
    assert len(mock_get_quote_from_airbus.call_args[0][1]["items"][0]["datastripIds"]) == 1


@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.get_contract_id")
@patch("resource_catalogue_fastapi.get_api_key")
@patch("kubernetes.config.load_incluster_config")
@patch("resource_catalogue_fastapi.requests.get")
@patch("resource_catalogue_fastapi.get_user_details")
def test_quote__airbus_optical_multi_order(
    mock_get_user_details,
    mock_get_request,
    mock_load_incluster_config,
    mock_get_api_key,
    mock_get_contract_id,
):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "properties": {"composed_of_acquisition_identifiers": [12345, 67890]}
    }

    mock_load_incluster_config.return_value = None
    mock_get_api_key.return_value = "test-api-key"
    mock_get_contract_id.return_value = ("test-contract-id", None)

    mock_get_user_details.return_value = ("test_user", ["test_workspace"])

    mock_response.raise_for_status = MagicMock()
    mock_get_request.return_value = mock_response
    headers = {"authorization": "Bearer valid_token", "accept": "application/json"}
    response = client.post(
        "stac/catalogs/commercial/catalogs/airbus/collections/airbus_pneo_data/items/12345/quote",
        headers=headers,
        json={
            "licence": "Standard",
        },
    )
    assert response.status_code == 500


@patch("resource_catalogue_fastapi.planet_client.PlanetClient.get_area_estimate")
@patch("resource_catalogue_fastapi.get_user_details")
@patch("resource_catalogue_fastapi.get_api_key")
def test_quote__planet(
    mock_get_api_key,
    mock_get_user_details,
    mock_get_quote_from_planet,
):

    mock_get_user_details.return_value = ("test_user", ["test_workspace"])

    mock_get_api_key.return_value = "test-api-key"
    mock_get_quote_from_planet.return_value = 34
    headers = {"authorization": "Bearer valid_token", "accept": "application/json"}
    response = client.post(
        "stac/catalogs/commercial/catalogs/planet/collections/PSScene/items/12345/quote",
        headers=headers,
        json={},
    )
    assert response.status_code == 200
    assert response.json() == {"value": 34, "units": "km2"}
    mock_get_quote_from_planet.assert_called_once_with("12345", "PSScene", None)


@patch("resource_catalogue_fastapi.get_api_key")
@patch("kubernetes.config.load_incluster_config")
@patch("kubernetes.client.CoreV1Api.read_namespaced_secret")
@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.generate_access_token")
@patch("resource_catalogue_fastapi.get_user_details")
def test_quote_invalid_token(
    mock_get_user_details,
    mock_generate_access_token,
    mock_read_secret,
    mock_load_incluster_config,
    mock_get_api_key,
):
    mock_load_incluster_config.return_value = None
    mock_generate_access_token.return_value = None
    mock_read_secret.return_value = MagicMock(data={"dummy": "ZGF0YQ=="})  # base64 encoded 'data'

    mock_get_user_details.return_value = ("test_user", ["test_workspace"])

    mock_get_api_key.return_value = "test-api-key"

    headers = {"authorization": "Bearer valid_token", "accept": "application/json"}
    response = client.post(
        "stac/catalogs/commercial/catalogs/airbus/collections/airbus_sar_data/items/12345/quote",
        headers=headers,
        json={"licence": "Single User Licence"},
    )
    assert response.status_code == 500


@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.get_contract_id")
@patch("resource_catalogue_fastapi.get_api_key")
@patch("kubernetes.config.load_incluster_config")
@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.get_quote_from_airbus")
@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.generate_access_token")
@patch("resource_catalogue_fastapi.get_user_details")
def test_quote_id_not_matched(
    mock_get_user_details,
    mock_generate_access_token,
    mock_get_quote_from_airbus,
    mock_load_incluster_config,
    mock_get_api_key,
    mock_get_contract_id,
):
    mock_get_quote_from_airbus.return_value = [{"acquisitionId": "otherId", "price": 200}]
    mock_generate_access_token.return_value = "valid_token"
    mock_get_user_details.return_value = ("test_user", ["test_workspace"])

    mock_load_incluster_config.return_value = None
    mock_get_api_key.return_value = "test-api-key"
    mock_get_contract_id.return_value = ("test-contract-id", None)

    headers = {"authorization": "Bearer valid_token", "accept": "application/json"}
    response = client.post(
        "stac/catalogs/commercial/catalogs/airbus/collections/airbus_sar_data/items/12345/quote",
        headers=headers,
        json={"licence": "Single User Licence"},
    )
    assert response.status_code == 404


@patch("resource_catalogue_fastapi.get_user_details")
@patch("resource_catalogue_fastapi.get_api_key")
def test_quote_from_planet(mock_get_api_key, mock_get_user_details, requests_mock):
    expected = {"value": 34, "units": "km2", "message": None}

    mock_get_user_details.return_value = ("test_user", ["test_workspace"])

    mock_get_api_key.return_value = "test-api-key"

    mock_planet_response = {"geometry": {"coordinates": [[[4, 5], [5, 6], [6, 7]]]}}

    requests_mock.get(
        "https://url.com/planet/collections/PSScene/items/test_id",
        json=mock_planet_response,
    )

    body = QuoteRequest(**{})
    request = MagicMock()
    request.url = "https://url.com"

    response = quote(
        request,
        None,
        OrderableCatalogue("planet"),
        OrderablePlanetCollection("PSScene"),
        "test_id",
        body,
    )

    assert response.__dict__ == expected


def test_thumbnail_airbus_collection():
    response = client.get(
        "stac/catalogs/commercial/catalogs/airbus/collections/airbus_sar_data/thumbnail",
    )
    assert response.status_code == 200
    assert response.headers["Content-Type"] == "image/jpeg"
