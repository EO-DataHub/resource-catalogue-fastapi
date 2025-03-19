import json
import os
from unittest.mock import MagicMock, call, patch

import pytest
import requests
from fastapi.testclient import TestClient

from resource_catalogue_fastapi import QuoteRequest, app, quote

client = TestClient(app)


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
@patch("resource_catalogue_fastapi.get_user_details")
@patch("resource_catalogue_fastapi.pulsar_client.create_producer")
def test_order_item_success(
    mock_create_producer,
    mock_get_user_details,
    mock_get_request,
    mock_post_request,
    mock_get_file_from_url,
    mock_upload_file_s3,
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
    assert response.json().get("properties").get("order.status") == "pending"

    # Check that one of the calls has the first argument's dict with 'order.status' set to 'pending'
    found = False
    for call_args in mock_upload_file_s3.call_args_list:
        data = json.loads(call_args[0][0])
        if data.get("properties", {}).get("order.status") == "pending":
            properties = data.get("properties", {})
            assert properties.get("created")
            assert properties.get("updated")
            order_options = properties.get("order_options")
            assert order_options.get("endUser").get("endUserName") == "test_user"
            assert order_options.get("licence") == "Single User License"
            assert order_options.get("productBundle").get("product_type") == "SSC"
            assert order_options.get("productBundle").get("orbit") == "rapid"
            found = True
            break

    assert found, "No call to upload_file_s3 with 'order.status' set to 'pending' found"
    mock_post_request.assert_called_once()


@patch("resource_catalogue_fastapi.upload_file_s3")
@patch("resource_catalogue_fastapi.get_file_from_url")
@patch("resource_catalogue_fastapi.utils.requests.post")
@patch("resource_catalogue_fastapi.pulsar_client.create_producer")
def test_order_item_failure(
    mock_create_producer, mock_post_request, mock_get_file_from_url, mock_upload_file_s3
):
    # Mock the dependencies
    mock_get_file_from_url.return_value = b'{"stac_item": "data"}'
    mock_producer = MagicMock()
    mock_create_producer.return_value = mock_producer
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Mocked error")
    mock_post_request.return_value = mock_response

    url = "http://testserver/stac/catalogs/commercial/catalogs/airbus/collections/airbus_sar_data/items/file"
    # Define the request payload
    payload = {"workspace": "test-workspace", "product_bundle": "bundle"}

    # Send the request
    response = client.post(
        "/stac/catalogs/commercial/catalogs/airbus/collections/airbus_sar_data/items/file/order",
        json=payload,
    )

    # Assertions
    assert response.status_code == 500
    assert response.json() == {"detail": "Error executing order workflow"}

    # Verify interactions with mocks
    mock_get_file_from_url.assert_called_with(url)
    assert mock_get_file_from_url.call_count == 3
    mock_upload_file_s3.assert_has_calls(
        [
            call(
                b'{"stac_item": "data"}',
                "test-bucket",
                "test-workspace/commercial-data/airbus_sar_data.json",
            ),
            call().__bool__(),
            call(
                '{"stac_item": "data", "assets": {}, "properties": {"order.status": "pending"}, '
                '"stac_extensions": ["https://stac-extensions.github.io/order/v1.1.0/schema.json"]}',
                "test-bucket",
                "test-workspace/commercial-data/airbus_sar_data/items/file.json",
            ),
            call().__bool__(),
            call(
                '{"stac_item": "data", "properties": {"order.status": "failed"}, '
                '"stac_extensions": ["https://stac-extensions.github.io/order/v1.1.0/schema.json"]}',
                "test-bucket",
                "test-workspace/commercial-data/airbus_sar_data/items/file.json",
            ),
        ],
        any_order=True,
    )
    assert mock_upload_file_s3.call_count == 3
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


@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.get_quote_from_airbus")
@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.generate_access_token")
def test_quote__airbus_sar(mock_generate_airbus_access_token, mock_get_quote_from_airbus):
    result = {"units": "EUR", "value": 100}
    mock_get_quote_from_airbus.return_value = [
        {"acquisitionId": "otherId", "price": 200},
        {"acquisitionId": "12345", "price": {"currency": "EUR", "total": 100}},
    ]
    mock_generate_airbus_access_token.return_value = "valid_token"
    headers = {"authorization": "Bearer valid_token", "accept": "application/json"}
    response = client.post(
        "stac/catalogs/commercial/catalogs/airbus/collections/airbus_sar_data/items/12345/quote",
        headers=headers,
        json={},
    )
    assert response.status_code == 200
    assert response.json() == result


@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.get_quote_from_airbus")
@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.generate_access_token")
def test_quote__airbus_optical(mock_generate_airbus_access_token, mock_get_quote_from_airbus):
    result = {"units": "EUR", "value": 100}
    mock_get_quote_from_airbus.return_value = {"currency": "EUR", "totalAmount": 100}
    mock_generate_airbus_access_token.return_value = "valid_token"
    headers = {"authorization": "Bearer valid_token", "accept": "application/json"}
    response = client.post(
        "stac/catalogs/commercial/catalogs/airbus/collections/airbus_spot_data/items/12345/quote",
        headers=headers,
        json={},
    )
    assert response.status_code == 200
    assert response.json() == result
    assert len(mock_get_quote_from_airbus.call_args[0][1]["items"][0]["datastripIds"]) == 1


@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.get_quote_from_airbus")
@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.generate_access_token")
def test_quote__airbus_optical_multi_order(
    mock_generate_airbus_access_token, mock_get_quote_from_airbus
):
    result = {"units": "EUR", "value": 100}
    mock_get_quote_from_airbus.return_value = {"currency": "EUR", "totalAmount": 100}
    mock_generate_airbus_access_token.return_value = "valid_token"
    headers = {"authorization": "Bearer valid_token", "accept": "application/json"}
    response = client.post(
        "stac/catalogs/commercial/catalogs/airbus/collections/airbus_pneo_data/items/12345/quote",
        headers=headers,
        json={
            "itemUuids": ["12345", "67890"],
        },
    )
    assert response.status_code == 200
    assert response.json() == result
    assert len(mock_get_quote_from_airbus.call_args[0][1]["items"][0]["dataSourceIds"]) == 2


@patch("resource_catalogue_fastapi.planet_client.PlanetClient.get_area_estimate")
def test_quote__planet(mock_get_quote_from_planet):
    mock_get_quote_from_planet.return_value = 34
    headers = {"authorization": "Bearer valid_token", "accept": "application/json"}
    response = client.post(
        "stac/catalogs/commercial/catalogs/planet/collections/collection-id/items/12345/quote",
        headers=headers,
        json={},
    )
    mock_get_quote_from_planet.assert_called_once_with("12345", "collection-id", None)
    assert response.status_code == 200
    assert response.json() == {"value": 34, "units": "km2"}


@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.generate_access_token")
def test_quote_invalid_token(mock_generate_access_token):
    mock_generate_access_token.return_value = None
    headers = {"authorization": "Bearer valid_token", "accept": "application/json"}
    response = client.post(
        "stac/catalogs/commercial/catalogs/airbus/collections/airbus_sar_data/items/12345/quote",
        headers=headers,
        json={},
    )
    assert response.status_code == 500


@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.get_quote_from_airbus")
@patch("resource_catalogue_fastapi.airbus_client.AirbusClient.generate_access_token")
def test_quote_id_not_matched(mock_generate_access_token, mock_get_quote_from_airbus):
    mock_get_quote_from_airbus.return_value = [{"acquisitionId": "otherId", "price": 200}]
    mock_generate_access_token.return_value = "valid_token"
    headers = {"authorization": "Bearer valid_token", "accept": "application/json"}
    response = client.post(
        "stac/catalogs/commercial/catalogs/airbus/collections/airbus_sar_data/items/12345/quote",
        headers=headers,
        json={},
    )
    assert response.status_code == 404


def test_quote_from_planet(requests_mock):

    expected = {"value": 34, "units": "km2"}

    mock_planet_response = {"geometry": {"coordinates": [[[4, 5], [5, 6], [6, 7]]]}}

    requests_mock.get(
        "https://url.com/planet/collections/test_collection/items/test_id",
        json=mock_planet_response,
    )

    body = QuoteRequest(**{})

    response = quote(None, "planet", "test_collection", "test_id", body)

    assert response.__dict__ == expected
