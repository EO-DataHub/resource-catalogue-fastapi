from unittest.mock import MagicMock, call, patch

import requests
from fastapi.testclient import TestClient

from resource_catalogue_fastapi import app

client = TestClient(app)


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
        b"file content", "test-bucket", "test-workspace/saved-data/file.json", False
    )
    mock_create_producer.assert_called_once_with(
        topic="harvested", producer_name="resource_catalogue_fastapi"
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
        b"file content", "test-bucket", "test-workspace/saved-data/file.json", False
    )


@patch("resource_catalogue_fastapi.upload_file_s3")
@patch("resource_catalogue_fastapi.get_file_from_url")
@patch("resource_catalogue_fastapi.utils.requests.post")
@patch("resource_catalogue_fastapi.pulsar_client.create_producer")
def test_order_item_success(
    mock_create_producer, mock_post_request, mock_get_file_from_url, mock_upload_file_s3
):
    # Mock the dependencies
    mock_get_file_from_url.return_value = b'{"stac_item": "data"}'
    mock_producer = MagicMock()
    mock_create_producer.return_value = mock_producer
    mock_response = MagicMock()
    mock_response.json.return_value = {"status": "success"}
    mock_response.raise_for_status = MagicMock()
    mock_post_request.return_value = mock_response

    url = "http://example.com/api/catalogue/stac/catalogs/supported-datasets/airbus/collections/airbus_sar_data/items/file.json"
    # Define the request payload
    payload = {"url": url, "product_bundle": "bundle", "coordinates": [[0, 0], [0, 1], [1, 1]]}

    # Send the request
    response = client.post(
        "/manage/catalogs/user-datasets/test-workspace/commercial-data", json=payload
    )

    # Assertions
    assert response.status_code == 200
    assert response.json() == {"message": "Item ordered successfully"}

    # Verify interactions with mocks
    mock_upload_file_s3.assert_has_calls(
        [
            call(
                b'{"stac_item": "data"}',
                "test-bucket",
                "test-workspace/commercial-data/collections/airbus_sar_data.json",
                False,
            ),
            call(
                '{"stac_item": "data", "assets": {}, "properties": {"order.status": "pending"}, '
                '"stac_extensions": ["https://stac-extensions.github.io/order/v1.1.0/schema.json"]}',
                "test-bucket",
                "test-workspace/commercial-data/collections/airbus_sar_data/items/file.json",
                True,
            ),
        ],
        any_order=True,
    )
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

    url = "http://example.com/api/catalogue/stac/catalogs/supported-datasets/airbus/collections/airbus_sar_data/items/file.json"
    # Define the request payload
    payload = {"url": url, "product_bundle": "bundle", "coordinates": [[0, 0], [0, 1], [1, 1]]}

    # Send the request
    response = client.post(
        "/manage/catalogs/user-datasets/test-workspace/commercial-data", json=payload
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
                "test-workspace/commercial-data/collections/airbus_sar_data.json",
                False,
            ),
            call().__bool__(),
            call(
                '{"stac_item": "data", "assets": {}, "properties": {"order.status": "pending"}, '
                '"stac_extensions": ["https://stac-extensions.github.io/order/v1.1.0/schema.json"]}',
                "test-bucket",
                "test-workspace/commercial-data/collections/airbus_sar_data/items/file.json",
                True,
            ),
            call().__bool__(),
            call(
                '{"stac_item": "data", "properties": {"order.status": "failed"}, '
                '"stac_extensions": ["https://stac-extensions.github.io/order/v1.1.0/schema.json"]}',
                "test-bucket",
                "test-workspace/commercial-data/collections/airbus_sar_data/items/file.json",
            ),
        ],
        any_order=True,
    )
    assert mock_upload_file_s3.call_count == 3
    mock_post_request.assert_called_once()


@patch("resource_catalogue_fastapi.requests.get")
@patch("resource_catalogue_fastapi.generate_airbus_access_token")
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
    mock_generate_token.assert_called_once_with("prod")
    mock_requests_get.assert_any_call(
        "https://example.com/thumbnail", headers={"Authorization": "Bearer mocked_access_token"}
    )


@patch("resource_catalogue_fastapi.requests.get")
@patch("resource_catalogue_fastapi.generate_airbus_access_token")
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
        "https://dev.eodatahub.org.uk/api/catalogue/stac/catalogs/supported-datasets/airbus/collections/collection/items/item"
    )
