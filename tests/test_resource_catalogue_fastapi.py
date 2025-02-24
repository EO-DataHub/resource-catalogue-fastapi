import os
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

    test_user_catalogue = os.getenv("USER_CATALOGUE_ID", "user")

    print(f"/manage/catalogs/{test_user_catalogue}/catalogs/test-workspace")

    # Send the request
    response = client.post(
        f"/manage/catalogs/{test_user_catalogue}/catalogs/test-workspace", json=payload
    )

    # Assertions
    assert response.status_code == 200
    assert response.json() == {"message": "Item created successfully"}

    # Verify interactions with mocks
    mock_get_file_from_url.assert_called_once_with("http://example.com/file.json")
    mock_upload_file_s3.assert_called_once_with(
        b"file content", "test-bucket", "test-workspace/saved-data/file.json"
    )
    mock_create_producer.assert_called_once_with(
        topic="harvested", producer_name="resource_catalogue_fastapi"
    )
    mock_producer.send.assert_called_once()


@patch("resource_catalogue_fastapi.delete_file_s3")
def test_delete_item_success(mock_delete_file_s3):
    # Define the request payload
    payload = {"url": "http://example.com/file.json"}

    test_user_catalogue = os.getenv("USER_CATALOGUE_ID", "user")

    # Send the request
    response = client.request(
        "DELETE", f"/manage/catalogs/{test_user_catalogue}/catalogs/test-workspace", json=payload
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

    test_user_catalogue = os.getenv("USER_CATALOGUE_ID", "user")

    # Send the request
    response = client.put(
        f"/manage/catalogs/{test_user_catalogue}/catalogs/test-workspace", json=payload
    )

    # Assertions
    assert response.status_code == 200
    assert response.json() == {"message": "Item updated successfully"}

    # Verify interactions with mocks
    mock_get_file_from_url.assert_called_once_with("http://example.com/file.json")
    mock_upload_file_s3.assert_called_once_with(
        b"file content", "test-bucket", "test-workspace/saved-data/file.json"
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

    test_rc_root = os.getenv("RC_ROOT", "api/catalogue/stac")
    test_catalogue_id = os.getenv("COMMERCIAL_CATALOGUE_ID", "commercial")

    url = f"http://example.com/{test_rc_root}/catalogs/{test_catalogue_id}/catalogs/airbus/collections/airbus_sar_data/items/file.json"
    # Define the request payload
    payload = {"url": url, "product_bundle": "bundle"}

    test_user_catalogue = os.getenv("USER_CATALOGUE_ID", "user")

    # Send the request
    response = client.post(
        f"/manage/catalogs/{test_user_catalogue}/catalogs/test-workspace/catalogs/commercial-data",
        json=payload,
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
                "test-workspace/commercial-data/airbus/collections/airbus_sar_data.json",
            ),
            call(
                '{"stac_item": "data", "assets": {}, "properties": {"order.status": "pending"}, '
                '"stac_extensions": ["https://stac-extensions.github.io/order/v1.1.0/schema.json"]}',
                "test-bucket",
                "test-workspace/commercial-data/airbus/collections/airbus_sar_data/items/file.json",
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

    test_rc_root = os.getenv("RC_ROOT", "api/catalogue/stac")
    test_catalogue_id = os.getenv("COMMERCIAL_CATALOGUE_ID", "commercial")

    url = f"http://example.com/{test_rc_root}/catalogs/{test_catalogue_id}/catalogs/airbus/collections/airbus_sar_data/items/file.json"
    # Define the request payload
    payload = {"url": url, "product_bundle": "bundle"}

    test_user_catalogue = os.getenv("USER_CATALOGUE_ID", "user")

    print(
        f"/manage/catalogs/{test_user_catalogue}/catalogs/test-workspace/catalogs/commercial-data"
    )

    # Send the request
    response = client.post(
        f"/manage/catalogs/{test_user_catalogue}/catalogs/test-workspace/catalogs/commercial-data",
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
                "test-workspace/commercial-data/airbus/collections/airbus_sar_data.json",
            ),
            call().__bool__(),
            call(
                '{"stac_item": "data", "assets": {}, "properties": {"order.status": "pending"}, '
                '"stac_extensions": ["https://stac-extensions.github.io/order/v1.1.0/schema.json"]}',
                "test-bucket",
                "test-workspace/commercial-data/airbus/collections/airbus_sar_data/items/file.json",
            ),
            call().__bool__(),
            call(
                '{"stac_item": "data", "properties": {"order.status": "failed"}, '
                '"stac_extensions": ["https://stac-extensions.github.io/order/v1.1.0/schema.json"]}',
                "test-bucket",
                "test-workspace/commercial-data/airbus/collections/airbus_sar_data/items/file.json",
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

    test_catalogue_id = os.getenv("COMMERCIAL_CATALOGUE_ID", "commercial")

    response = client.get(
        f"/stac/catalogs/{test_catalogue_id}/catalogs/airbus/collections/collection/items/item/thumbnail"
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

    test_rc_root = os.getenv("RC_ROOT", "api/catalogue/stac")
    test_catalogue_id = os.getenv("COMMERCIAL_CATALOGUE_ID", "commercial")

    print(
        f"/stac/catalogs/{test_catalogue_id}/catalogs/airbus/collections/collection/items/item/thumbnail"
    )

    response = client.get(
        f"/stac/catalogs/{test_catalogue_id}/catalogs/airbus/collections/collection/items/item/thumbnail"
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "External thumbnail link not found in item"}

    # Verify interactions with mocks
    mock_requests_get.assert_called_once_with(
        f"https://dev.eodatahub.org.uk/{test_rc_root}/catalogs/{test_catalogue_id}/catalogs/airbus/collections/collection/items/item"
    )
