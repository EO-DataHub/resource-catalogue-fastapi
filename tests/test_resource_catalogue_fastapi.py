from unittest.mock import MagicMock, patch

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
    response = client.post("/catalogs/user-datasets/test-workspace", json=payload)

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

    # Send the request
    response = client.request("DELETE", "/catalogs/user-datasets/test-workspace", json=payload)

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
    response = client.put("/catalogs/user-datasets/test-workspace", json=payload)

    # Assertions
    assert response.status_code == 200
    assert response.json() == {"message": "Item updated successfully"}

    # Verify interactions with mocks
    mock_get_file_from_url.assert_called_once_with("http://example.com/file.json")
    mock_upload_file_s3.assert_called_once_with(
        b"file content", "test-bucket", "test-workspace/saved-data/file.json"
    )
