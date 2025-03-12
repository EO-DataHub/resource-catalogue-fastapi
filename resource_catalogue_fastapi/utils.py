import json
import logging
import os
import time
import urllib.request
from distutils.util import strtobool
from typing import List, Optional
from urllib.parse import urlparse
from urllib.request import urlopen

import boto3
import jwt
import requests
from botocore.exceptions import ClientError
from fastapi import Depends, HTTPException, Request

logger = logging.getLogger(__name__)  # Add this line to define the logger

ADES_URL = os.getenv("ADES_URL")
WORKSPACES_CLAIM_PATH = os.getenv("WORKSPACES_CLAIM_PATH", "workspaces")


def get_path_params(request: Request):
    logger.debug("CALCULATING PATH PARAMETERS")
    return request.scope.get("path_params", {})


async def get_body_params(request: Request) -> Optional[dict]:
    try:
        body = await request.json()
        return body
    except Exception as e:
        logger.error(f"Error parsing request body: {e}")
        return None


def get_nested_value(data: dict, path: str, default=None):
    """Retrieve a nested value from a dictionary using a dot-separated path."""
    keys = path.split(".")
    for key in keys:
        if isinstance(data, dict) and key in data:
            data = data[key]
        else:
            return default
    return data


def get_user_details(request: Request) -> tuple:
    """Get user details from the request"""
    token = request.headers.get("authorization", "")
    stripped_token = token.replace("Bearer ", "")
    if stripped_token:
        credentials = jwt.decode(
            stripped_token,
            options={"verify_signature": False},
            algorithms=["HS256"],
        )
        logging.debug(f"Credentials: {credentials}")
        username = credentials.get("preferred_username", "")
        workspaces = get_nested_value(credentials, WORKSPACES_CLAIM_PATH, [])

    else:
        username = ""
        workspaces = []
    return username, workspaces


async def validate_workspace_access(
    request: Request,
    path_params: dict,
) -> bool:
    """Check if the user has access to the workspace"""
    username, workspaces = get_user_details(request)

    # Add values for logs
    logger.info("Logged in as user: %s", username)
    logger.info("Workspaces: %s", workspaces)

    if not (workspace := path_params.get("workspace")):
        body_params = await get_body_params(request)
        logger.debug(f"Checking body params for workspace: {body_params}")
        if body_params:
            workspace = body_params.get("workspace")

    if not workspace:
        logger.info("No workspace found in the request")
        return False

    logger.info(f"Workspace: {workspace}")
    if workspace not in workspaces:
        return False
    return True


last_deploy_times = {}


# Dependency to check the last call time
def rate_limit(deploy_workspace: str):
    current_time = time.time()
    print(last_deploy_times.get(deploy_workspace, 0))
    last_call_time = last_deploy_times.get(deploy_workspace, 0)
    if current_time - last_call_time < 5:
        raise HTTPException(
            status_code=429, detail="Too Many Requests: Please wait 5 seconds before retrying."
        )
    last_deploy_times[deploy_workspace] = current_time


def get_workspace(request: Request) -> str:
    return request.path_params["workspace"]


def rate_limiter_dependency(workspace=Depends(get_workspace)):  # noqa: B008
    if strtobool(os.getenv("ENABLE_RATE_LIMIT", "false")):
        rate_limit(workspace)


def upload_file_s3(body: str, bucket: str, key: str) -> bool:
    """Upload data to an S3 bucket. Returns a bool indicating whether the file existed previously"""
    s3_client = boto3.client("s3")
    file_exists = False

    try:
        s3_client.put_object(Body=body, Bucket=bucket, Key=key)
    except ClientError as e:
        logging.error(f"File upload failed: {e}")
        raise

    return file_exists


def delete_file_s3(bucket: str, key: str):
    """Delete a file from an S3 bucket."""
    try:
        s3_client = boto3.client("s3")
        s3_client.delete_object(Bucket=bucket, Key=key)
    except ClientError as e:
        logging.error(f"File deletion failed: {e}")
        raise


def get_file_from_url(url: str, retries: int = 0) -> str:
    """Returns contents of data available at given URL"""
    if retries == 3:
        # Max number of retries
        return None
    try:
        with urlopen(url, timeout=5) as response:
            body = response.read()
    except urllib.error.URLError:
        logging.error(f"Unable to access {url}, retrying...")
        return get_file_from_url(url, retries + 1)
    return body.decode("utf-8")


def get_nested_files_from_url(url: str) -> list:
    """Obtain all nested files to upload to a workspace from a url"""
    files_to_add = [url]

    parsed_url = urlparse(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    path_parts = parsed_url.path.split("/")

    # Obtain the collection if it exists
    try:
        collections_index = path_parts.index("collections")
    except ValueError:
        return files_to_add
    collection_path = "/".join(path_parts[: collections_index + 2])

    # Add collection to the start of the list so it is uploaded first
    files_to_add.insert(0, f"{base_url}{collection_path}")
    return files_to_add


def update_stac_order_status(stac_item: dict, item_id: str, order_status: str):
    """Update the STAC item with the order status using the STAC Order extension"""
    # Update or add fields relating to the order
    if "properties" not in stac_item:
        stac_item["properties"] = {}

    if item_id is not None:
        stac_item["properties"]["order.id"] = item_id
    stac_item["properties"]["order.status"] = order_status

    # Update or add the STAC extension if not already present
    order_extension_url = "https://stac-extensions.github.io/order/v1.1.0/schema.json"
    if "stac_extensions" not in stac_item:
        stac_item["stac_extensions"] = []

    if order_extension_url not in stac_item["stac_extensions"]:
        stac_item["stac_extensions"].append(order_extension_url)


def execute_order_workflow(
    provider_workspace: str,
    user_workspace: str,
    workflow_name: str,
    authorization: str,
    stac_uri: str,
    commercial_data_bucket: str,
    product_bundle: str,
    coordinates: list,
    end_users: Optional[List],
):
    """Executes a data adaptor workflow in the provider's workspace as the given user with auth"""

    url = f"{ADES_URL}/{provider_workspace}/ogc-api/processes/{workflow_name}/execution"
    headers = {
        "Authorization": authorization,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Prefer": "respond-async",
    }
    logger.info(f"Executing workflow {workflow_name} for user {user_workspace}")
    payload = {
        "inputs": {
            "workspace": user_workspace,
            "commercial_data_bucket": commercial_data_bucket,
            "product_bundle": product_bundle,
            "stac_key": stac_uri,
        }
    }
    if coordinates:
        payload["inputs"]["coordinates"] = str(coordinates)
    else:
        payload["inputs"]["coordinates"] = "[]"
    if end_users is not None:
        payload["inputs"]["end_users"] = json.dumps(end_users)

    logger.info(f"Sending request to {url} with payload: {payload}")

    # TODO: Remove
    raise NotImplementedError("Sending of request is halted for debugging purposes")

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()
