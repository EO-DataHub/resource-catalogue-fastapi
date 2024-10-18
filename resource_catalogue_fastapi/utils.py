import logging
import os
import time
import urllib.request
from distutils.util import strtobool
from urllib.parse import urlparse
from urllib.request import urlopen

import boto3
import jwt
import requests
from botocore.exceptions import ClientError
from fastapi import Depends, HTTPException, Request

logger = logging.getLogger(__name__)  # Add this line to define the logger


def get_path_params(request: Request):
    logger.debug("CALCULATING PATH PARAMETERS")
    return request.scope.get("path_params", {})


def opa_request(input_data: dict, opa_service_endpoint: str) -> requests.Response:
    """Send a request to OPA service"""
    logger.debug(f"Sending request to OPA service {opa_service_endpoint} with input: {input_data}")
    return requests.post(opa_service_endpoint, json=input_data)


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
        username = credentials.get("preferred_username", "")
        roles = credentials.get("realm_access", {}).get("roles", [])

    else:
        username = ""
        roles = []
    return username, roles


def check_policy(
    request: Request,
    path_params: dict,
    opa_service_endpoint: str,
    workspaces_domain: str,
) -> bool:
    """Check OPA policy to determine if the request is allowed"""
    username, roles = get_user_details(request)

    # Add values for logs
    logger.info("Logged in as user: %s", username)
    logger.info("Roles: %s", roles)

    if not (workspace := path_params.get("workspace")):
        return False

    input_data = {
        "input": {
            "roles": roles,
            "host": f"{workspace}.{workspaces_domain}",
            "username": username,
            "method": request.method,
        }
    }

    response = opa_request(input_data, opa_service_endpoint)
    logger.debug(f"OPA response: {response.json()}")
    if response.status_code != 200 or not response.json().get("result", False):
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


def upload_file_s3(body: str, bucket: str, key: str):
    """Upload data to an S3 bucket."""
    try:
        s3_client = boto3.client("s3")
        s3_client.put_object(Body=body, Bucket=bucket, Key=key)
    except ClientError as e:
        logging.error(f"File upload failed: {e}")
        raise


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
    """Obtain all nesteed files to upload to a workspace from a url"""
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
