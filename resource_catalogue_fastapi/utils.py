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

ADES_URL = os.getenv("ADES_URL")
AIRBUS_API_KEY = os.getenv("AIRBUS_API_KEY")


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


def upload_file_s3(body: str, bucket: str, key: str, error_on_exist: bool = False) -> bool:
    """Upload data to an S3 bucket. Returns a bool indicating whether the file existed previously"""
    s3_client = boto3.client("s3")
    file_exists = False

    try:
        # Check if the file already exists
        s3_client.head_object(Bucket=bucket, Key=key)
        file_exists = True
    except ClientError as e:
        # If a 404 error is raised, the file does not exist
        if e.response["Error"]["Code"] != "404":
            logging.error(f"Error checking if file exists: {e}")
            raise
    if error_on_exist and file_exists:
        logging.error(f"File already exists: {key}")
        raise FileExistsError(f"File already exists: {key}")
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
    stac_key: str,
    workspace_bucket: str,
    workspaces_domain: str,
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
            "stac_key": stac_key,
            "workspace_bucket": workspace_bucket,
            "workspace_domain": workspaces_domain,
            "env": "prod",
        }
    }

    logger.info(f"Sending request to {url} with payload: {payload}")

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()


def generate_airbus_access_token(env: str = "dev") -> str:
    """Generate access token for Airbus API"""
    if env == "prod":
        url = "https://authenticate.foundation.api.oneatlas.airbus.com/auth/realms/IDP/protocol/openid-connect/token"
    else:
        url = "https://authenticate-int.idp.private.geoapi-airbusds.com/auth/realms/IDP/protocol/openid-connect/token"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = [
        ("apikey", AIRBUS_API_KEY),
        ("grant_type", "api_key"),
        ("client_id", "IDP"),
    ]

    response = requests.post(url, headers=headers, data=data)

    return response.json().get("access_token")
