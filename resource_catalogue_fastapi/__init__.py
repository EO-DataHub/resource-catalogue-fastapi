import json
import logging
import os
from distutils.util import strtobool

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pulsar import Client as PulsarClient
from pydantic import BaseModel

from .utils import (
    check_policy,
    delete_file_s3,
    get_file_from_url,
    get_nested_files_from_url,
    get_path_params,
    rate_limiter_dependency,
    upload_file_s3,
)

logging.basicConfig(
    level=logging.DEBUG if os.getenv("DEBUG") else logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger(__name__)  # Add this line to define the logger

# Domain for workspaces, used for OPA policy check
WORKSPACES_DOMAIN = os.getenv("WORKSPACES_DOMAIN", "workspaces.dev.eodhp.eco-ke-staging.com")

# OPA service endpoint
OPA_SERVICE_ENDPOINT = os.getenv(
    "OPA_SERVICE_ENDPOINT", "http://opal-client.opal:8181/v1/data/workspaces/allow"
)

# Used for local testing where OPA is not available
ENABLE_OPA_POLICY_CHECK = strtobool(os.getenv("ENABLE_OPA_POLICY_CHECK", "false"))

# S3 bucket to store user data
S3_BUCKET = os.getenv("S3_BUCKET", "test-bucket")

# Root path for FastAPI
RC_FASTAPI_ROOT_PATH = os.getenv("RC_FASTAPI_ROOT_PATH", "/api/catalogue/manage")

# Pulsar client setup
PULSAR_URL = os.environ.get("PULSAR_URL", "pulsar://pulsar-broker.pulsar:6650")
pulsar_client = PulsarClient(PULSAR_URL)
producer = pulsar_client.create_producer(
    topic="harvested", producer_name="resource_catalogue_fastapi"
)


app = FastAPI(
    title="EODHP Resource Catalogue Manager",
    description=(
        "This is the API to manage user catalogues within the resource catalogue for the "
        "EODHP project.\nThe following endpoints are currently implemented."
    ),
    version="0.1.0",
    root_path=RC_FASTAPI_ROOT_PATH,
)

# Define static file path
static_filepath = os.getenv("STATIC_FILE_PATH", "static")

# Mount static files with STAC FastApi
app.mount("/static", StaticFiles(directory=static_filepath), name="static")


def opa_dependency(request: Request, path_params: dict = Depends(get_path_params)):  # noqa: B008
    if ENABLE_OPA_POLICY_CHECK:
        if not check_policy(request, path_params, OPA_SERVICE_ENDPOINT, WORKSPACES_DOMAIN):
            raise HTTPException(status_code=403, detail="Access denied")


class ItemRequest(BaseModel):
    url: str


def upload_nested_files(url: str, workspace: str) -> list:
    """Upload a file along with any nested files to a workspace"""
    files_to_add = get_nested_files_from_url(url)
    keys = []
    for url_to_add in files_to_add:
        logger.info(f"Adding url {url_to_add}")
        body = get_file_from_url(url_to_add)

        workspace_key = f"{workspace}/saved-data/{url_to_add.split('/', 9)[-1]}"
        if not os.path.splitext(workspace_key)[1]:
            workspace_key += ".json"

        logger.info(f"Uploading item to workspace {workspace} with key {workspace_key}")

        # Upload item to S3
        upload_file_s3(body, S3_BUCKET, workspace_key)

        logger.info("Item uploaded successfully")
        keys.append(workspace_key)
    return keys


@app.post("/catalogs/user-datasets/{workspace}", dependencies=[Depends(opa_dependency)])
async def create_item(
    workspace: str,
    request: ItemRequest,
    rate_limiter=Depends(rate_limiter_dependency),  # noqa: B008
):
    """Endpoint to create a new item and collection within a workspace"""

    url = request.url
    added_keys = upload_nested_files(url, workspace)

    output_data = {
        "id": f"{workspace}/create_item",
        "workspace": workspace,
        "bucket_name": S3_BUCKET,
        "added_keys": added_keys,
        "updated_keys": [],
        "deleted_keys": [],
        "source": workspace,
        "target": f"user-datasets/{workspace}",
    }
    logger.info(f"Sending message to pulsar: {output_data}")
    producer.send((json.dumps(output_data)).encode("utf-8"))

    return JSONResponse(content={"message": "Item created successfully"}, status_code=200)


@app.delete("/catalogs/user-datasets/{workspace}", dependencies=[Depends(opa_dependency)])
async def delete_item(
    workspace: str,
    request: ItemRequest,
    rate_limiter=Depends(rate_limiter_dependency),  # noqa: B008
):
    """Endpoint to delete an item in a workspace's collection"""

    url = request.url
    workspace_key = f"{workspace}/saved-data/{url.split('/', 9)[-1]}"
    if not os.path.splitext(workspace_key)[1]:
        workspace_key += ".json"

    logger.info(f"Deleting item from workspace {workspace} with key {workspace_key}")
    delete_file_s3(S3_BUCKET, workspace_key)

    output_data = {
        "id": f"{workspace}/delete_item",
        "workspace": workspace,
        "bucket_name": S3_BUCKET,
        "added_keys": [],
        "updated_keys": [],
        "deleted_keys": [workspace_key],
        "source": workspace,
        "target": f"user-datasets/{workspace}",
    }
    producer.send((json.dumps(output_data)).encode("utf-8"))

    return JSONResponse(content={"message": "Item deleted successfully"}, status_code=200)


@app.put("/catalogs/user-datasets/{workspace}", dependencies=[Depends(opa_dependency)])
async def update_item(
    workspace: str,
    request: ItemRequest,
    rate_limiter=Depends(rate_limiter_dependency),  # noqa: B008
):
    """Endpoint to update an item and collection within a workspace"""

    url = request.url
    updated_keys = upload_nested_files(url, workspace)

    output_data = {
        "id": f"{workspace}/update_item",
        "workspace": workspace,
        "bucket_name": S3_BUCKET,
        "added_keys": [],
        "updated_keys": updated_keys,
        "deleted_keys": [],
        "source": workspace,
        "target": f"user-datasets/{workspace}",
    }
    logger.info(f"Sending message to pulsar: {output_data}")
    producer.send((json.dumps(output_data)).encode("utf-8"))

    return JSONResponse(content={"message": "Item updated successfully"}, status_code=200)
