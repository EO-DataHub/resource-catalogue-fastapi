import json
import logging
import os
from distutils.util import strtobool
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pulsar import Client as PulsarClient
from pydantic import BaseModel, Field

from .utils import (
    check_policy,
    delete_file_s3,
    execute_order_workflow,
    get_file_from_url,
    get_nested_files_from_url,
    get_path_params,
    rate_limiter_dependency,
    update_stac_order_status,
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
producer = None


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


# Dependency function to get or create the producer
def get_producer():
    global producer
    if producer is None:
        producer = pulsar_client.create_producer(
            topic="harvested", producer_name="resource_catalogue_fastapi"
        )
    return producer


def opa_dependency(request: Request, path_params: dict = Depends(get_path_params)):  # noqa: B008
    if ENABLE_OPA_POLICY_CHECK:
        if not check_policy(request, path_params, OPA_SERVICE_ENDPOINT, WORKSPACES_DOMAIN):
            raise HTTPException(status_code=403, detail="Access denied")


class OrderStatus(Enum):
    ORDERABLE = "orderable"
    ORDERED = "ordered"
    PENDING = "pending"
    SHIPPING = "shipping"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


class ItemRequest(BaseModel):
    url: str
    extra_data: Optional[Dict[str, Any]] = Field(default_factory=dict)


def upload_nested_files(
    url: str, workspace: str, catalog_name: str = "saved-data", order_status: Optional[str] = None
) -> Tuple[Dict[str, List[str]], Optional[str]]:
    """Upload a file along with any nested files to a workspace"""
    files_to_add = get_nested_files_from_url(url)
    keys = {"added_keys": [], "updated_keys": []}
    ordered_item_key = None
    for url_to_add in files_to_add:
        logger.info(f"Adding url {url_to_add}")
        body = get_file_from_url(url_to_add)

        # Extract the path after the first catalog from the URL to create the S3 key
        path_after_catalog = url_to_add.split("/", 9)[-1]
        workspace_key = f"{workspace}/{catalog_name}/{path_after_catalog}"
        if not os.path.splitext(workspace_key)[1]:
            workspace_key += ".json"

        if url_to_add == url and order_status is not None:
            try:
                json_body = json.loads(body)
                update_stac_order_status(json_body, None, order_status)
                body = json.dumps(json_body)
                ordered_item_key = workspace_key
            except Exception as e:
                logger.error(f"Error parsing item {url} to order as STAC: {e}")
                raise

        logger.info(f"Uploading item to workspace {workspace} with key {workspace_key}")

        # Upload item to S3
        is_updated = upload_file_s3(body, S3_BUCKET, workspace_key)

        logger.info("Item uploaded successfully")

        if is_updated:
            keys["updated_keys"].append(workspace_key)
        else:
            keys["added_keys"].append(workspace_key)
    return keys, ordered_item_key


@app.post("/catalogs/user-datasets/{workspace}", dependencies=[Depends(opa_dependency)])
async def create_item(
    workspace: str,
    request: ItemRequest,
    rate_limiter=Depends(rate_limiter_dependency),  # noqa: B008
    producer=Depends(get_producer),  # noqa: B008
):
    """Endpoint to create a new item and collection within a workspace"""

    url = request.url
    keys, _ = upload_nested_files(url, workspace)

    output_data = {
        "id": f"{workspace}/create_item",
        "workspace": workspace,
        "bucket_name": S3_BUCKET,
        "added_keys": keys.get("added_keys", []),
        "updated_keys": keys.get("updated_keys", []),
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
    producer=Depends(get_producer),  # noqa: B008
):
    """Endpoint to delete an item in a workspace's collection"""

    url = request.url
    # Extract the path after the first catalog from the URL to create the S3 key
    path_after_catalog = url.split("/", 9)[-1]
    workspace_key = f"{workspace}/saved-data/{path_after_catalog}"
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
    producer=Depends(get_producer),  # noqa: B008
):
    """Endpoint to update an item and collection within a workspace"""

    url = request.url
    keys, _ = upload_nested_files(url, workspace)

    output_data = {
        "id": f"{workspace}/update_item",
        "workspace": workspace,
        "bucket_name": S3_BUCKET,
        "added_keys": keys.get("added_keys", []),
        "updated_keys": keys.get("updated_keys", []),
        "deleted_keys": [],
        "source": workspace,
        "target": f"user-datasets/{workspace}",
    }
    logger.info(f"Sending message to pulsar: {output_data}")
    producer.send((json.dumps(output_data)).encode("utf-8"))

    return JSONResponse(content={"message": "Item updated successfully"}, status_code=200)


@app.post(
    "/catalogs/user-datasets/{workspace}/ordered-data", dependencies=[Depends(opa_dependency)]
)
async def order_item(
    request: Request,
    workspace: str,
    item_request: ItemRequest,
    producer=Depends(get_producer),  # noqa: B008
):
    """Endpoint to create a new item and collection within a workspace with an order status, and
    execute a workflow to order the item"""

    authorization = request.headers.get("Authorization")

    url = item_request.url
    keys, stac_key = upload_nested_files(url, workspace, "ordered-data", OrderStatus.PENDING.value)

    output_data = {
        "id": f"{workspace}/order_item",
        "workspace": workspace,
        "bucket_name": S3_BUCKET,
        "added_keys": keys.get("added_keys", []),
        "updated_keys": keys.get("updated_keys", []),
        "deleted_keys": [],
        "source": workspace,
        "target": f"user-datasets/{workspace}",
    }
    logger.info(f"Sending message to pulsar: {output_data}")
    producer.send((json.dumps(output_data)).encode("utf-8"))

    ades_response = execute_order_workflow(
        "airbus", workspace, "airbus-sar-adaptor", authorization, stac_key, S3_BUCKET
    )
    logger.info(f"Response from ADES: {ades_response}")

    return JSONResponse(content={"message": "Item ordered successfully"}, status_code=200)
