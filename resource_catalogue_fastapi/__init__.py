import json
import logging
import os
from distutils.util import strtobool
from enum import Enum
from typing import Annotated, Any, Dict, List, Optional, Tuple

import requests
from fastapi import Body, Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pulsar import Client as PulsarClient
from pydantic import BaseModel, Field

from .utils import (
    delete_file_s3,
    execute_order_workflow,
    generate_airbus_access_token,
    get_file_from_url,
    get_nested_files_from_url,
    get_path_params,
    get_user_details,
    rate_limiter_dependency,
    update_stac_order_status,
    upload_file_s3,
    validate_workspace_access,
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

EODH_DOMAIN = os.getenv("EODH_DOMAIN", "dev.eodatahub.org.uk")

# OPA service endpoint
OPA_SERVICE_ENDPOINT = os.getenv(
    "OPA_SERVICE_ENDPOINT", "http://opal-client.opal:8181/v1/data/workspaces/allow"
)

# Used for local testing where OPA is not available
ENABLE_OPA_POLICY_CHECK = strtobool(os.getenv("ENABLE_OPA_POLICY_CHECK", "false"))

# S3 bucket to store user data
S3_BUCKET = os.getenv("S3_BUCKET", "test-bucket")

# Root path for FastAPI
RC_FASTAPI_ROOT_PATH = os.getenv("RC_FASTAPI_ROOT_PATH", "/api/catalogue")

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
    docs_url="/manage/docs",
    openapi_url="/manage/openapi.json",
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


def workspace_access_dependency(
    request: Request, path_params: dict = Depends(get_path_params)  # noqa: B008
):
    if ENABLE_OPA_POLICY_CHECK:
        if not validate_workspace_access(request, path_params):
            raise HTTPException(status_code=403, detail="Access denied")


def ensure_user_logged_in(request: Request):
    if ENABLE_OPA_POLICY_CHECK:
        username, _ = get_user_details(request)

        # Add values for logs
        logger.info("Logged in as user: %s", username)

        if not username:
            raise HTTPException(status_code=404)


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


class OrderRequest(ItemRequest):
    product_bundle: str
    coordinates: Optional[list] = Field(default_factory=list)


def upload_nested_files(
    url: str, workspace: str, catalog_name: str = "saved-data", order_status: Optional[str] = None
) -> Tuple[Dict[str, List[str]], Optional[str], str]:
    """Upload a file along with any nested files to a workspace"""
    files_to_add = get_nested_files_from_url(url)
    keys = {"added_keys": [], "updated_keys": []}
    ordered_item_key = None
    for url_to_add in files_to_add:
        logger.info(f"Adding url {url_to_add}")
        body = get_file_from_url(url_to_add)

        # Extract the path after the first catalog from the URL to create the S3 key
        path_after_catalog = url_to_add.split("/", 9)[-1]
        try:
            collection_id = path_after_catalog.split("collections/")[1].split("/items/")[0]
        except (IndexError, KeyError):
            collection_id = None
        workspace_key = f"{workspace}/{catalog_name}/{path_after_catalog}"
        if not os.path.splitext(workspace_key)[1]:
            workspace_key += ".json"

        if url_to_add == url and order_status is not None:
            try:
                json_body = json.loads(body)
                json_body["assets"] = {}
                update_stac_order_status(json_body, None, order_status)
                # Temporary fix for EODHP-1162
                for link in json_body.get("links", []):
                    link["href"] = link["href"].replace(
                        "api/catalogue/stac/v1/supported-datasets",
                        "api/catalogue/stac/v1/catalogs/supported-datasets",
                    )
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
    return keys, ordered_item_key, collection_id


def upload_single_item(url: str, workspace: str, workspace_key: str, order_status: str):
    """Uploads one item found at given URL to a workspace, updating the order status"""
    body = get_file_from_url(url)
    try:
        json_body = json.loads(body)
        update_stac_order_status(json_body, None, order_status)
        body = json.dumps(json_body)
    except Exception as e:
        logger.error(f"Error parsing item {url} to order as STAC: {e}")
        raise

    logger.info(f"Uploading item to workspace {workspace} with key {workspace_key}")

    # Upload item to S3
    is_updated = upload_file_s3(body, S3_BUCKET, workspace_key)

    logger.info("Item uploaded successfully")

    return is_updated


@app.post(
    "/manage/catalogs/user-datasets/{workspace}",
    dependencies=[Depends(workspace_access_dependency)],
)
async def create_item(
    workspace: str,
    request: ItemRequest,
    rate_limiter=Depends(rate_limiter_dependency),  # noqa: B008
    producer=Depends(get_producer),  # noqa: B008
):
    """Endpoint to create a new item and collection within a workspace"""

    url = request.url
    keys, _, _ = upload_nested_files(url, workspace)

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


@app.delete(
    "/manage/catalogs/user-datasets/{workspace}",
    dependencies=[Depends(workspace_access_dependency)],
)
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


@app.put(
    "/manage/catalogs/user-datasets/{workspace}",
    dependencies=[Depends(workspace_access_dependency)],
)
async def update_item(
    workspace: str,
    request: ItemRequest,
    rate_limiter=Depends(rate_limiter_dependency),  # noqa: B008
    producer=Depends(get_producer),  # noqa: B008
):
    """Endpoint to update an item and collection within a workspace"""

    url = request.url
    keys, _, _ = upload_nested_files(url, workspace)

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
    "/manage/catalogs/user-datasets/{workspace}/commercial-data",
    dependencies=[Depends(workspace_access_dependency)],
    responses={
        200: {
            "content": {"application/json": {"example": {"message": "Item ordered successfully"}}}
        }
    },
)
async def order_item(
    request: Request,
    workspace: str,
    order_request: Annotated[
        OrderRequest,
        Body(
            examples=[
                {
                    "url": f"https://{EODH_DOMAIN}/api/catalogue/stac/catalogs/supported-datasets/airbus/collections/airbus_pneo_data/items/ACQ_PNEO3_05300415120321",
                    "product_bundle": "general_use",
                    "coordinates": "[[[0, 0], [0, 1], [1, 1], [0, 0]]]",
                },
            ]
        ),
    ],
    producer=Depends(get_producer),  # noqa: B008
):
    """Create a new item and collection within a workspace with an order status, and
    execute a workflow to order the item from a commercial data provider.

    * url: The EODHP STAC item URL to order
    * product_bundle: The product bundle to order from the commercial data provider
    * coordinates: (Optional) Coordinates of any area of interest (AOI)
    * extra_data: (Optional) A placeholder for future data options to include in the item"""

    authorization = request.headers.get("Authorization")

    url = order_request.url
    keys, stac_key, collection_id = upload_nested_files(
        url, workspace, "commercial-data", OrderStatus.PENDING.value
    )

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
    if collection_id.startswith("airbus"):
        catalog_name = "airbus"
        commercial_data_bucket = "commercial-data-airbus"
        if collection_id == "airbus_sar_data":
            adaptor_name = "airbus-sar-adaptor"
        else:
            adaptor_name = "airbus-optical-adaptor"
    else:
        catalog_name = "planet"
        adaptor_name = "planet-adaptor"
        commercial_data_bucket = S3_BUCKET

    try:
        ades_response = execute_order_workflow(
            catalog_name,
            workspace,
            adaptor_name,
            authorization,
            f"s3://{S3_BUCKET}/{stac_key}",
            commercial_data_bucket,
            order_request.product_bundle,
            order_request.coordinates,
        )
        logger.info(f"Response from ADES: {ades_response}")
    except Exception as e:
        logger.error(f"Error executing order workflow: {e}")
        upload_single_item(url, workspace, stac_key, OrderStatus.FAILED.value)
        logger.info(f"Sending message to pulsar: {output_data}")
        producer.send((json.dumps(output_data)).encode("utf-8"))
        raise HTTPException(status_code=500, detail="Error executing order workflow") from e

    logger.info(f"Sending message to pulsar: {output_data}")
    producer.send((json.dumps(output_data)).encode("utf-8"))

    return JSONResponse(content={"message": "Item ordered successfully"}, status_code=200)


def fetch_airbus_asset(collection: str, item: str, asset_name: str) -> Response:
    """Fetch an asset via an external link in an Airbus item, using a generated access token"""
    item_url = f"https://{EODH_DOMAIN}/api/catalogue/stac/catalogs/supported-datasets/airbus/collections/{collection}/items/{item}"
    logger.info(f"Fetching item data from {item_url}")
    item_response = requests.get(item_url)
    item_response.raise_for_status()
    item_data = item_response.json()
    asset_link = item_data.get("assets", {}).get(f"external_{asset_name}", {}).get("href")
    if not asset_link:
        raise HTTPException(status_code=404, detail=f"External {asset_name} link not found in item")
    logger.info(f"Fetching {asset_name} from {asset_link}")

    access_token = generate_airbus_access_token("prod")
    headers = {"Authorization": f"Bearer {access_token}"}
    asset_response = requests.get(asset_link, headers=headers)
    asset_response.raise_for_status()
    logger.info(f"{asset_name} retrieved successfully")

    return Response(
        content=asset_response.content,
        media_type=asset_response.headers.get("Content-Type"),
    )


@app.get(
    "/stac/catalogs/supported-datasets/airbus/collections/{collection}/items/{item}/thumbnail",
    dependencies=[Depends(ensure_user_logged_in)],
)
async def get_thumbnail(collection: str, item: str):
    """Endpoint to get the thumbnail of an item"""
    try:
        return fetch_airbus_asset(collection, item, "thumbnail")

    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get(
    "/stac/catalogs/supported-datasets/airbus/collections/{collection}/items/{item}/quicklook",
    dependencies=[Depends(ensure_user_logged_in)],
)
async def get_quicklook(collection: str, item: str):
    """Endpoint to get the quicklook of an item"""
    try:
        return fetch_airbus_asset(collection, item, "quicklook")

    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
