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

from .airbus_client import AirbusClient
from .planet_client import PlanetClient
from .utils import (
    OrderStatus,
    delete_file_s3,
    execute_order_workflow,
    get_file_from_url,
    get_nested_files_from_url,
    get_path_params,
    get_user_details,
    rate_limiter_dependency,
    update_stac_order_status,
    upload_file_s3,
    upload_stac_hierarchy_for_order,
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

AIRBUS_ENV = os.getenv("AIRBUS_ENV", "prod")
airbus_client = AirbusClient(AIRBUS_ENV)
planet_client = PlanetClient()

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


async def workspace_access_dependency(
    request: Request, path_params: dict = Depends(get_path_params)  # noqa: B008
):
    if ENABLE_OPA_POLICY_CHECK:
        if not await validate_workspace_access(request, path_params):
            raise HTTPException(status_code=403, detail="Access denied")


def ensure_user_logged_in(request: Request):
    if ENABLE_OPA_POLICY_CHECK:
        username, _ = get_user_details(request)

        # Add values for logs
        logger.info("Logged in as user: %s", username)

        if not username:
            raise HTTPException(status_code=404)


class OrderableCatalogEnum(str, Enum):
    planet = "planet"
    airbus = "airbus"


class EndUser(BaseModel):
    endUserName: str
    country: str


class ItemRequest(BaseModel):
    url: str
    extra_data: Optional[Dict[str, Any]] = Field(default_factory=dict)


class OrderRequest(BaseModel):
    workspace: str
    product_bundle: str
    coordinates: Optional[list] = Field(default_factory=list)
    endUserCountry: Optional[str] = None


class QuoteRequest(BaseModel):
    """Request body for quote endpoint"""

    coordinates: list = None
    itemUuids: list = []


class QuoteResponse(BaseModel):
    """Response body for quote endpoint"""

    value: float
    units: str


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
        workspace_key = f"{workspace}/{catalog_name}/{path_after_catalog}"
        if not os.path.splitext(workspace_key)[1]:
            workspace_key += ".json"

        if url_to_add == url and order_status is not None:
            try:
                json_body = json.loads(body)
                json_body["assets"] = {}
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
    "/stac/catalogs/commercial/catalogs/{catalog}/collections/{collection}/items/{item}/order",
    dependencies=[Depends(workspace_access_dependency)],
    responses={
        200: {
            "content": {"application/json": {"example": {"message": "Item ordered successfully"}}}
        }
    },
)
async def order_item(
    request: Request,
    catalog: OrderableCatalogEnum,
    collection: str,
    item: str,
    order_request: Annotated[
        OrderRequest,
        Body(
            examples=[
                {
                    "workspace": "my-workspace",
                    "product_bundle": "general_use",
                    "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
                    "endUserCountry": "GB",
                },
            ]
        ),
    ],
    producer=Depends(get_producer),  # noqa: B008
):
    """Create a new item and collection within a workspace with an order status, and
    execute a workflow to order the item from a commercial data provider.

    * workspace: The workspace into which the item will be ordered and delivered
    * product_bundle: The product bundle to order from the commercial data provider
    * coordinates: (Optional) Coordinates of a polygon to limit the AOI of the item for purchase where
      possible. Given in the same nested format as STAC
    * endUserCountry: (Optional) A country code corresponding to the country of the end user"""

    authorization = request.headers.get("Authorization")

    workspace = order_request.workspace
    order_url = str(request.url)
    base_item_url = order_url.rsplit("/order", 1)[0]
    added_keys, stac_item_key, item_data = upload_stac_hierarchy_for_order(
        base_item_url, catalog, collection, item, workspace
    )

    # End users must be supplied for PNEO orders, and at least as an empty list for other optical orders
    optical_collections = ["airbus_pneo_data", "airbus_phr_data", "airbus_spot_data"]
    end_users = None
    if collection in optical_collections:
        end_users = []
        if country_code := order_request.endUserCountry:
            airbus_client.validate_country_code(country_code)
            username, _ = get_user_details(request)
            end_users = [{"endUserName": username, "country": country_code}]
    if collection == "airbus_pneo_data" and not end_users:
        raise HTTPException(
            status_code=400,
            detail="End users must be supplied for PNEO orders",
        )

    output_data = {
        "id": f"{workspace}/order_item",
        "workspace": workspace,
        "bucket_name": S3_BUCKET,
        "added_keys": added_keys,
        "updated_keys": [],
        "deleted_keys": [],
        "source": workspace,
        "target": f"user-datasets/{workspace}",
    }
    if collection == "airbus_sar_data":
        adaptor_name = "airbus-sar-adaptor"
        commercial_data_bucket = "commercial-data-airbus"
    elif catalog == "airbus":
        adaptor_name = "airbus-optical-adaptor"
        commercial_data_bucket = "airbus-commercial-data"
    else:
        adaptor_name = "planet-adaptor"
        commercial_data_bucket = S3_BUCKET

    try:
        ades_response = execute_order_workflow(
            catalog,
            workspace,
            adaptor_name,
            authorization,
            f"s3://{S3_BUCKET}/{stac_item_key}",
            commercial_data_bucket,
            order_request.product_bundle,
            order_request.coordinates,
            end_users,
        )
        logger.info(f"Response from ADES: {ades_response}")
    except Exception as e:
        logger.error(f"Error executing order workflow: {e}")
        upload_single_item(base_item_url, workspace, stac_item_key, OrderStatus.FAILED.value)
        logger.info(f"Sending message to pulsar: {output_data}")
        producer.send((json.dumps(output_data)).encode("utf-8"))
        raise HTTPException(status_code=500, detail="Error executing order workflow") from e

    logger.info(f"Sending message to pulsar: {output_data}")
    producer.send((json.dumps(output_data)).encode("utf-8"))

    location_url = (
        f"{EODH_DOMAIN}/api/catalogue/user/catalogs/{workspace}/catalogs/commercial-data/"
        f"catalogs/{catalog}/collections/{collection}/items/{item}"
    )

    return JSONResponse(content=item_data, status_code=201, headers={"Location": location_url})


@app.post(
    "/stac/catalogs/commercial/catalogs/{catalog}/collections/{collection}/items/{acquisition_id}/quote",
    response_model=QuoteResponse,
    responses={200: {"content": {"application/json": {"example": {"value": 100, "units": "EUR"}}}}},
    dependencies=[Depends(ensure_user_logged_in)],
)
def quote(
    request: Request,
    catalog: OrderableCatalogEnum,
    collection: str,
    acquisition_id: str,
    body: Annotated[
        QuoteRequest,
        Body(
            examples=[
                {
                    "coordinates": [[[8.1, 31.7], [8.1, 31.6], [8.2, 31.9], [8.0, 31.5]]],
                    "itemUuids": [
                        "12345678-1234-1234-1234-123456789012",
                        "87654321-4321-4321-4321-210987654321",
                    ],
                }
            ],
        ),
    ],
):
    """Return a quote for a Planet or Airbus acquisition ID within an EODH catalogue and collection.

    * coordinates: Coordinates to limit the AOI of the item for purchase where possible. Given in the
      same nested format as STAC
    * itemUuids: (Airbus-only) This is required for stereo and multi Pléiades Neo orders only, and
      consists of a list of ids corresponding to individual mono items that are part of the order

    """

    coordinates = body.coordinates

    if catalog == "airbus":
        if collection == "airbus_sar_data":
            if AIRBUS_ENV == "prod":
                url = "https://sar.api.oneatlas.airbus.com/v1/sar/prices"
            else:
                url = "https://dev.sar.api.oneatlas.airbus.com/v1/sar/prices"
            request_body = {"acquisitions": [acquisition_id]}
        else:
            url = "https://order.api.oneatlas.airbus.com/api/v1/prices"
            spectral_processing = "bundle"
            if collection == "airbus_pneo_data":
                product_type = "PleiadesNeoArchiveMono"
                contract_id = "CTR24005241"
                item_uuids = body.itemUuids
                item_id = None
                if len(item_uuids) > 1:
                    product_type = "PleiadesNeoArchiveMulti"
                    spectral_processing = "full_bundle"
            elif collection == "airbus_phr_data":
                product_type = "PleiadesArchiveMono"
                contract_id = "UNIVERSITY_OF_LEICESTER_Orders"
                item_uuids = None
                item_id = acquisition_id
            elif collection == "airbus_spot_data":
                product_type = "SPOTArchive1.5Mono"
                contract_id = "UNIVERSITY_OF_LEICESTER_Orders"
                item_uuids = None
                item_id = acquisition_id
            else:
                return JSONResponse(
                    status_code=404,
                    content={"detail": f"Collection {collection} not recognised"},
                )
            request_body = {
                "aoi": [
                    {
                        "id": 1,
                        "name": "Polygon 1",
                        "geometry": {"type": "Polygon", "coordinates": coordinates},
                    }
                ],
                "programReference": "",
                "contractId": contract_id,
                "items": [
                    {
                        "notifications": [],
                        "stations": [],
                        "productTypeId": product_type,
                        "aoiId": 1,
                        "properties": [],
                    }
                ],
                "primaryMarket": "NQUAL",
                "secondaryMarket": "",
                "customerReference": "Polygon 1",
                "optionsPerProductType": [
                    {
                        "productTypeId": product_type,
                        "options": [
                            {"key": "delivery_method", "value": "on_the_flow"},
                            {"key": "fullStrip", "value": "false"},
                            {"key": "image_format", "value": "dimap_geotiff"},
                            {"key": "licence", "value": "standard"},
                            {"key": "pixel_coding", "value": "12bits"},
                            {"key": "priority", "value": "standard"},
                            {"key": "processing_level", "value": "primary"},
                            {"key": "radiometric_processing", "value": "reflectance"},
                            {"key": "spectral_processing", "value": spectral_processing},
                        ],
                    }
                ],
                "orderGroup": "",
                "delivery": {"type": "network"},
            }

            if item_uuids:
                data_source_ids = []
                for item_uuid in item_uuids:
                    data_source_ids.append({"catalogId": "PublicMOC", "catalogItemId": item_uuid})
                request_body["items"][0]["dataSourceIds"] = data_source_ids

            if item_id:
                request_body["items"][0]["datastripIds"] = [item_id]

        # username, _ = get_user_details(request)
        access_token = airbus_client.generate_access_token()
        if not access_token:
            return JSONResponse(
                status_code=500, content={"detail": "Failed to generate access token"}
            )

        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

        try:
            response_body = airbus_client.get_quote_from_airbus(url, request_body, headers)
        except requests.RequestException as e:
            return JSONResponse(status_code=500, content={"detail": str(e)})

        price_json = {}
        if collection == "airbus_sar_data":
            for item in response_body:
                if item.get("acquisitionId") == acquisition_id:
                    price_json = {
                        "units": item["price"]["currency"],
                        "value": item["price"]["total"],
                    }
                    break
        else:
            price_json = {
                "units": response_body["currency"],
                "value": response_body["totalAmount"],
            }

        if price_json:
            return QuoteResponse(value=price_json["value"], units=price_json["units"])

        return JSONResponse(
            status_code=404, content={"detail": "Quote not found for given acquisition ID"}
        )

    elif catalog == "planet":
        try:
            area = planet_client.get_area_estimate(acquisition_id, collection, coordinates)

            if collection.lower() == "skysatscene" and area < 3:
                # SkySatScene has a minimum order size of 3 km2
                area = 3

            return QuoteResponse(value=area, units="km2")

        except Exception as e:
            return JSONResponse(content={"message": str(e)}, status_code=400)

    else:
        return JSONResponse(
            content={"message:" f"{catalog} not recognised"},
            status_code=404,
        )


def fetch_airbus_asset(collection: str, item: str, asset_name: str) -> Response:
    """Fetch an asset via an external link in an Airbus item, using a generated access token"""
    item_url = f"https://{EODH_DOMAIN}/api/catalogue/stac/catalogs/supported-datasets/catalogs/airbus/collections/{collection}/items/{item}"
    logger.info(f"Fetching item data from {item_url}")
    item_response = requests.get(item_url)
    item_response.raise_for_status()
    item_data = item_response.json()
    asset_link = item_data.get("assets", {}).get(f"external_{asset_name}", {}).get("href")
    if not asset_link:
        raise HTTPException(status_code=404, detail=f"External {asset_name} link not found in item")
    logger.info(f"Fetching {asset_name} from {asset_link}")

    access_token = airbus_client.generate_access_token()
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
