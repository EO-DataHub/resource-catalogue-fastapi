import hashlib
import json
import logging
import os
from distutils.util import strtobool
from enum import Enum
from typing import Annotated, Any, Dict, List, Optional, Tuple, Union

import requests
from fastapi import Body, Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pulsar import Client as PulsarClient
from pydantic import BaseModel, Field

from .airbus_client import AirbusClient
from .planet_client import PlanetClient
from .utils import (
    OrderStatus,
    check_user_can_access_a_workspace,
    check_user_can_access_requested_workspace,
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

PLANET_COLLECTIONS = os.getenv("PLANET_COLLECTIONS", "PSScene,SkySatCollect").split(",")

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
    """Get or create a producer for the Pulsar client"""
    global producer
    if producer is None:
        producer = pulsar_client.create_producer(
            topic="transformed", producer_name="resource_catalogue_fastapi"
        )
    return producer


async def workspace_access_dependency(
    request: Request, path_params: dict = Depends(get_path_params)  # noqa: B008
):
    """Dependency to check if a user has access to a specified workspace"""
    if ENABLE_OPA_POLICY_CHECK:
        if not await check_user_can_access_requested_workspace(request, path_params):
            raise HTTPException(status_code=403, detail="Access denied")


def ensure_user_can_access_a_workspace(request: Request):
    """Dependency to check if a user has access to a workspace"""
    if ENABLE_OPA_POLICY_CHECK:
        if not check_user_can_access_a_workspace(request):
            raise HTTPException(status_code=403, detail="Access denied")


def ensure_user_logged_in(request: Request):
    """Dependency to check if a user is logged in"""
    if ENABLE_OPA_POLICY_CHECK:
        username, _ = get_user_details(request)

        # Add values for logs
        logger.info("Logged in as user: %s", username)

        if not username:
            raise HTTPException(status_code=403, detail="Access denied")


class ParentCatalogue(str, Enum):
    """Parent catalogue for commercial data in the resource catalogue"""

    supported_datasets = "supported-datasets"
    commercial = "commercial"


class OrderableCatalogue(str, Enum):
    """Catalogues for ordering commercial data"""

    planet = "planet"
    airbus = "airbus"


OrderablePlanetCollection = Enum(
    "OrderablePlanetCollection", {name: name for name in PLANET_COLLECTIONS}
)


class OrderableAirbusCollection(str, Enum):
    """Collections for ordering Airbus commercial data"""

    sar = "airbus_sar_data"
    pneo = "airbus_pneo_data"
    phr = "airbus_phr_data"
    spot = "airbus_spot_data"


# Combine the members of OrderableAirbusCollection and OrderablePlanetCollection
combined_collections = {e.name: e.value for e in OrderableAirbusCollection}
combined_collections.update({e.name: e.value for e in OrderablePlanetCollection})
OrderableCollection = Enum("OrderableCollection", combined_collections)


class ItemRequest(BaseModel):
    """Request body for create, update and delete item endpoints"""

    url: str
    extra_data: Optional[Dict[str, Any]] = Field(default_factory=dict)


class ProductBundle(str, Enum):
    """Product bundles for Planet and Airbus optical data"""

    general_use = "General use"
    visual = "Visual"
    basic = "Basic"
    analytic = "Analytic"


class ProductBundleRadar(str, Enum):
    """Product bundles for Airbus SAR data"""

    SSC = "SSC"
    MGD = "MGD"
    GEC = "GEC"
    EEC = "EEC"


class LicenceRadar(str, Enum):
    """Licence types for Airbus SAR data"""

    SINGLE = "Single User Licence"
    MULTI_2_5 = "Multi User (2 - 5) Licence"
    MULTI_6_30 = "Multi User (6 - 30) Licence"

    @property
    def airbus_value(self):
        """Map the licence type to the Airbus API value"""
        mappings = {
            "Single User Licence": "Single User License",
            "Multi User (2 - 5) Licence": "Multi User (2 - 5) License",
            "Multi User (6 - 30) Licence": "Multi User (6 - 30) License",
        }
        return mappings[self.value]


class LicenceOptical(str, Enum):
    """Licence types for Airbus optical data"""

    STANDARD = "Standard"
    BACKGROUND = "Background Layer"
    STANDARD_BACKGROUND = "Standard + Background Layer"
    ACADEMIC = "Academic"
    MEDIA = "Media Licence"
    STANDARD_MULTI_2_5 = "Standard Multi End-Users (2-5)"
    STANDARD_MULTI_6_10 = "Standard Multi End-Users (6-10)"
    STANDARD_MULTI_11_30 = "Standard Multi End-Users (11-30)"
    STANDARD_MULTI_30 = "Standard Multi End-Users (>30)"

    @property
    def airbus_value(self):
        """Map the licence type to the Airbus API value"""
        mappings = {
            "Standard": "standard",
            "Background Layer": "background_layer",
            "Standard + Background Layer": "stand_background_layer",
            "Academic": "educ",
            "Media Licence": "media",
            "Standard Multi End-Users (2-5)": "standard_1_5",
            "Standard Multi End-Users (6-10)": "standard_6_10",
            "Standard Multi End-Users (11-30)": "standard_11_30",
            "Standard Multi End-Users (>30)": "standard_up_30",
        }
        return mappings[self.value]


class Orbit(str, Enum):
    """Orbit types for Airbus SAR data"""

    RAPID = "rapid"
    SCIENCE = "science"


class ResolutionVariant(str, Enum):
    """Resolution variants for Airbus SAR data"""

    RE = "RE"
    SE = "SE"


class Projection(str, Enum):
    """Projection types for Airbus SAR data"""

    AUTO = "Auto"
    UTM = "UTM"
    UPS = "UPS"

    @property
    def airbus_value(self):
        """Map the projection type to the Airbus API value"""
        mappings = {
            "Auto": "auto",
            "UTM": "UTM",
            "UPS": "UPS",
        }
        return mappings[self.value]


class RadarOptions(BaseModel):
    """Radar options for Airbus SAR data"""

    orbit: Orbit
    resolutionVariant: Optional[ResolutionVariant] = None
    projection: Optional[Projection] = None

    def model_dump(self):
        """Return the model data as a dictionary to send to the Airbus API"""
        data = super().model_dump()
        data = {k: v for k, v in data.items() if v is not None}
        if self.projection:
            data["projection"] = self.projection.airbus_value
        return data


class OrderRequest(BaseModel):
    """Request body for order endpoint"""

    productBundle: Union[ProductBundle, ProductBundleRadar]
    coordinates: Optional[list] = Field(default_factory=list)
    endUserCountry: Optional[str] = None
    licence: Optional[Union[LicenceOptical, LicenceRadar]] = None
    radarOptions: Optional[RadarOptions] = None


class QuoteRequest(BaseModel):
    """Request body for quote endpoint"""

    coordinates: list = None
    licence: Optional[Union[LicenceOptical, LicenceRadar]] = None


class QuoteResponse(BaseModel):
    """Response body for quote endpoint"""

    value: float
    units: str


def validate_licence(
    collection: str, licence: str
) -> Optional[Union[LicenceOptical, LicenceRadar]]:
    """Validate the licence type against allowed values for the collection"""
    if collection == OrderableAirbusCollection.sar.value:
        allowed_licences = {e.value for e in LicenceRadar}
        if not licence:
            raise HTTPException(
                status_code=400,
                detail=f"Licence is required for a radar item. Valid licences are: {allowed_licences}",
            )
        if licence not in allowed_licences:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid licence for a radar item. Valid licences are: {allowed_licences}",
            )
        return LicenceRadar(licence)

    elif collection in {e.value for e in OrderableAirbusCollection}:
        allowed_licences = {e.value for e in LicenceOptical}
        if not licence:
            raise HTTPException(
                status_code=400,
                detail=f"Licence is required for an optical item. Valid licences are: {allowed_licences}",
            )
        if licence not in allowed_licences:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid licence for an optical item. Valid licences are: {allowed_licences}",
            )
        return LicenceOptical(licence)
    return None


def validate_product_bundle(
    collection: str, product_bundle: str
) -> Union[ProductBundle, ProductBundleRadar]:
    """Validate the product bundle against allowed values for the collection"""
    if collection == OrderableAirbusCollection.sar.value:
        allowed_bundles = {e.value for e in ProductBundleRadar}
        if product_bundle not in allowed_bundles:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid product bundle for a radar item. Valid bundles are: {allowed_bundles}",
            )
        return ProductBundleRadar(product_bundle)

    allowed_bundles = {e.value for e in ProductBundle}
    if product_bundle not in allowed_bundles:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid product bundle. Valid bundles are: {allowed_bundles}",
        )
    return ProductBundle(product_bundle)


def validate_radar_options(
    collection: str, radar_options: Optional[RadarOptions], product_bundle: str
) -> Optional[Dict[str, Any]]:
    """Validate the radar options for Airbus SAR data"""
    if collection != OrderableAirbusCollection.sar.value:
        return None
    if not radar_options:
        raise HTTPException(
            status_code=400,
            detail="Radar options missing for a radar item.",
        )
    if not radar_options.orbit:
        raise HTTPException(
            status_code=400,
            detail="Orbit is required for a radar item.",
        )
    if product_bundle != ProductBundleRadar.SSC.value and not radar_options.resolutionVariant:
        raise HTTPException(
            status_code=400,
            detail="Resolution variant is required for a radar item when the product bundle is not SSC.",
        )
    if product_bundle == ProductBundleRadar.SSC.value and radar_options.resolutionVariant:
        raise HTTPException(
            status_code=400,
            detail=(
                "Resolution variant should not be provided for a radar item when the product "
                "bundle is SSC."
            ),
        )
    if (
        product_bundle not in [ProductBundleRadar.SSC.value, ProductBundleRadar.MGD.value]
        and not radar_options.projection
    ):
        raise HTTPException(
            status_code=400,
            detail="Projection is required for a radar item when the product bundle is not SSC or MGD.",
        )
    if (
        product_bundle in [ProductBundleRadar.SSC.value, ProductBundleRadar.MGD.value]
        and radar_options.projection
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "Projection should not be provided for a radar item when the product bundle is "
                "SSC or MGD."
            ),
        )
    radar_bundle = radar_options.model_dump()
    radar_bundle["product_type"] = product_bundle
    return radar_bundle


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
        upload_file_s3(body, S3_BUCKET, workspace_key)

        logger.info("Item uploaded successfully")

        keys["added_keys"].append(workspace_key)
    return keys, ordered_item_key


@app.get("/manage/health", summary="Health Check")
async def health_check():
    """Health check endpoint to verify Airbus client connectivity"""
    try:
        access_token = airbus_client.generate_access_token()
        if isinstance(access_token, str):
            return JSONResponse(content={"status": "healthy"}, status_code=200)
        else:
            raise HTTPException(
                status_code=500, detail="Airbus client failed to generate a valid access token"
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}") from e


@app.post(
    "/manage/catalogs/user-datasets/{workspace}",
    dependencies=[Depends(workspace_access_dependency)],
    deprecated=True,
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
    deprecated=True,
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
    deprecated=True,
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
    "/stac/catalogs/{parent_catalog}/catalogs/{catalog}/collections/{collection}/items/{item}/order",
    dependencies=[Depends(ensure_user_can_access_a_workspace)],
    responses={
        201: {
            "content": {
                "application/json": {
                    "example": {
                        "type": "Feature",
                        "stac_version": "1.0.0",
                        "stac_extensions": [
                            "https://stac-extensions.github.io/order/v1.1.0/schema.json"
                        ],
                        "id": "example-item",
                        "properties": {
                            "datetime": "2023-01-01T00:00:00Z",
                            "order:status": "Pending",
                            "created": "2025-01-01T00:00:00Z",
                            "updated": "2025-01-01T00:00:00Z",
                            "order_options": {
                                "productBundle": "Analytic",
                                "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
                                "endUser": {"country": "GB", "endUserName": "example-user"},
                                "licence": "Standard",
                                "radarOptions": {
                                    "orbit": "rapid",
                                    "resolutionVariant": "RE",
                                    "projection": "Auto",
                                },
                            },
                        },
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
                        },
                        "links": [],
                        "assets": {},
                    }
                }
            },
            "headers": {"Location": {"description": "URL of the created resource"}},
        },
        400: {"description": "Bad Request"},
        403: {"description": "Access Denied"},
        404: {"description": "Not Found"},
        500: {"description": "Internal Server Error"},
    },
)
async def order_item(
    request: Request,
    parent_catalog: ParentCatalogue,
    catalog: OrderableCatalogue,
    collection: OrderableCollection,
    item: str,
    order_request: Annotated[
        OrderRequest,
        Body(
            examples=[
                {
                    "productBundle": "General use",
                },
                {
                    "productBundle": "Analytic",
                    "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
                    "endUserCountry": "GB",
                    "licence": "Standard",
                },
                {
                    "productBundle": "GEC",
                    "endUserCountry": "GB",
                    "licence": "Single User Licence",
                    "radarOptions": {
                        "orbit": "rapid",
                        "resolutionVariant": "RE",
                        "projection": "Auto",
                    },
                },
            ]
        ),
    ],
    producer=Depends(get_producer),  # noqa: B008
):
    """Create a new item and collection within a workspace with an order status, and
    execute a workflow to order the item from a commercial data provider.

    * productBundle: The product bundle to order from the commercial data provider
    * coordinates: (Optional) Coordinates of a polygon to limit the AOI of the item for purchase where
      possible. Given in the same nested format as STAC
    * endUserCountry: (Optional) A country code corresponding to the country of the end user
    * licence: (Airbus only) The licence type for the order
    * radarOptions: (Airbus SAR-only) The radar options for the order.
        * orbit: The orbit type for the order
        * resolutionVariant: The resolution variant for the order (Not required for SSC product bundle)
        * projection: The projection for the order (Not required for SSC or MGD product bundles)
    """

    licence = validate_licence(collection.value, order_request.licence)
    product_bundle = validate_product_bundle(collection.value, order_request.productBundle)
    radar_options = validate_radar_options(
        collection.value, order_request.radarOptions, product_bundle.value
    )
    coordinates = order_request.coordinates if order_request.coordinates else None

    tag = ""
    if product_bundle:
        logging.info("Product bundle found")
        tag += f"-{product_bundle.value}"
    if radar_options:
        logging.info("Radar options found")
        if orbit := radar_options.get("orbit"):
            tag += "-" + orbit
        if resolution_variant := radar_options.get("resolutionVariant"):
            tag += "-" + resolution_variant
        if projection := radar_options.get("projection"):
            tag += "-" + projection
    if coordinates:
        logging.info("Coordinates found")
        tag += "-" + str(hashlib.md5(str(order_request.coordinates).encode("utf-8")).hexdigest())
    tag = f"_{tag[1:]}"  # remove first character (hyphen), replace with underscore

    username, workspaces = get_user_details(request)
    # workspaces from user details was originally a list, now usually expect string containing one workspace.
    workspace = workspaces[0] if isinstance(workspaces, list) else workspaces
    if not workspace:
        # This should never occur due to the workspace access dependency
        raise HTTPException(status_code=404)

    location_url = (
        f"{EODH_DOMAIN}/api/catalogue/stac/catalogs/user/catalogs/{workspace}/catalogs/commercial-data/"
        f"catalogs/{catalog.value}/collections/{collection.value}/items/{item}{tag}"
    )

    authorization = request.headers.get("Authorization")

    order_url = str(request.url)
    base_item_url = order_url.rsplit("/order", 1)[0]
    order_options = {
        "productBundle2": product_bundle.value,  # don't upload with this line
        "coordinates": coordinates,
        "endUser": {"country": order_request.endUserCountry, "endUserName": username},
        "licence": licence.airbus_value if licence else None,
    }
    if radar_options:
        order_options["radarOptions"] = radar_options

    status, added_keys, stac_item_key, transformed_item_key, item_data = (
        upload_stac_hierarchy_for_order(
            base_item_url,
            catalog.value,
            collection.value,
            item,
            workspace,
            order_options,
            S3_BUCKET,
            tag,
            location_url,
        )
    )

    logging.info(f"Status: {status}")

    if status in ["succeeded", "pending"]:
        message = f"Order not placed. Current item status is {status}"
        logging.info(message)
        return JSONResponse(
            content=item_data,
            status_code=200,
            headers={
                "Location": location_url,
                "Message": message,
            },
        )

    # Check if the item is a multi or stereo PNEO order
    if collection.value == OrderableAirbusCollection.pneo and (
        multi_ids := item_data.get("properties", {}).get(  # noqa: F841
            "composed_of_acquisition_identifiers"
        )
    ):
        return JSONResponse(
            status_code=500,
            content={"detail": "Multi and Stereo orders are not currently supported"},
        )

    # End users must be supplied for PNEO orders, and at least as an empty list for other optical orders
    optical_collections = {
        e.value for e in OrderableAirbusCollection if e != OrderableAirbusCollection.sar
    }
    end_users = None
    if collection.value in optical_collections:
        end_users = []
        if country_code := order_request.endUserCountry:
            airbus_client.validate_country_code(country_code)
            end_users = [{"endUserName": username, "country": country_code}]
    if collection.value == OrderableAirbusCollection.pneo.value and not end_users:
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
        "source": "/",
        "target": "/",
    }
    if collection.value == OrderableAirbusCollection.sar.value:
        adaptor_name = "airbus-sar-adaptor"
        commercial_data_bucket = "commercial-data-airbus"
    elif collection.value in optical_collections:
        adaptor_name = "airbus-optical-adaptor"
        commercial_data_bucket = "airbus-commercial-data"
    else:
        adaptor_name = "planet-adaptor"
        commercial_data_bucket = S3_BUCKET

    product_bundle_value = product_bundle.value
    if radar_options:
        product_bundle_value = json.dumps(radar_options)
    try:
        ades_response = execute_order_workflow(
            catalog.value,
            workspace,
            adaptor_name,
            authorization,
            f"s3://{S3_BUCKET}/{stac_item_key}",
            commercial_data_bucket,
            S3_BUCKET,
            PULSAR_URL,
            product_bundle_value,
            order_request.coordinates,
            end_users,
            licence.airbus_value if licence else None,
        )
        logger.info(f"Response from ADES: {ades_response}")
    except Exception as e:
        logger.error(f"Error executing order workflow: {e}")
        update_stac_order_status(item_data, None, OrderStatus.FAILED.value)
        upload_file_s3(json.dumps(item_data), S3_BUCKET, stac_item_key)
        upload_file_s3(json.dumps(item_data), S3_BUCKET, transformed_item_key)
        logger.info(f"Sending message to pulsar: {output_data}")
        producer.send((json.dumps(output_data)).encode("utf-8"))
        raise HTTPException(status_code=500, detail="Error executing order workflow") from e

    logger.info(f"Sending message to pulsar: {output_data}")
    producer.send((json.dumps(output_data)).encode("utf-8"))

    return JSONResponse(content=item_data, status_code=201, headers={"Location": location_url})


@app.post(
    "/stac/catalogs/{parent_catalog}/catalogs/{catalog}/collections/{collection}/items/{item}/quote",
    response_model=QuoteResponse,
    responses={200: {"content": {"application/json": {"example": {"value": 100, "units": "EUR"}}}}},
    dependencies=[Depends(ensure_user_logged_in)],
)
def quote(
    request: Request,
    parent_catalog: ParentCatalogue,
    catalog: OrderableCatalogue,
    collection: OrderableCollection,
    item: str,
    body: Annotated[
        QuoteRequest,
        Body(
            examples=[
                {
                    "coordinates": [
                        [[8.1, 31.7], [8.1, 31.6], [8.2, 31.9], [8.0, 31.5], [8.1, 31.7]]
                    ],
                    "licence": "Standard",
                }
            ],
        ),
    ],
):
    """Return a quote for a Planet or Airbus item ID within an EODH catalogue and collection.

    * coordinates: (optional) Coordinates to limit the AOI of the item for purchase where possible.
      Given in the same nested format as STAC
    * licence: (Airbus-only) The licence type for the order
    """

    coordinates = body.coordinates
    licence = validate_licence(collection.value, body.licence)

    order_url = str(request.url)
    base_item_url = order_url.rsplit("/quote", 1)[0]
    item_data = None

    if catalog.value == OrderableCatalogue.airbus.value:
        if collection.value == OrderableAirbusCollection.sar.value:
            if AIRBUS_ENV == "prod":
                url = "https://sar.api.oneatlas.airbus.com/v1/sar/prices"
            else:
                url = "https://dev.sar.api.oneatlas.airbus.com/v1/sar/prices"
            request_body = {"acquisitions": [item], "orderTemplate": licence.airbus_value}
        elif collection.value in {e.value for e in OrderableAirbusCollection}:
            url = "https://order.api.oneatlas.airbus.com/api/v1/prices"
            spectral_processing = "bundle"
            if collection.value == OrderableAirbusCollection.pneo.value:
                # Check if item is part of a multi order
                item_response = requests.get(base_item_url)
                item_response.raise_for_status()
                item_data = item_response.json()

                product_type = "PleiadesNeoArchiveMono"
                contract_id = "CTR24005241"
                datastrip_id = None
                item_uuids = [item_data.get("properties", {}).get("id")]
                if multi_ids := item_data.get("properties", {}).get(  # noqa: F841
                    "composed_of_acquisition_identifiers"
                ):
                    item_uuids = []
                    product_type = "PleiadesNeoArchiveMulti"
                    spectral_processing = "full_bundle"
                    return JSONResponse(
                        status_code=500,
                        content={"detail": "Multi and Stereo orders are not currently supported"},
                    )
                    # Unreachable code preparing item_uuids, awaiting further implementation of multi orders.
                    for multi_id in multi_ids:
                        multi_url = f"{base_item_url}/../{multi_id}"
                        multi_response = requests.get(multi_url)
                        multi_response.raise_for_status()
                        multi_data = multi_response.json()
                        item_uuids.append(multi_data.get("properties", {}).get("id"))
            elif collection.value == OrderableAirbusCollection.phr.value:
                product_type = "PleiadesArchiveMono"
                contract_id = "UNIVERSITY_OF_LEICESTER_Orders"
                item_uuids = None
                datastrip_id = item
            elif collection.value == OrderableAirbusCollection.spot.value:
                product_type = "SPOTArchive1.5Mono"
                contract_id = "UNIVERSITY_OF_LEICESTER_Orders"
                item_uuids = None
                datastrip_id = item
            if not coordinates:
                # Fetch coordinates from the STAC item
                if not item_data:
                    item_response = requests.get(base_item_url)
                    item_response.raise_for_status()
                    item_data = item_response.json()
                coordinates = item_data["geometry"]["coordinates"]

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
                            {"key": "licence", "value": licence.airbus_value},
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

            if datastrip_id:
                request_body["items"][0]["datastripIds"] = [datastrip_id]
        else:
            return JSONResponse(
                status_code=404,
                content={
                    "detail": f"Collection {collection.value} not recognised as an Airbus collection"
                },
            )
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
        if collection.value == OrderableAirbusCollection.sar.value:
            for response_item in response_body:
                if response_item.get("acquisitionId") == item:
                    price_json = {
                        "units": response_item["price"]["currency"],
                        "value": response_item["price"]["total"],
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

    elif catalog.value == OrderableCatalogue.planet.value:
        try:
            area = planet_client.get_area_estimate(item, collection.value, coordinates)

            if collection.value == "SkySatScene" and area < 3:
                # SkySatScene has a minimum order size of 3 km2
                area = 3

            return QuoteResponse(value=area, units="km2")

        except Exception as e:
            return JSONResponse(content={"message": str(e)}, status_code=400)

    else:
        return JSONResponse(
            content={"message:" f"{catalog.value} not recognised"},
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


# Support multiple paths for backward compatibility. Set to commercial when data is properly ingested
@app.get(
    "/stac/catalogs/commercial/catalogs/airbus/collections/{collection}/items/{item}/thumbnail",
    dependencies=[Depends(ensure_user_logged_in)],
)
@app.get(
    "/stac/catalogs/supported-datasets/catalogs/airbus/collections/{collection}/items/{item}/thumbnail",
    dependencies=[Depends(ensure_user_logged_in)],
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


# Support multiple paths for backward compatibility. Set to commercial when data is properly ingested
@app.get(
    "/stac/catalogs/commercial/catalogs/airbus/collections/{collection}/items/{item}/quicklook",
    dependencies=[Depends(ensure_user_logged_in)],
)
@app.get(
    "/stac/catalogs/supported-datasets/catalogs/airbus/collections/{collection}/items/{item}/quicklook",
    dependencies=[Depends(ensure_user_logged_in)],
)
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


@app.get("/stac/catalogs/commercial/catalogs/airbus/collections/{collection}/thumbnail")
@app.get("/stac/catalogs/supported-datasets/catalogs/airbus/collections/{collection}/thumbnail")
async def get_airbus_collection_thumbnail(collection: str):
    """Endpoint to get the thumbnail of an Airbus collection"""
    # Thumbnail is a local file, return it directly
    thumbnail_path = f"resource_catalogue_fastapi/thumbnails/{collection}.jpg"
    if not os.path.exists(thumbnail_path):
        raise HTTPException(status_code=404, detail="Thumbnail not found")
    return FileResponse(thumbnail_path)
