import json
import logging
import os
from distutils.util import strtobool

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pulsar import Client as PulsarClient

from .utils import (
    check_policy,
    get_file_from_url,
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

# OPA service endpoint
OPA_SERVICE_ENDPOINT = os.getenv(
    "OPA_SERVICE_ENDPOINT", "http://opal-client.opal:8181/v1/data/ades/allow"
)

# Used for local testing where OPA is not available
ENABLE_OPA_POLICY_CHECK = strtobool(os.getenv("ENABLE_OPA_POLICY_CHECK", "false"))

# Root path for FastAPI
RC_FASTAPI_ROOT_PATH = os.getenv("RC_FASTAPI_ROOT_PATH", "/api/catalogue/manage")

# ADES Policy bucket name
ADES_POLICY_BUCKET = os.getenv("ADES_POLICY_BUCKET", "ades-policy")

# Job owner storage directory
JOB_OWNER_STORAGE_DIR = os.getenv("JOB_OWNER_STORAGE_DIR", "/job-data")

PULSAR_URL = os.environ.get("PULSAR_URL", "pulsar://pulsar-broker.pulsar:6650")
pulsar_client = PulsarClient(PULSAR_URL)

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

# Define global variables to store job and workspace owners
job_deployed_workspace = {}  # used to link workflow id and job id to deploying workspace
job_owners = {}  # used to link deploying workspace and job id to executing user

# Load job owners from file
if os.path.exists(f"{JOB_OWNER_STORAGE_DIR}/job-owners.json"):
    with open(f"{JOB_OWNER_STORAGE_DIR}/job-owners.json", "r") as file:
        job_owners = json.load(file)
else:
    job_owners = {}


# Load job workspace owners from file
if os.path.exists(f"{JOB_OWNER_STORAGE_DIR}/job-workspace-owners.json"):
    with open(f"{JOB_OWNER_STORAGE_DIR}/job-workspace-owners.json", "r") as file:
        job_deployed_workspace = json.load(file)
else:
    job_deployed_workspace = {}


def opa_dependency(request: Request, path_params: dict = Depends(get_path_params)):  # noqa: B008
    if ENABLE_OPA_POLICY_CHECK:
        if not check_policy(
            request,
            path_params,
            job_deployed_workspace,
            job_owners,
            OPA_SERVICE_ENDPOINT,
            ADES_POLICY_BUCKET,
        ):
            raise HTTPException(status_code=403, detail="Access denied")


@app.post("/catalogs/user-datasets/{workspace}", dependencies=[Depends(opa_dependency)])
async def create_item(
    workspace: str,
    request: Request,
    rate_limiter=Depends(rate_limiter_dependency),  # noqa: B008
):
    """Endpoint to create a new item in a workspace's collection"""

    headers = dict(request.headers)
    headers.pop("Authorization", None)

    # if "application/json" in request.headers["Content-Type"]:
    #     logger.info("Deploying workflow from URL to workspace %s", workspace)
    #     # Handle HTTP file content
    #     data = {"json": await request.json()}
    #     workflow_id = get_workflow_id_from_url(data["json"]["executionUnit"]["href"])
    # elif "application/cwl+yaml" in request.headers["Content-Type"]:
    #     logger.info("Deploying workflow from CWL file content to workspace %s", workspace)
    #     # Handle string file content
    #     file_content = await request.body()

    #     # Prepare the payload for the subsequent API request
    #     data = {"data": file_content}  # Decode bytes to string
    #     workflow_id = get_workflow_id_from_file(data["data"])
    # else:
    #     raise HTTPException(
    #         status_code=415,
    #         detail="Unsupported content type. Only application/json and application/cwl+yaml are supported",
    #     )
    # Test example - should become input from the user
    url = "https://dev.eodatahub.org.uk/api/catalogue/stac/catalogs/supported-datasets/airbus/collections/airbus_sar_data/items/TDX-1_SL_D_spot_029R_75359_A12865033_1711"
    body = get_file_from_url(url)

    workspace_key = f"{workspace}/saved-data/{url.split('/', 8)[-1]}.json"

    logger.info(f"Uploading item to workspace {workspace} with key {workspace_key}")

    # Upload item to S3
    s3_bucket = os.getenv("S3_BUCKET", "eodhp-dev-workspaces")
    upload_file_s3(body, s3_bucket, workspace_key)

    logger.info("Item uploaded successfully")
    output_data = {
        "id": f"{workspace}/create_item",
        "workspace": workspace,
        "bucket_name": s3_bucket,
        "added_keys": [workspace_key],
        "updated_keys": [],
        "deleted_keys": [],
        "source": "https://dev.eodatahub.org.uk/api/catalogue/stac/catalogs/supported-datasets",
        "target": f"https://{workspace}.workspaces.dev.eodhp.eco-ke-staging.com/files/eodhp-dev-workspaces/saved-data",
    }
    producer = pulsar_client.create_producer(
        topic="harvested", producer_name="resource_catalogue_fastapi"
    )
    producer.send((json.dumps(output_data)).encode("utf-8"))

    return JSONResponse(content={"message": "Item created successfully"}, status_code=201)
