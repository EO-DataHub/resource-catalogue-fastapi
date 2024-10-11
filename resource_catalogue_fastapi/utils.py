import json
import logging
import time
import urllib.request
from urllib.request import urlopen

import boto3
import botocore
import jwt
import requests
from botocore.exceptions import ClientError
from fastapi import Depends, HTTPException, Request

logger = logging.getLogger(__name__)  # Add this line to define the logger


def get_path_params(request: Request):
    logger.debug("CALCULATING PATH PARAMETERS")
    return request.scope.get("path_params", {})


def save_data(job_owner_storage_dir: str, job_owners: dict, job_deployed_workspace: dict):
    """Save job owners to files"""
    with open(f"{job_owner_storage_dir}/job-owners.json", "w") as f:
        json.dump(job_owners, f)
    with open(f"{job_owner_storage_dir}/job-workspace-owners.json", "w") as f:
        json.dump(job_deployed_workspace, f)


def opa_request(input_data: dict, opa_service_endpoint: str) -> requests.Response:
    """Send a request to OPA service"""
    logger.debug("Sending request to OPA service with input: %s", input_data)
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


def get_access_policy(deploy_workspace: str, workflow_id: str, ades_policy_bucket: str) -> dict:
    """Get the access policy for a workflow"""
    s3_client = boto3.client("s3")

    deploy_workspace = convert_workspace_to_username(deploy_workspace)

    key = f"workflows/{deploy_workspace}/{workflow_id}.access-policy.json"

    try:
        s3_data = s3_client.get_object(Bucket=ades_policy_bucket, Key=key)
    except botocore.exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "NoSuchKey":
            logger.info(
                "No access policy found for workflow %s in workspace %s",
                workflow_id,
                deploy_workspace,
            )
            return None
        else:
            logger.info(
                "Error getting access policy for workflow %s in workspace %s",
                workflow_id,
                deploy_workspace,
            )
            return None
    return json.loads(s3_data.get("Body").read().decode("utf-8"))


def check_policy(
    request: Request,
    path_params: dict,
    job_deployed_workspace: dict,
    job_owners: dict,
    opa_service_endpoint: str,
    ades_policy_bucket: str,
) -> bool:
    """Check OPA policy to determine if the request is allowed"""
    username, roles = get_user_details(request)

    # Add values for logs
    logger.info("Logged in as user: %s", username)
    logger.info("Roles: %s", roles)

    input_data = {
        "input": {
            "roles": roles,
            "path": request.url.path,
            "username": username,
            "method": request.method,
        }
    }

    # Log path parameters
    logger.debug("Input parameters from path: %s", path_params)

    # Add values for workflow access
    if (
        request.method in ["POST", "GET"]
        and path_params.get("deploy_workspace")
        and path_params.get("workflow_id")
    ):
        # User is executing or interrogating a workflow
        logger.info("Checking policy for workflow access")
        # Get access policy for workflow
        access_policy = get_access_policy(
            path_params["deploy_workspace"],
            path_params["workflow_id"],
            ades_policy_bucket,
        )
        logger.info("Access policy is: %s", access_policy)
        if access_policy:
            input_data["input"]["is_public"] = access_policy["public"]
            input_data["input"]["allowed_users"] = access_policy["allowed_access"]["users"]
    elif path_params.get("deploy_workspace") and path_params.get("job_id"):
        # User is monitoring a job
        logger.info("Checking job owner for job access")
        # Get executing workspace for the given job
        deploy_workspace = path_params["deploy_workspace"]
        job_id = path_params["job_id"]

        workspace_job_key = f"{deploy_workspace}-{job_id}"

        # Get executing workspace for the given job
        execute_username = job_owners.get(workspace_job_key)

        execute_workspace = convert_username_to_workspace(execute_username)

        input_data["input"]["executing_user"] = execute_workspace
    elif request.method == "POST" and path_params.get("deploy_workspace"):
        # User is deploying a workflow
        logger.info("Checking policy for workflow deployment")
        # Get deploying workspace
        deploy_workspace = path_params["deploy_workspace"]
        input_data["input"]["deploying_user"] = deploy_workspace
    elif path_params.get("workflow_job_id"):
        # User is accessing logs
        logger.info("Checking job owner for logs access")
        # Get executing workspace for the given job
        workflow_job_id = path_params["workflow_job_id"]

        job_id = workflow_job_id[-36:]

        # Get deployed workspace for the given job
        deploy_workspace = job_deployed_workspace.get(workflow_job_id)

        # Get executing workspace for the given job
        execute_workspace = job_owners.get(f"{deploy_workspace}-{job_id}")

        input_data["input"]["executing_user"] = execute_workspace

    response = opa_request(input_data, opa_service_endpoint)
    if response.status_code != 200 or not response.json().get("result", False):
        return False
    return True


def convert_workspace_to_username(workspace: str) -> str:
    return workspace.replace("_", "-")


def convert_username_to_workspace(username: str) -> str:
    if not username:
        return ""
    return username.replace("-", "_")


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
    rate_limit(workspace)


def upload_file_s3(body: str, bucket: str, key: str):
    """Upload data to an S3 bucket."""
    try:
        s3_client = boto3.client("s3")
        s3_client.put_object(Body=body, Bucket=bucket, Key=key)
    except ClientError as e:
        logging.error(f"File upload failed: {e}")
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
