import logging
import os
from base64 import b64decode, b64encode
from datetime import datetime, timedelta
from functools import cache
from typing import Annotated

import requests
from fastapi import HTTPException
from kubernetes import client, config
from kubernetes.aio.client import V1Secret
from kubernetes.client.exceptions import ApiException
from pydantic import BaseModel, BeforeValidator

from .models import QuoteResponse

logger = logging.getLogger(__name__)


def val_timestamp(value: str) -> datetime:
    ts = float(b64decode(value).decode()) / 1000.0
    return datetime.fromtimestamp(ts)


def val_int(value: str) -> int:
    return int(b64decode(value).decode())


def val_str(value: str) -> str:
    return b64decode(value).decode()


class Credentials(BaseModel):
    access_token: Annotated[str, BeforeValidator(val_str)]
    expires_at: Annotated[datetime, BeforeValidator(val_timestamp)]
    organization_id: Annotated[int, BeforeValidator(val_int)]
    refresh_token: Annotated[str, BeforeValidator(val_str)]
    scope: Annotated[str, BeforeValidator(val_str)]
    token_type: Annotated[str, BeforeValidator(val_str)]


class ContractInfo(BaseModel):
    contract_id: int
    organisation_id: int


# Refresh slightly before the token actually expires so we never hand out a
# token that dies mid-request.
_EXPIRY_MARGIN = timedelta(minutes=5)
_PROVIDER = "open-cosmos"
_CLIENT_ID = os.getenv("OPEN_COSMOS_CLIENT_ID_REFRESH", "")


# kubectl -n ws-open-cosmos-order-testing get secret oauth-open-cosmos
def read_credentials(workspace: str) -> Credentials:
    """Read the current OAuth credentials from the workspace's Kubernetes secret."""

    try:
        # Initialize Kubernetes API client
        config.load_incluster_config()
        v1 = client.CoreV1Api()
        namespace = f"ws-{workspace}"

        logging.info("Fetching credentials from Kubernetes...")
        r: V1Secret = v1.read_namespaced_secret(f"oauth-{_PROVIDER}", namespace)  # pyright: ignore

        return Credentials(**r.data)  # pyright: ignore
    except ApiException as e:
        if e.status == 404:
            raise HTTPException(
                status_code=403,
                detail=f"No Open Cosmos credentials found for workspace '{workspace}'. "
                "Please link your Open Cosmos account in your workspace settings.",
            ) from e
        if e.status == 403:
            raise HTTPException(
                status_code=500,
                detail=f"Permission denied reading Open Cosmos credentials for workspace '{workspace}'.",
            ) from e
        raise HTTPException(status_code=500, detail=f"Failed to read Open Cosmos credentials: {e.reason}") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error reading Open Cosmos credentials: {e}") from e


def get_credentials(workspace: str) -> Credentials:
    """Return valid OAuth credentials for the workspace.

    The secret is read fresh on every call (no caching) so that credentials
    refreshed out-of-band by the credentials service are picked up immediately.
    If the access token has expired, it is refreshed before being returned.
    """
    credentials = read_credentials(workspace)

    if credentials.expires_at - _EXPIRY_MARGIN > datetime.now():
        return credentials

    logging.info("Open Cosmos access token has expired; refreshing.")
    return refresh_credentials(workspace, credentials)


def refresh_credentials(workspace: str, credentials: Credentials) -> Credentials:
    """Refresh the Open Cosmos access token and persist it for the workspace."""

    # Refresh the access token with Open Cosmos
    data = {"client_id": _CLIENT_ID, "grant_type": "refresh_token", "refresh_token": credentials.refresh_token}
    r = requests.post("https://login.open-cosmos.com/oauth/token", data=data)

    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise HTTPException(status_code=401, detail="Failed to refresh Open Cosmos access token.") from e
    j = r.json()

    expires_at = datetime.now() + timedelta(seconds=j["expires_in"])
    updated_credentials = credentials.model_copy(
        update={
            "access_token": j["access_token"],
            "expires_at": expires_at,
            "scope": j.get("scope", credentials.scope),
        }
    )

    # Persist the updated credentials to Kubernetes
    # Initialize Kubernetes API client
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    namespace = f"ws-{workspace}"

    def _encode(value: str) -> str:
        return b64encode(value.encode()).decode()

    secret_data = {
        "access_token": _encode(updated_credentials.access_token),
        "expires_at": _encode(str(int(updated_credentials.expires_at.timestamp() * 1000))),
        "organization_id": _encode(str(updated_credentials.organization_id)),
        "refresh_token": _encode(updated_credentials.refresh_token),
        "scope": _encode(updated_credentials.scope),
        "token_type": _encode(updated_credentials.token_type),
    }
    secret = client.V1Secret(data=secret_data)

    logging.info("Updating credentials in Kubernetes...")
    try:
        v1.replace_namespaced_secret(f"oauth-{_PROVIDER}", namespace, secret)
    except ApiException as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to persist refreshed Open Cosmos credentials: {e.reason}"
        ) from e

    # Reread the updated credentials from Kubernetes to ensure they are up-to-date.
    return read_credentials(workspace)


@cache
def get_contract_info(workspace: str) -> ContractInfo:
    """
    Get the contract ID and organisation ID for the default contract.
    There's currently no way to get the organisation ID from the API, so it will be stored in the k8s secrets.
    """

    credentials = get_credentials(workspace)

    headers = {"Authorization": f"Bearer {credentials.access_token}"}
    r = requests.get(
        f"https://app.open-cosmos.com/api/data/v1/dpap/organisations/{credentials.organization_id}/policies",
        headers=headers,
    )
    r.raise_for_status()
    policies = r.json()["data"]

    if len(policies) == 0:
        raise HTTPException(status_code=404, detail="No policies found for organization")

    for policy in policies:
        if policy["default_contract"]:
            contract_id = policy["contract_id"]
            return ContractInfo(contract_id=contract_id, organisation_id=credentials.organization_id)

    # If we don't have a default contract, use the first one.
    contract_id = policies[0]["contract_id"]
    return ContractInfo(contract_id=contract_id, organisation_id=credentials.organization_id)


def _format_errors(errors: list[dict[str, str]] | None) -> str:
    if not errors:
        return ""
    return "\n".join(error["message"] for error in errors)


def open_cosmos_get_quote(workspace: str, collection_id: str, item_id: str) -> QuoteResponse:
    credentials = get_credentials(workspace)
    contract_info = get_contract_info(workspace)

    headers = {"Authorization": f"Bearer {credentials.access_token}"}
    r = requests.get(
        "https://app.open-cosmos.com/api/data/v0/order/price",
        params={"contract_id": contract_info.contract_id, "collection": collection_id, "item": item_id},
        headers=headers,
    )

    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # It seems all responses from Open Cosmos have a JSON body with an "errors" field. But we can't be sure.
        # detail = _format_errors(j["errors"])
        raise HTTPException(status_code=404, detail=r.text) from e

    j = r.json()
    quote = j["data"]

    return QuoteResponse(value=quote["final"], units=quote["currency"], message=_format_errors(j["errors"]))
