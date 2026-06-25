import logging
import os
from base64 import b64decode
from datetime import datetime, timedelta
from functools import cache
from typing import Annotated, Any

import requests
from fastapi import HTTPException
from kubernetes import client, config
from kubernetes.aio.client import V1Secret
from kubernetes.client.exceptions import ApiException
from pydantic import BaseModel, BeforeValidator

from .models import QuoteResponse

logger = logging.getLogger(__name__)


def val_timestamp(value: str) -> datetime:
    ts = float(b64decode(value)) / 1000.0
    return datetime.fromtimestamp(ts)


def val_int(value: str) -> int:
    return int(b64decode(value))


def val_str(value: str) -> str:
    return b64decode(value).decode("utf-8")


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

# Base URL of the workspace-services API that owns the oauth-open-cosmos secret.
# It runs alongside this API, so this is expected to be set at runtime; the
# placeholder default keeps things importable until the real address is wired in.
WORKSPACE_SERVICES_URL = os.getenv("WORKSPACE_SERVICES_URL", "http://workspace-services-placeholder/api")


# kubectl -n ws-open-cosmos-order-testing get secret oauth-open-cosmos
def read_credentials(workspace: str) -> Credentials:
    """Read the current OAuth credentials from the workspace's Kubernetes secret."""
    provider = "open-cosmos"

    try:
        # Initialize Kubernetes API client
        config.load_incluster_config()
        v1 = client.CoreV1Api()
        namespace = f"ws-{workspace}"

        logging.info("Fetching credentials from Kubernetes...")
        r: V1Secret = v1.read_namespaced_secret(f"oauth-{provider}", namespace)  # pyright: ignore

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


def _request_refreshed_session(credentials: Credentials) -> dict[str, Any]:
    """Exchange the refresh token for a new Open Cosmos access token.

    Expected to return the fields needed to build the workspace-services session
    payload: ``access_token``, ``refresh_token``, ``expires_at`` (milliseconds
    since the epoch), and optionally ``scope`` and ``token_type``.

    TODO: the Open Cosmos token refresh endpoint is not yet known; stubbed until
    then so an expired token is an explicit error rather than a confusing 401.
    """
    raise NotImplementedError("Open Cosmos token refresh endpoint is not yet known.")


def refresh_credentials(workspace: str, credentials: Credentials) -> Credentials:
    """Refresh the Open Cosmos access token and persist it for the workspace.

    1. Exchange the refresh token for a new session with Open Cosmos.
    2. POST the new session to workspace-services, which writes it back to the
       workspace's Kubernetes secret (oauth-open-cosmos).
    3. Re-read and return the now-current credentials from the secret.
    """
    session = _request_refreshed_session(credentials)

    # Matches workspace-services' OpenCosmosSessionPayload. Note expiresAt is an
    # int64 of milliseconds since the epoch, and organization_id is snake_case.
    payload = {
        "accessToken": session["access_token"],
        "refreshToken": session["refresh_token"],
        "expiresAt": session["expires_at"],
        "scope": session.get("scope", ""),
        "tokenType": session.get("token_type", ""),
        "organization_id": credentials.organization_id,
    }

    # TODO: workspace-services requires BearerAuth; wire up the token once known.
    headers: dict[str, str] = {}

    r = requests.post(
        f"{WORKSPACE_SERVICES_URL}/workspaces/{workspace}/open-cosmos/session",
        json=payload,
        headers=headers,
    )

    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise HTTPException(status_code=401) from e

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
