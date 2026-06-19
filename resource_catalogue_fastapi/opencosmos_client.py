import json
import logging
from base64 import b64decode
from datetime import datetime
from functools import cache
from typing import Annotated

import requests
from kubernetes import client, config
from .models import QuoteResponse
from pydantic import BaseModel, BeforeValidator

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


# kubectl -n ws-opencosmos-order-testing get secret oauth-opencosmos
@cache
def get_credentials(workspace: str) -> Credentials:
    provider = "opencosmos"

    # Initialize Kubernetes API client
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    namespace = f"ws-{workspace}"

    # Retrieve the credentials from Kubernetes Secrets
    logging.info("Fetching credentials from Kubernetes...")
    r = v1.read_namespaced_secret(f"oauth-{provider}", namespace)
    credentials = json.loads(r.data)

    # TODO: Check expiry and refresh if necessary.

    return Credentials(**credentials)


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

    for policy in policies:
        if policy["default_contract"]:
            contract_id = policy["contract_id"]
            return ContractInfo(contract_id=contract_id, organisation_id=credentials.organization_id)

    # If we don't have a default contract, use the first one.
    contract_id = policies[0]["contract_id"]
    return ContractInfo(contract_id=contract_id, organisation_id=credentials.organization_id)


def _format_errors(errors: list[dict[str, str]]) -> str:
    return "\n".join(
        error["message"] for error in errors
    )


def opencosmos_get_quote(workspace: str, collection_id: str, item_id: str) -> QuoteResponse:
    credentials = get_credentials(workspace)
    contract_info = get_contract_info(workspace)

    headers = {"Authorization": f"Bearer {credentials.access_token}"}
    r = requests.get(
        "https://app.open-cosmos.com/api/data/v0/order/price",
        params={"contract_id": contract_info.contract_id, "collection": collection_id, "item": item_id},
        headers=headers,
    )

    j = r.json()

    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        e.detail = _format_errors(j["errors"])
        raise e

    quote = j["data"]

    return QuoteResponse(value=quote["final"], units=quote["currency"], message=_format_errors(j["errors"]))
