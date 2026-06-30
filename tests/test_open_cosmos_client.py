import base64
from collections.abc import Iterator
from datetime import datetime
from typing import Any
from unittest import mock

import pytest
from requests.models import Response

from resource_catalogue_fastapi.models import QuoteResponse
from resource_catalogue_fastapi.open_cosmos_client import (
    Credentials,
    get_contract_info,
    get_credentials,
    open_cosmos_get_quote,
    val_int,
    val_str,
    val_timestamp,
)


def _b64(value: str) -> str:
    """Encode a string the way the Open Cosmos k8s secret stores each field."""
    return base64.b64encode(value.encode("utf-8")).decode("utf-8")


# Timestamps in milliseconds since the epoch (how Open Cosmos encodes expiry).
_PAST_MS = "1700000000000"  # 2023-11-14, used for decode assertions
_FUTURE_MS = "4102444800000"  # 2100-01-01, safely unexpired


def _credentials_payload(expires_ms: str = _FUTURE_MS) -> dict[str, str]:
    return {
        "access_token": _b64("test_access_token"),
        "expires_at": _b64(expires_ms),
        "organization_id": _b64("42"),
        "refresh_token": _b64("test_refresh_token"),
        "scope": _b64("read write"),
        "token_type": _b64("Bearer"),
    }


@pytest.fixture(autouse=True)
def clear_caches() -> Iterator[None]:
    """get_contract_info is @cache decorated, so clear between tests to avoid
    leaking results across cases. (get_credentials is deliberately uncached.)"""
    get_contract_info.cache_clear()
    yield
    get_contract_info.cache_clear()


# ---------------------------------------------------------------------------
# Field validators
# ---------------------------------------------------------------------------


def test_val_str_decodes_base64() -> None:
    assert val_str(_b64("hello")) == "hello"


def test_val_int_decodes_base64() -> None:
    assert val_int(_b64("123")) == 123


def test_val_timestamp_decodes_milliseconds() -> None:
    assert val_timestamp(_b64(_PAST_MS)) == datetime.fromtimestamp(1700000000.0)


def test_credentials_model_decodes_all_fields() -> None:
    credentials = Credentials(**_credentials_payload(expires_ms=_PAST_MS))  # pyright: ignore

    assert credentials.access_token == "test_access_token"
    assert credentials.refresh_token == "test_refresh_token"
    assert credentials.organization_id == 42
    assert credentials.scope == "read write"
    assert credentials.token_type == "Bearer"
    assert credentials.expires_at == datetime.fromtimestamp(1700000000.0)


# ---------------------------------------------------------------------------
# get_credentials (Kubernetes secret lookup)
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_k8s_credentials() -> Iterator[Any]:
    with (
        mock.patch("resource_catalogue_fastapi.open_cosmos_client.config.load_incluster_config"),
        mock.patch("resource_catalogue_fastapi.open_cosmos_client.client.CoreV1Api") as mock_core_v1_api,
    ):
        mock_instance = mock.Mock()
        # The kubernetes client deserialises V1Secret.data to a dict[str, str]
        # of base64 values, not a JSON string.
        mock_instance.read_namespaced_secret.return_value = mock.Mock(data=_credentials_payload())
        mock_core_v1_api.return_value = mock_instance
        yield mock_instance


def test_get_credentials_reads_secret_from_workspace_namespace(mock_k8s_credentials: Any) -> None:
    credentials = get_credentials("open-cosmos-order-testing")

    assert credentials.access_token == "test_access_token"
    assert credentials.organization_id == 42
    mock_k8s_credentials.read_namespaced_secret.assert_called_once_with(
        "oauth-open-cosmos", "ws-open-cosmos-order-testing"
    )


def test_get_credentials_reads_secret_every_call(mock_k8s_credentials: Any) -> None:
    # No caching: the secret is read fresh each time so externally-refreshed
    # credentials are picked up immediately.
    get_credentials("workspace-a")
    get_credentials("workspace-a")

    assert mock_k8s_credentials.read_namespaced_secret.call_count == 2


def test_get_credentials_returns_unexpired_token_without_refreshing(mock_k8s_credentials: Any) -> None:
    with mock.patch("resource_catalogue_fastapi.open_cosmos_client.refresh_credentials") as mock_refresh:
        credentials = get_credentials("workspace-a")

    assert credentials.access_token == "test_access_token"
    mock_refresh.assert_not_called()


def test_get_credentials_refreshes_when_expired() -> None:
    refreshed = mock.Mock(access_token="fresh_access_token")
    with (
        mock.patch("resource_catalogue_fastapi.open_cosmos_client.config.load_incluster_config"),
        mock.patch("resource_catalogue_fastapi.open_cosmos_client.client.CoreV1Api") as mock_core_v1_api,
        mock.patch(
            "resource_catalogue_fastapi.open_cosmos_client.refresh_credentials",
            return_value=refreshed,
        ) as mock_refresh,
    ):
        mock_instance = mock.Mock()
        mock_instance.read_namespaced_secret.return_value = mock.Mock(data=_credentials_payload(expires_ms=_PAST_MS))
        mock_core_v1_api.return_value = mock_instance

        credentials = get_credentials("workspace-a")

    assert credentials is refreshed
    mock_refresh.assert_called_once()


# ---------------------------------------------------------------------------
# get_contract_info
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_get_credentials() -> Iterator[Any]:
    with mock.patch("resource_catalogue_fastapi.open_cosmos_client.get_credentials") as mock_creds:
        mock_creds.return_value = mock.Mock(access_token="test_access_token", organization_id=42)
        yield mock_creds


def _policies_response(policies: list[dict[str, Any]]) -> Any:
    mock_response = mock.Mock(spec=Response)
    mock_response.json.return_value = {"data": policies}
    mock_response.raise_for_status.return_value = None
    return mock_response


def test_get_contract_info_returns_default_contract(mock_get_credentials: Any) -> None:
    policies = [
        {"contract_id": 100, "default_contract": False},
        {"contract_id": 200, "default_contract": True},
    ]
    with mock.patch(
        "resource_catalogue_fastapi.open_cosmos_client.requests.get",
        return_value=_policies_response(policies),
    ):
        info = get_contract_info("workspace-default")

    assert info.contract_id == 200
    assert info.organisation_id == 42


def test_get_contract_info_falls_back_to_first_contract(mock_get_credentials: Any) -> None:
    policies = [
        {"contract_id": 100, "default_contract": False},
        {"contract_id": 200, "default_contract": False},
    ]
    with mock.patch(
        "resource_catalogue_fastapi.open_cosmos_client.requests.get",
        return_value=_policies_response(policies),
    ):
        info = get_contract_info("workspace-no-default")

    assert info.contract_id == 100
    assert info.organisation_id == 42


# ---------------------------------------------------------------------------
# open_cosmos_get_quote
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_quote_dependencies() -> Iterator[None]:
    with (
        mock.patch(
            "resource_catalogue_fastapi.open_cosmos_client.get_credentials",
            return_value=mock.Mock(access_token="test_access_token", organization_id=42),
        ),
        mock.patch(
            "resource_catalogue_fastapi.open_cosmos_client.get_contract_info",
            return_value=mock.Mock(contract_id=200, organisation_id=42),
        ),
    ):
        yield


def test_open_cosmos_get_quote_returns_quote_response(mock_quote_dependencies: None) -> None:
    mock_response = mock.Mock(spec=Response)
    mock_response.json.return_value = {
        "data": {"final": 42.5, "currency": "GBP"},
        "errors": [],
    }
    mock_response.raise_for_status.return_value = None

    with mock.patch(
        "resource_catalogue_fastapi.open_cosmos_client.requests.get",
        return_value=mock_response,
    ):
        quote = open_cosmos_get_quote("workspace", "collection-1", "item-1")

    assert isinstance(quote, QuoteResponse)
    assert quote.value == 42.5
    assert quote.units == "GBP"
    assert quote.message == ""
