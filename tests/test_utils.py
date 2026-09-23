"""Tests for get_user_details' JWT signature verification.

The bug being fixed here: jwt.decode was called with options={"verify_signature": False},
so any claims in a Bearer token - including "workspaces" - were trusted without the token
ever having to be genuinely signed by Keycloak. This file is the one place that checks the
signature verification itself actually rejects a forged token, since a bug here lets every
workspace-access check elsewhere in the app be bypassed with whatever claims an attacker
likes.

There is no private key for the real Keycloak instance, so these sign with a throwaway RSA
keypair and mock resource_catalogue_fastapi.utils._jwks_client() to hand back its public
half, rather than calling the real endpoint over the network.
"""

import types
from collections.abc import Iterator
from unittest.mock import patch

import jwt
import jwt.utils
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from fastapi import HTTPException
from starlette.requests import Request

from resource_catalogue_fastapi import utils

PRIVATE_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _request(authorization: str | None) -> Request:
    headers = []
    if authorization is not None:
        headers.append((b"authorization", authorization.encode()))
    return Request({"type": "http", "headers": headers, "path_params": {}})


def _token(key: RSAPrivateKey, aud: str = "eodh", **claims: object) -> str:
    return jwt.encode(
        {"sub": "test-user", "preferred_username": "test-user", "aud": aud, **claims}, key, algorithm="RS256"
    )


@pytest.fixture(autouse=True)
def mock_jwks() -> Iterator[None]:
    """Stands in for a real call to Keycloak: hands back our own throwaway public key."""
    with patch.object(utils, "_jwks_client") as mock_client:
        mock_client.return_value.get_signing_key_from_jwt.return_value = types.SimpleNamespace(
            key=PRIVATE_KEY.public_key()
        )
        yield


def test_a_genuinely_signed_token_is_accepted() -> None:
    token = _token(PRIVATE_KEY, workspaces=["test_workspace"])

    username, workspaces = utils.get_user_details(_request(f"Bearer {token}"))

    assert username == "test-user"
    assert workspaces == ["test_workspace"]


def test_a_forged_signature_is_rejected() -> None:
    """This is the exact bug that shipped: verify_signature was False, so any signature -
    including one that is not cryptographically valid at all - was accepted.
    """
    header = jwt.utils.base64url_encode(b'{"alg":"RS256","typ":"JWT"}').decode()
    payload = jwt.utils.base64url_encode(
        b'{"sub":"attacker","preferred_username":"attacker","workspaces":["test_workspace"],"aud":"eodh"}'
    ).decode()
    forged_signature = jwt.utils.base64url_encode(b"not-a-real-signature").decode()
    forged_token = f"{header}.{payload}.{forged_signature}"

    with pytest.raises(HTTPException) as raised:
        utils.get_user_details(_request(f"Bearer {forged_token}"))

    assert raised.value.status_code == 401


def test_a_token_signed_by_a_different_key_is_rejected() -> None:
    """Guards against accepting any valid-looking signature rather than specifically
    Keycloak's: a token signed end-to-end correctly, just with the wrong key.
    """
    other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    token = _token(other_key, workspaces=["test_workspace"])

    with pytest.raises(HTTPException) as raised:
        utils.get_user_details(_request(f"Bearer {token}"))

    assert raised.value.status_code == 401


def test_the_wrong_audience_is_rejected() -> None:
    """A genuinely signed token issued for a different client should not be accepted here."""
    token = _token(PRIVATE_KEY, aud="some-other-client", workspaces=["test_workspace"])

    with pytest.raises(HTTPException) as raised:
        utils.get_user_details(_request(f"Bearer {token}"))

    assert raised.value.status_code == 401


def test_no_token_returns_anonymous_user() -> None:
    username, workspaces = utils.get_user_details(_request(None))

    assert username == ""
    assert workspaces == []
