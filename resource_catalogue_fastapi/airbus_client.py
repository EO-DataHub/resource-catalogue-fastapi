import base64
import logging
import os

import requests
from kubernetes import client, config

logger = logging.getLogger(__name__)  # Add this line to define the logger


class AirbusClient:
    def __init__(self, airbus_env: str):
        self.airbus_env = airbus_env
        self.airbus_api_key = os.getenv("AIRBUS_API_KEY")

    def get_secret_airbus_api_key(self, username):
        """Get API key from secrets for a given user"""
        config.load_incluster_config()
        api_instance = client.CustomObjectsApi()

        # Find the namespace for the user from the workspace CRD
        workspace = api_instance.get_namespaced_custom_object(
            group="core.telespazio-uk.io",
            version="v1alpha1",
            namespace="workspaces",
            plural="workspaces",
            name=username,
        )
        namespace = workspace["spec"]["namespace"]

        # Get the secret in the user namespace
        v1 = client.CoreV1Api()
        secret = v1.read_namespaced_secret("api-keys", namespace)
        api_key = base64.b64decode(secret.data["airbus-key"]).decode("utf-8")
        return api_key

    def generate_access_token(self, username: str = "") -> str:
        """Generate access token for Airbus API"""
        if self.airbus_env == "prod":
            url = "https://authenticate.foundation.api.oneatlas.airbus.com/auth/realms/IDP/protocol/openid-connect/token"
        else:
            url = "https://authenticate-int.idp.private.geoapi-airbusds.com/auth/realms/IDP/protocol/openid-connect/token"

        if username:
            api_key = self.get_secret_airbus_api_key(username)
        else:
            api_key = self.airbus_api_key

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = [
            ("apikey", api_key),
            ("grant_type", "api_key"),
            ("client_id", "IDP"),
        ]

        response = requests.post(url, headers=headers, data=data)

        return response.json().get("access_token")

    def get_quote_from_airbus(self, url: str, body: dict, headers: dict) -> dict:
        """Get a quote from Airbus API"""
        response = requests.post(url, json=body, headers=headers)
        response.raise_for_status()
        return response.json()
