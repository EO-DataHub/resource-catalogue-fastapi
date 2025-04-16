import base64
import json
import logging
import os

import requests
from fastapi import HTTPException
from typing import Optional, Tuple
from kubernetes import client, config
from .utils import get_api_key

logger = logging.getLogger(__name__)  # Add this line to define the logger


class AirbusClient:
    """Client for Airbus API and Airbus-specific functions"""

    def __init__(self, airbus_env: str):
        self.airbus_env = airbus_env
        self.airbus_api_key = os.getenv("AIRBUS_API_KEY")

    def generate_access_token(self, workspace: str = "") -> str:
        """Generate access token for Airbus API"""
        if self.airbus_env == "prod":
            url = "https://authenticate.foundation.api.oneatlas.airbus.com/auth/realms/IDP/protocol/openid-connect/token"
        else:
            url = "https://authenticate-int.idp.private.geoapi-airbusds.com/auth/realms/IDP/protocol/openid-connect/token"

        if workspace:
            api_key = get_api_key("airbus", workspace)
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
        logger.debug(f"Airbus API request: {url}, {body}")
        response = requests.post(url, json=body, headers=headers)
        logger.info(f"Airbus API response: {response.json()}")
        response.raise_for_status()
        return response.json()

    def validate_country_code(self, country_code: str):
        """Ensure that a given country code is valid against current Airbus API"""
        url = "https://order.api.oneatlas.airbus.com/api/v1/properties"
        access_token = self.generate_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}
        properties_response = requests.get(url, headers=headers)
        properties = properties_response.json().get("properties")

        countries = next((prop["values"] for prop in properties if prop["key"] == "countries"), [])
        country_ids = [country["id"] for country in countries]

        if country_code not in country_ids:
            valid_codes = ", ".join(country_ids)
            raise HTTPException(
                status_code=400,
                detail=f"End user country code {country_code} is invalid. Valid codes are: {valid_codes}",
            )

    def get_contract_id(
        self, workspace: str, collection_id: str
    ) -> Tuple[Optional[str], Optional[str]]:
        """Retrieve the correct contract ID that is stored in a secret within the workspace.
        Each collection is associated with different contract_ids"""

        contract_id = None

        try:
            config.load_incluster_config()
            v1 = client.CoreV1Api()
            secret = v1.read_namespaced_secret("otp-airbus", f"ws-{workspace}")

        except client.exceptions.ApiException as e:
            logger.error(f"Error fetching secret: {e}")
            return None, "Linked Account not found."

        # Check the contract data - Airbus only
        contracts_b64 = secret.data.get("contracts")

        if contracts_b64 is None:
            return (
                None,
                f"No contract ID is found in workspace {workspace} for airbus",
            )

        contracts = json.loads(base64.b64decode(contracts_b64).decode("utf-8"))
        contracts_optical = contracts.get("optical")
        contracts_sar = contracts.get("sar")

        if collection_id == "airbus_sar_data":
            if not contracts_sar:
                return (
                    None,
                    f"Collection {collection_id} not available to order for workspace {workspace}.",
                )
        else:
            if not contracts_optical:
                return (
                    None,
                    f"""Collection {collection_id} not available to order for workspace {workspace}.
                    No Airbus Optical contract ID found""",
                )

            if collection_id == "airbus_pneo_data":
                # PNEO Contract
                contract_id = next(
                    (key for key, value in contracts_optical.items() if "PNEO" in value), None
                )
                if contract_id is not None:
                    return contract_id, None
                else:
                    return None, f"Airbus PNEO contract ID not found in workspace {workspace}"

            if collection_id in ["airbus_phr_data", "airbus_spot_data"]:

                # LEGACY Contract
                contract_id = next(
                    (key for key, value in contracts_optical.items() if "LEGACY" in value), None
                )

                if contract_id is not None:
                    return contract_id, None
                else:
                    return None, f"Airbus LEGACY contract ID not found in workspace {workspace}"

        return contract_id, None
