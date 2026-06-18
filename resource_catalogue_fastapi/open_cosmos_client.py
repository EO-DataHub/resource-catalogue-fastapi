import logging
import os

import requests
from fastapi import HTTPException

logger = logging.getLogger(__name__)


class OpenCosmosClient:
    """Client for Open Cosmos API"""

    def __init__(self) -> None:
        self.base_url = os.getenv("OPEN_COSMOS_BASE_URL", "https://app.open-cosmos.com")
        self.client_id = os.getenv("OPEN_COSMOS_CLIENT_ID")
        self.client_secret = os.getenv("OPEN_COSMOS_CLIENT_SECRET")

    def generate_access_token(self) -> str | None:
        """Generate an OAuth2 access token for the Open Cosmos API"""
        url = "https://login.open-cosmos.com/oauth/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "audience": "https://beeapp.open-cosmos.com",
        }
        response = requests.post(url, json=data)
        response.raise_for_status()
        return response.json().get("access_token")

    def get_quote(self, item_id: str, collection_id: str, workspace: str) -> dict:
        """Get a price quote from the Open Cosmos pricing API.

        Placeholder — wire up the pricing endpoint URL and response mapping
        once the Open Cosmos pricing API is available.
        """
        raise NotImplementedError("Open Cosmos pricing API is not yet available")
