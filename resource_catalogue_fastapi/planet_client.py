import logging
import math
import os

import requests
from fastapi import HTTPException
from pyproj import Geod
from shapely.geometry import Polygon

logger = logging.getLogger(__name__)  # Add this line to define the logger


class PlanetClient:
    def __init__(self):
        pass

    def get_quote_from_planet(self, body: dict) -> str:
        """Get a quote from Planet API"""
        acquisition_ids = body["acquisitions"]
        collection_id = body["collection"]

        total_areas = 0

        for acquisition_id in acquisition_ids:

            try:
                feature = requests.get(
                    f"{os.environ['BASE_URL']}/api/catalogue/stac/catalogs/supported-datasets/planet/collections/{collection_id}/items/{acquisition_id}"
                ).json()
            except requests.exceptions.JSONDecodeError as e:
                raise HTTPException(404) from e

            coordinates = feature["geometry"]["coordinates"][0]  # double nested

            area = self.calculate_area(coordinates)

            total_areas += self.round_area(area)

        return {"value": total_areas, "units": "km2"}

    def calculate_area(self, coordinates: list) -> float:
        """Calculates area in km2 of polygon given a list of coordinates"""
        polygon = Polygon(coordinates)
        geod = Geod(ellps="WGS84")
        poly_area, _ = geod.geometry_area_perimeter(polygon)

        return abs(poly_area / 1e6)  # convert from m2 to km2, ensure value is positive

    def round_area(self, area: float) -> int:
        """Area is an estimate. Round up to the nearest integer."""
        return int(math.ceil(area))
