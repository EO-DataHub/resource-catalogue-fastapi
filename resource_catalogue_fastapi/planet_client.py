import logging
import math
import os
import traceback

import requests
from fastapi import HTTPException
from pyproj import Geod
from shapely import GEOSException
from shapely.geometry import Polygon

logger = logging.getLogger(__name__)  # Add this line to define the logger


class PlanetClient:
    """Client for Planet API and Planet-specific functions"""

    def __init__(self):
        pass

    def get_area_estimate(self, acquisition_id: str, collection_id: str, aoi: list = ()) -> float:
        """Calculate the area intersection between an acquisition ID and an AOI (if provided), to be used
        as an estimate for the Planet area quote"""

        try:
            feature = requests.get(
                f"{os.environ['PLANET_BASE_URL'].rstrip('/')}/collections/{collection_id}/items/{acquisition_id}"
            ).json()
        except requests.exceptions.JSONDecodeError as e:
            raise HTTPException(404) from e

        coordinates = feature["geometry"]["coordinates"][0]  # double nested

        if aoi:
            aoi_structure = Polygon(aoi[0])  # also double nested
            coordinates_structure = Polygon(coordinates)

            try:
                intersection_coordinates = aoi_structure.intersection(coordinates_structure)
                area = self.calculate_area(intersection_coordinates)

            except GEOSException:
                logging.error(traceback.format_exc())
                raise Exception(
                    "Invalid input. Check that coordinates are not self-intersecting polygons"
                ) from None

        else:
            area = self.calculate_area(coordinates)

        return self.round_area(area)

    def calculate_area(self, coordinates: list) -> float:
        """Calculates area in km2 of polygon given a list of coordinates"""
        polygon = Polygon(coordinates)
        geod = Geod(ellps="WGS84")
        poly_area, _ = geod.geometry_area_perimeter(polygon)

        return abs(poly_area / 1e6)  # convert from m2 to km2, ensure value is positive

    def round_area(self, area: float) -> int:
        """Area is an estimate. Round up to the nearest integer."""
        return int(math.ceil(area))
