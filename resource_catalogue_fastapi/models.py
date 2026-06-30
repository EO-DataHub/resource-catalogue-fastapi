import os
from enum import Enum, StrEnum
from typing import Any

from pydantic import BaseModel, Field


# TODO: This should be populated from the catalogue
class OrderableOpenCosmosCollection(StrEnum):
    accenture_1_l1c_cogs = "accenture-1-l1c-cogs"
    hammer_l1c_cogs = "hammer-l1c-cogs"
    mantis_l1d_cogs = "mantis-l1d-cogs"
    menut_l1a_cogs = "menut-l1a-cogs"
    menut_l1b_cogs = "menut-l1b-cogs"
    menut_l1c_cogs = "menut-l1c-cogs"
    platero_l1c_cogs = "platero-l1c-cogs"


# For some reason Planet collections come from an environment variable. Why not Airbus too?
PLANET_COLLECTIONS = os.getenv("PLANET_COLLECTIONS", "PSScene,SkySatCollect").split(",")


class ParentCatalogue(StrEnum):
    """Parent catalogue for commercial data in the resource catalogue"""

    supported_datasets = "supported-datasets"
    commercial = "commercial"


class OrderableCatalogue(StrEnum):
    """Catalogues for ordering commercial data"""

    planet = "planet"
    airbus = "airbus"
    open_cosmos = "open-cosmos"


OrderablePlanetCollection = Enum("OrderablePlanetCollection", {name: name for name in PLANET_COLLECTIONS})


class OrderableAirbusCollection(StrEnum):
    """Collections for ordering Airbus commercial data"""

    sar = "airbus_sar_data"
    pneo = "airbus_pneo_data"
    phr = "airbus_phr_data"
    spot = "airbus_spot_data"


# Combine the members of OrderableAirbusCollection, OrderablePlanetCollection, and OrderableOpenCosmosCollection
# In the most oddball way possible.
combined_collections = {e.name: e.value for e in OrderableAirbusCollection}
combined_collections.update({name: name for name in PLANET_COLLECTIONS})
combined_collections.update({e.name: e.value for e in OrderableOpenCosmosCollection})
OrderableCollection = Enum("OrderableCollection", combined_collections)


class ItemRequest(BaseModel):
    """Request body for create, update and delete item endpoints"""

    url: str
    extra_data: dict[str, Any] | None = Field(default_factory=dict)


class ProductBundle(StrEnum):
    """Product bundles for Planet and Airbus optical data"""

    general_use = "General Use"
    visual = "Visual"
    basic = "Basic"
    analytic = "Analytic"


product_bundle_no_aoi = [("SkySatCollect", ProductBundle.analytic)]


class ProductBundleRadar(StrEnum):
    """Product bundles for Airbus SAR data"""

    SSC = "SSC"
    MGD = "MGD"
    GEC = "GEC"
    EEC = "EEC"


class LicenceRadar(StrEnum):
    """Licence types for Airbus SAR data"""

    SINGLE = "Single User Licence"
    MULTI_2_5 = "Multi User (2 - 5) Licence"
    MULTI_6_30 = "Multi User (6 - 30) Licence"

    def airbus_value(self, collection: Enum) -> str:
        """Map the licence type to the Airbus API value"""
        # collection not used in this mapping, but kept for consistency
        mappings = {
            "Single User Licence": "Single User License",
            "Multi User (2 - 5) Licence": "Multi User (2 - 5) License",
            "Multi User (6 - 30) Licence": "Multi User (6 - 30) License",
        }
        return mappings[self.value]


class LicenceOptical(StrEnum):
    """Licence types for Airbus optical data"""

    STANDARD = "Standard"
    BACKGROUND = "Background Layer"
    STANDARD_BACKGROUND = "Standard + Background Layer"
    ACADEMIC = "Academic"
    MEDIA = "Media Licence"
    STANDARD_MULTI_2_5 = "Standard Multi End-Users (2-5)"
    STANDARD_MULTI_6_10 = "Standard Multi End-Users (6-10)"
    STANDARD_MULTI_11_30 = "Standard Multi End-Users (11-30)"
    STANDARD_MULTI_30 = "Standard Multi End-Users (>30)"

    def airbus_value(self, collection: Enum) -> str:
        """Map the licence type to the Airbus API value"""
        mappings = {
            "Standard": "standard",
            "Background Layer": "background_layer",
            "Standard + Background Layer": "stand_background_layer",
            "Academic": "educ",
            "Media Licence": "media",
            "Standard Multi End-Users (2-5)": "standard_1_5",
            "Standard Multi End-Users (6-10)": "standard_6_10",
            "Standard Multi End-Users (11-30)": "standard_11_30",
            "Standard Multi End-Users (>30)": "standard_up_30",
        }
        if collection.value == OrderableAirbusCollection.pneo.value:
            # PNEO collection uses different licence names
            mappings = {
                "Standard": "standard",
                "Background Layer": "background_layer",
                "Standard + Background Layer": "stand_background_layer",
                "Academic": "Academic",
                "Media Licence": "media",
                "Standard Multi End-Users (2-5)": "Standard_1_5",
                "Standard Multi End-Users (6-10)": "Standard_6_10",
                "Standard Multi End-Users (11-30)": "Standard_11_30",
                "Standard Multi End-Users (>30)": "Standard_up_30",
            }

        return mappings[self.value]


class Orbit(StrEnum):
    """Orbit types for Airbus SAR data"""

    RAPID = "rapid"
    SCIENCE = "science"


class ResolutionVariant(StrEnum):
    """Resolution variants for Airbus SAR data"""

    RE = "RE"
    SE = "SE"


class Projection(StrEnum):
    """Projection types for Airbus SAR data"""

    AUTO = "Auto"
    UTM = "UTM"
    UPS = "UPS"

    @property
    def airbus_value(self) -> str:
        """Map the projection type to the Airbus API value"""
        mappings = {
            "Auto": "auto",
            "UTM": "UTM",
            "UPS": "UPS",
        }
        return mappings[self.value]


class RadarOptions(BaseModel):
    """Radar options for Airbus SAR data"""

    orbit: Orbit
    resolutionVariant: ResolutionVariant | None = None
    projection: Projection | None = None

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        """Return the model data as a dictionary to send to the Airbus API"""
        data = super().model_dump(**kwargs)
        data = {k: v for k, v in data.items() if v is not None}
        if self.projection:
            data["projection"] = self.projection.airbus_value
        return data


class OrderRequest(BaseModel):
    """Request body for order endpoint"""

    productBundle: ProductBundle | ProductBundleRadar
    coordinates: list | None = Field(default_factory=list)
    endUserCountry: str | None = None
    licence: LicenceOptical | LicenceRadar | None = None
    radarOptions: RadarOptions | None = None


class QuoteRequest(BaseModel):
    """Request body for quote endpoint"""

    coordinates: list | None = None
    licence: LicenceOptical | LicenceRadar | None = None
    productBundle: ProductBundle | None = None


class QuoteResponse(BaseModel):
    """Response body for quote endpoint"""

    value: float
    units: str
    message: str | None = None
