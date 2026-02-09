import pytest

from resource_catalogue_fastapi.planet_client import PlanetClient


@pytest.fixture
def planet_client() -> PlanetClient:
    return PlanetClient()


def test_calculate_area(planet_client: PlanetClient) -> None:
    coordinates = [[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]
    area = planet_client.calculate_area(coordinates)

    assert area == 12308.778361469453


@pytest.mark.parametrize(
    ("area", "expected_approximation"),
    [
        pytest.param(0.9, 1),
        pytest.param(1, 1),
        pytest.param(1.1, 2),
        pytest.param(1.001, 2),
    ],
)
def test_round_area(planet_client: PlanetClient, area: float, expected_approximation: int) -> None:
    approx_area = planet_client.round_area(area)

    assert approx_area == expected_approximation
