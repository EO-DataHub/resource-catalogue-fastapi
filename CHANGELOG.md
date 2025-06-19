# Changelog

## v0.1.23 (2025-06-19)

Added tags and updated OpenAPI documentation

## v0.1.22 (2025-05-12)

Update licence enum for PNEO items

## v0.1.21 (2025-05-30)

Keep legacy endpoints for outdated apps

## v0.1.20 (2025-05-29)

Replace legacy supported-datasets path with commercial

## v0.1.19 (2025-05-27)

Align General Use bundle naming with frontend

## v0.1.18 (2025-05-23)

- EODHP-1294 update ordered item title field
- EODHP-1325 forward errors from airbus quotes to frontend

## v0.1.17 (2025-04-30)

Move Airbus SAR bucket to Prod

## v0.1.16 (2025-0043-25)

SAR credential validation added before quote/order

## v0.1.15 (2025-04-17)

Update tag to remove spaces in STAC IDs

## v0.1.14 (2025-04-17)

Validation check for quote/order per provider to check that the calling workspace has an API key + contract ID (airbus only)

## v0.1.13 (2025-04-15)

Update quotes for specific Planet order combinations

## v0.1.12 (2025-03-31)

Fixed order extension formatting

## v0.1.11 (2025-03-28)

Collection thumbnail proxy and health check
- EODHP-1246 Add health check endpoint
- EODHP-1076 update descriptions for top level supported datasets catalogues

## v0.1.10 (2025-03-25)

Fixed radarOptions typing

## v0.1.9 (2025-03-24)

Fixed location header

## v0.1.8 (2025-03-24)

Update ordering user journey
- EODHP-1223 Airbus PNEO orders need to know end users
- EODHP-1212 commercial data ordering user journey should be easier to follow and script
- Remove ogc-api from ades calls

## v0.1.7 (2025-02-20)

Added AOI ordering

## v0.1.6 (2025-02-12)

- Improved documentation for data ordering
- Authentication moved from OPA check to workspaces API check. Requests must include a workspace-scoped token

## v0.1.5 (2025-02-10)

Updates to allow APIs to call all three commercial data adaptors

## v0.1.4 (2025-01-22)

Adds proxy endpoint for airbus optical thumbnails, requiring users to be logged in to view external thumbnails with an api key

## v0.1.3 (2024-11-29)

Updated ordering endpoint to latest version of SAR adaptor workflow:
- Ordered data moved to commercial-data subfolder
- Updated inputs for adaptor
- Avoid starting an order if the requested item already exists in the workspace

## v0.1.2 (2024-11-12)

Added endpoint to order data via an ADES workflow

## v0.1.1 (2024-11-01)

Updated requirements to latest version

## v0.1.0 (2024-10-11)

Initial Commit for resource-catalogue-fastapi user service
- Add FastApi application to allow users to add items to catalogues through pulsar messages
- Add middleware to integrate with OPA authentication to restrict user access
