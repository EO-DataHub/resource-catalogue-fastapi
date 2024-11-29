# Changelog

## v0.1.3 (2024-11-12)

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
