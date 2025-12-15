.PHONY: dockerbuild dockerpush test testonce ruff black lint isort pre-commit-check requirements-update requirements setup
VERSION ?= latest
IMAGENAME = resource-catalogue-fastapi
DOCKERREPO ?= public.ecr.aws/eodh
uv-run ?= uv run --no-sync

dockerbuild:
	DOCKER_BUILDKIT=1 docker build -t ${IMAGENAME}:${VERSION} .

dockerpush: dockerbuild
	docker tag ${IMAGENAME}:${VERSION} ${DOCKERREPO}/${IMAGENAME}:${VERSION}
	docker push ${DOCKERREPO}/${IMAGENAME}:${VERSION}

test:
	${uv-run} ptw resource_catalogue_fastapi

testonce:
	${uv-run} pytest

ruff:
	${uv-run} ruff check .

black:
	${uv-run} black .

isort:
	${uv-run} isort . --profile black

validate-pyproject:
	${uv-run} validate-pyproject pyproject.toml

lint: ruff black isort validate-pyproject

uv-sync:
	${uv-run} sync --frozen

.git/hooks/pre-commit:
	${uv-run} pre-commit install
	curl -o .pre-commit-config.yaml https://raw.githubusercontent.com/EO-DataHub/github-actions/main/.pre-commit-config-python.yaml

setup: uv-sync .git/hooks/pre-commit
