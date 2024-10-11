# UK EO Data Hub Platform: ADES-FastApi Component

This component is the initial implementation of the User Account Service that sits in front of the ADES service and allows more refined interaction with the workflow runner within the EO Data Hub. This repository provides a FastApi application that makes calls to the ADES component, also containing middleware which authenticates the incoming requests using an OPA client. Access to workflows and jobs is only provided if the username of the requesting user matches that found in the URL request.

Note swagger static files are available in the [swagger-ui](https://github.com/swagger-api/swagger-ui/tree/master/dist) repo.


# Development of this component

## Run the server locally
This application uses a uvicorn server to run the FastApi application, you can initiate this server locally using the below command: 
```commandline
uvicorn resource_catalogue_fastapi:app --reload
```
This application also requires access an ADES service as well as an OPA client via URLs, which can be provided using the following environment variables:
```commandline
OPA_SERVICE_ENDPOINT
ADES_SERVICE_ENDPOINT
```
## Getting started

### Install via makefile

```commandline
make setup
```

This will create a virtual environment called `venv`, build `requirements.txt` and
`requirements-dev.txt` from `pyproject.toml` if they're out of date, install the Python
and Node dependencies and install `pre-commit`.

It's safe and fast to run `make setup` repeatedly as it will only update these things if
they have changed.

After `make setup` you can run `pre-commit` to run pre-commit checks on staged changes and
`pre-commit run --all-files` to run them on all files. This replicates the linter checks that
run from GitHub actions.


### Alternative installation

You will need Python 3.11. On Debian you may need:
* `sudo add-apt-repository -y 'deb http://ppa.launchpad.net/deadsnakes/ppa/ubuntu focal main'` (or `jammy` in place of `focal` for later Debian)
* `sudo apt update`
* `sudo apt install python3.11 python3.11-venv`

and on Ubuntu you may need
* `sudo add-apt-repository -y 'ppa:deadsnakes/ppa'`
* `sudo apt update`
* `sudo apt install python3.11 python3.11-venv`

To prepare running it:

* `virtualenv venv -p python3.11`
* `. venv/bin/activate`
* `rehash`
* `python -m ensurepip -U`
* `pip3 install -r requirements.txt`
* `pip3 install -r requirements-dev.txt`

You should also configure your IDE to use black so that code is automatically reformatted on save.

## Building and testing

This component uses `pytest` tests and the `ruff` and `black` linters. `black` will reformat your code in an opinionated way.  
When testing this module, you need to ensure you have unset the environment variable `ENABLE_OPA_POLICY_CHECK`, or set it to `true`: `export ENABLE_OPA_POLICY_CHECK=true`,
so ensure the policy is checked when testing.

A number of `make` targets are defined:
* `make test`: run tests continuously
* `make testonce`: run tests once
* `make lint`: lint and reformat
* `make dockerbuild`: build a `latest` Docker image (use `make dockerbuild `VERSION=1.2.3` for a release image)
* `make dockerpush`: push a `latest` Docker image (again, you can add `VERSION=1.2.3`) - normally this should be done
  only via the build system and its GitHub actions.

## Managing requirements

Requirements are specified in `pyproject.toml`, with development requirements listed separately. Specify version
constraints as necessary but not specific versions. After changing them:

* Run `pip-compile` (or `pip-compile -U` to upgrade requirements within constraints) to regenerate `requirements.txt`
* Run `pip-compile --extra dev -o requirements-dev.txt` (again, add `-U` to upgrade) to regenerate
  `requirements-dev.txt`.
* Run the `pip3 install -r requirements.txt` and `pip3 install -r requirements-dev.txt` commands again and test.
* Commit these files.

If you see the error

```commandline
Backend subprocess exited when trying to invoke get_requires_for_build_wheel
Failed to parse /.../template-python/pyproject.toml
```

then install and run `validate-pyproject pyproject.toml` and/or `pip3 install .` to check its syntax.

To check for vulnerable dependencies, run `pip-audit`.

## Releasing

Ensure that `make lint` and `make test` work correctly and produce no further changes to code formatting before
continuing.

Releases tagged `latest` and targeted at development environments can be created from the `main` branch. Releases for
installation in non-development environments should be created from a Git tag named using semantic versioning. For
example, using

* `git tag 1.2.3`
* `git push --tags`
