# Contributing to Ops terra utils

Thank you for your interest in contributing to this repository! This document outlines the process for making contributions, testing changes, and submitting pull requests.

## Directory Structure

- **`python/`**: Contains all Python scripts. New scripts should be placed here.
- **`wdl/`**: Contains all WDLs. New WDLs should be placed in this directory.
- **`python/utils/`**: Houses reusable Python utility functions. If possible, reuse or contribute new functions here.

## Interacting with Terra and TDR
If you are interacting with Terra or TDR at all in your script you will want to follow the pattern of importing Token and RunRequest from the utils like below.

```python
from utils.request_util import RunRequest
from utils.token_util import Token
from utils.terra_util import Terra
from utils.tdr_util import TDR

# Initialize the Terra and TDR classes
token = Token(cloud=TOKEN_TYPE)  # Either gcp or azure
request_util = RunRequest(token=token)
tdr = TDR(request_util=request_util)
terra = Terra(request_util=request_util)
```
You should not be interacting with Terra or TDR directly in your script. Instead, you should use the `Terra` and `TDR` classes to interact with Terra and TDR respectively. If an API call is not available in the `Terra` or `TDR` classes, you can add it to the respective class.


## Workflow for Adding WDLs

1. **Create a WDL**: Add your new WDL to the `wdl/` directory.

2. **Add Dockstore Information**: Update the `.dockstore.yml` file with information about the WDL.

3. **Add a README**: Each WDL should have a `README.txt` file describing the WDL. Ensure that the `.dockstore.yml` file links to this `README.txt`. For examples of READMEs, see the existing WDLs.

4. **Publishing and testing WDLs with Dockstore**:
   - Navigate to [Dockstore](https://dockstore.org/my-workflows/).
   - Search for the WDL in the unpublished section.
   - After selecting the WDL, go to the "Versions" tab.
   - Set the appropriate branch as the default.
   - Publish the WDL
   - Go to public page for WDL and it to Terra Workspace.
   - Test WDL from within workspace.

5. **Updating and testing already published WDLs**:
   - Make changes to the WDL.
   - Import workflow to a Terra workspace. Switch to test branch on workflows page after importing if not merged to main yet.
   - Run workflow within Terra.

## Working with Docker

The repository uses the following Docker image: `us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest`.

### Automatic Docker Build:
- The Docker image is automatically built and pushed whenever changes are merged into the `main` branch.

### Testing with a Non-Latest Docker Image:
- If you're testing changes in the python scripts before merging to main, you will need to build the Docker image locally and push it to a different tag.

Build Image
```bash
docker build -t us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:<tag> .
```
Push Image
```bash
docker push us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:<tag>
```
- Update your WDL to point to this new Docker image tag for testing purposes.

### Running Python script in Docker locally:
- To run a Python script in Docker locally, you can use the following command:
```bash
docker run -v $(pwd):/app us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest python /app/python/script_name.py --arg1 value --arg2 value
```

### Adding libraries to the Docker image:
- If you need to add a new library to the Docker image, update the `requirements.txt` with the new library.

## Automated Tests

We have two automated tests run via GitHub actions: linting and `womtools` for WDL validation.

## Submitting Changes

Before merging any branches to main:
1. Ensure that all tests pass.
2. Update the README with any new information about the repository or its contents.
3. Update WDL-specific README with any new information about the WDLs.
4. Test python code locally and if WDL changes are made, test the WDLs in a Terra Workspace.
5. Get approval from a team member
