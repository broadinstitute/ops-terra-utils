# Contributing to Ops terra utils

Thank you for your interest in contributing to this repository! This document outlines the process for making contributions, testing changes, and submitting pull requests.

## Directory Structure

- **`python/`**: Contains all Python scripts. New scripts should be placed here.
- **`wdl/`**: Contains all WDLs. New WDLs should be placed in this directory.

All shared Python functionality for interacting with TDR, Terra, GCP, etc. is located in the [pyops-service-toolkit](https://github.com/broadinstitute/pyops-service-toolkit)
repository, which is a requirement in the [requirements.txt](requirements.txt) file.

## Template Python Script
If you are creating a new python script from scratch you can use [template_script.py](https://github.com/broadinstitute/ops-terra-utils/blob/main/python/template_script.py) as a starting point. This template includes the necessary imports and a basic structure for the script.

## Interacting with Terra and TDR
If you are interacting with Terra or TDR at all in your script, you will want to follow the pattern of importing
Token and RunRequest from [pyops-service-toolkit](https://github.com/broadinstitute/pyops-service-toolkit):

```python
from ops_utils..request_util import RunRequest
from ops_utils..token_util import Token
from ops_utils..terra_util import Terra
from ops_utils..tdr_util import TDR

# Initialize the Terra and TDR classes
token = Token(cloud=TOKEN_TYPE)  # Either gcp or azure
request_util = RunRequest(token=token)
tdr = TDR(request_util=request_util)
terra = Terra(request_util=request_util)
```
You should not be interacting with Terra or TDR directly in your script. Instead, you should use the `Terra` and `TDR` classes to interact with Terra and TDR respectively. If an API call is not available in the `Terra` or `TDR` classes, you can add it to the respective class.

## Integration Tests
Integration tests are located in the `python/tests/integration_tests/`. These tests are run for all utils on all initial merges OR when commenting `/run_tests` after initial PR. To run locally you can do `pytest python/tests/` from the root of the repo.

If you are adding to utils you should add integration tests for your new functionality.

## Workflow for Adding WDLs

1. **Create a WDL**: Add your new WDL to the `wdl/` directory.

2. **Add Dockstore Information**: Update the `.dockstore.yml` file with information about the WDL.

3. **Add a README**: Each WDL should have a `README.txt` file describing the WDL. Ensure that the `.dockstore.yml` file links to this `README.txt`. For examples of READMEs, see the existing WDLs.
4. **Testing WDLs**
    * **Testing WDL locally**
      * See directions [below](#testing-wdls-locally)
    * **Publishing and testing WDLs with Dockstore**:
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

### Testing WDLs locally
There are two different ways to test your changes locally. The fist is the more robust way, and ensures that your WDL _and_ Python code are working as expected. The second option mainly tests your Python code can run through Cromwell and is not actually testing the WDL you've written. This is helpful for quick validations and for extremely straightforward WDLs that are essentially wrappers for Python code.

#### Testing WDL and Python

* Install and configure Cromshell:
   ```sh
   # Install - within your venv of choice
   pip -m install cromshell
   # Set env path for config file:
   export CROMSHELL_CONFIG=$(readlink -f ./dev)
   ```

* Create a Docker container running a local cromwell server
  ```sh
  #Start docker container running cromwell
  docker compose up -d dev-cromwell
  ```

* You should now be able to submit workflows to the running docker container using Cromshell

  ```sh
   # Submitting test workflow
      cromshell submit ./dev/tests/HelloWorld.wdl ./dev/tests/hello_world_inputs.json
      {"id": "498cde1f-5495-4b6e-a7ec-89d6d5f4903b", "status": "Submitted"}
   # Checking workflow status
      cromshell status 498cde1f-5495-4b6e-a7ec-89d6d5f4903b
      {"status":"Succeeded", "id":"498cde1f-5495-4b6e-a7ec-89d6d5f4903b"}
   # Get workflow metadata
      cromshell metadata 498cde1f-5495-4b6e-a7ec-89d6d5f4903b
   # Get workflow outputs
      cromshell list-outputs 498cde1f-5495-4b6e-a7ec-89d6d5f4903b
         HelloWorld.output_file: /cromwell-executions/HelloWorld/498cde1f-5495-4b6e-a7ec-89d6d5f4903b/call-HelloWorldTask/execution/stdout
      ## This file is on the docker container so we need to copy it over in order to access it:
      docker compose cp dev-cromwell:/cromwell-executions/HelloWorld/498cde1f-5495-4b6e-a7ec-89d6d5f4903b/call-HelloWorldTask/execution/stdout ./test_wdl_stdout
      cat ./test_wdl_stdout
      Hello World!
  ```

#### Testing Execution of Python scripts through Cromwell
This method of testing creates a WDL on the fly, and _does not_ actually test the WDL associated with your code. It instead is testing the functionality of your Python code running in Cromwell.
This method of testing makes a few assumptions:
* All arguments to your Python code are named exactly the same in your WDL
* All arguments that are booleans in your WDL are passed as "store_true" arguments to your Python code

1. Install jq if it's not already installed: `brew install jq`
2. First fill out the `test_inputs.json` file with your inputs defined (these are located within the wdl/{wdl_name} directory)
3. You can now run your Python script in the manner described below (note that the first argument is NOT the path to the Python script but rather the NAME of the Python script).

      ```sh
         dev/submit_wdl_to_cromwell.sh submit {python_script_to_run} {path_to_input_json}

         e.g.
         dev/submit_wdl_to_cromwell.sh submit azure_tdr_to_gcp_file_transfer.py wdl/FileExportAzureTdrToGcp/test_inputs.json

         Job submitted successfully. Job ID: {job_guid}

         You can check the status of the job by running:
         dev/submit_wdl_to_cromwell.sh monitor
         Checking status of job 8b137972-f81a-4581-a01b-22a5ae5a68fc

         {"status":"Running",
         "id ":"8b137972-f81a-4581-a01b-22a5ae5a68fc"}
      ```

### Adding libraries to the Docker image:
- If you need to add a new library to the Docker image, update the [`requirements.txt`](requirements.txt) with the new library.

## Automated Tests
We have two automated tests run via GitHub actions: linting and `womtools` for WDL validation.

## Submitting Changes
Before merging any branches to main:
1. Ensure that all tests pass.
2. Update the [README.md](README.md) with any new information about the repository or its contents.
3. Update WDL-specific README with any new information about the WDLs.
4. Test Python code locally and if WDL changes are made, test the WDLs in a Terra Workspace.
5. Get approval on the PR from a team member

## New Workflow Checklist
If you're adding a new workflow, use the following checklist as a guide for changes that are required:
- [ ] A new WDL and associated Python script that's been tested (see [here](#testing-wdls-locally) for guidance on testing)
- [ ] A README has been added with information regarding the new workflow and it's inputs/outputs. This should live next to the WDL file. (See [here](wdl/CopyDataset/README.md) as an example README)
- [ ] The [.dockstore.yml](.dockstore.yml) has been updated to add your new workflow name and point it to the new WDL file and README
- [ ] A `template_input.json` file (named exactly like this) has been created to be used for testing. This should also live next to the WDL file. (See [here](wdl/CopyDataset/template_input.json) as an example JSON)
