# Ops terra utils Overview

Welcome to this repository! This repo contains Python scripts and WDLs designed to be used in conjunction with Terra and the Terra Data Repository (TDR).

### What does this repo do?

- **Python scripts**: Automate tasks such as ingesting metadata, managing files in TDR, and interacting with Terra workspaces.
- **WDLs**: Wrap these Python scripts for execution in Terra workflows.

### What does this repo not do?

- This repo does not include the direct setup of Terra or TDR environments.
- It does not manage external dependencies for running workflows unless done through the provided Docker image or WDL.

For more detailed information, please refer to [CONTRIBUTING.md](CONTRIBUTING.md).

## Getting Started

### How to set up the repo:

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
2. Running Python Scripts: You can run the Python scripts directly after cloning. Some of the key scripts are designed to interact with Terra and TDR for tasks such as metadata ingestion, dataset management, etc.

3. Using Docker: There is a Docker image available that comes with all the required dependencies for running the scripts:
`us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest`

4. Running WDLs: The WDLs in this repository are designed to be run in Terra. You can import the WDLs into your Terra workspace and run them there.

## Running Code Locally
- To run a Python script locally you will need all required dependencies installed. It will be some subset, if not all, of [requirements.txt](requirements.txt).
```bash
python python/script_name.py --arg1 value --arg2 value
```
- Alternatively, if you're using Docker:
```bash
docker run -v $(pwd):/app us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest python /app/script_name.py --arg1 value --arg2 value
```


## Testing WDL's locally

- Prereqs
  - Cromshell

   ```sh
   # Install - within your venv of choice
   pip -m install cromshell
   # Set env path for config file:
   export CROMSHELL_CONFIG=$(readlink -f ./dev/.cromshell)
   ```

  - Docker container running local cromwell server

  ```sh
  #Start docker container running cromwell
  docker compose up -d dev-cromwell
  ```

- Testing Locally
  - you should now be able to submit workflows to the running docker container using cromshell

  ```sh
   # Submitting test workflow
      cromshell submit ./dev/tests/Hello_World.wdl ./dev/tests/hello_world_inputs.json
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

=======

For more setup details, see [CONTRIBUTING.md](CONTRIBUTING.md).
