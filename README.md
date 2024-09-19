[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)

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
   export CROMSHELL_CONFIG=$(readlink -f ./dev/)
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

## Pre-Commit
*[pre-commit](https://pre-commit.com/#intro) is enabled on this repository*

### Usage
* Install the `pre-commit` package using one of the following two options:
  * via pip: `pip install pre-commit`
  * via homebrew: `brew install pre-commit`
  * Set up `pre-commit` locally so that it runs on each commit automatically by running `pre-commit install`
      * Note, if you run into the following error when running the installation command: `Cowardly refusing to install hooks with core.hooksPath set.`, then try running the following two commands and then re-attempt to run `pre-commit install`
    * `git config --unset-all core.hooksPath`
    * `git config --global --unset-all core.hooksPath`

### Configuration
* The hooks that are automatically set to run on each commit are configured in the [.pre-commit.yml](https://github.com/broadinstitute/spitfire/blob/master/.pre-commit-config.yaml)
* To add more hooks, browse available hooks [here](https://pre-commit.com/hooks.html)

### Overriding a Hook
* After you've installed `pre-commit`, all hooks will run automatically on _each file you've changed in your feature branch_
* To override the check and commit directly, run `git commit -m "YOU COMMIT MESSAGE" --no-verify`
  * The only time you may want to use the `--no-verify` flag is if your commit is WIP and you just need to commit something for testing.

### GitHub Actions
* There are linting checks currently configured for this repository, so committing without fully formatting or type annotating a file may result in failed GitHub actions.

### Specific checks and troubleshooting
#### mypy
`mypy` is one of the checks that are run as part of the current `pre-commit` checks. Sometimes it's super helpful, but occasionally it's just wrong. If you have line(s) failing on `mypy` errors, try the following:
1. If `mypy` isn't already installed, install it using pip: `pip install mypy`
2. If you're not able to troubleshoot with the given information automatically output my `pre-commit`, you can run
```commandline
mypy --ignore-missing-imports --ignore-missing-imports {PATH TO YOUR FILE}
```
There are lots of additional options for running `mypy` if this still isn't helpful. You can see them all by running `mypy -h`, or looking through their [documentation](https://mypy.readthedocs.io/en/latest/).
3. The part in the brackets (such as `[assignment]`, `[misc]`, etc.) is the error code. If you think `mypy` is throwing an error incorrectly on a given line, you can ignore that error using the following syntax in your code on the line the check is failing on. In the following example, the error code is an `assignment` error. You can swap this out for whatever type of error code `mypy` is reporting for that line.
```Python
def my_function(x): # type: ignore[assignment]
    return x
```

### Some things to note
* Because `pre-commit` will automatically reformat and make other adjustments (due to some of the configurations currently set up in the configuration yml), it could be that you run `git commit` and end up with 0 files tracked for commit. You may have to re-run your exact commit command to add the tracked files. If all automatic re-formatting has been run successfully, you'll see a message that says something like `2 files changed, 52 insertions(+), 1 deletion(-)`
  * At this point, you can run `git push` to push your local files to your remote branch
* For hooks that do not automatically reformat your file (for example the hook that checks for type annotations), you will have to modify your file and address the missing annotations before you'll be able to add those files as tracked files ready for commit. If annotations have successfully been fixed, you can re-run the exact same commit command and you'll see the same message as mentioned above.
  * At this point, you can run `git push` to push your local files to your remote branch

---

For more setup details, see [CONTRIBUTING.md](CONTRIBUTING.md).
