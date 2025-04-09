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

### Who to Contact
- For questions, assistance, suggestions, or special requests, feel free to connect with the team on the Slack channel #ops-terra-utils.

## Quick Start Guide
If you're new to repository or any of these features, follow the guide here to get started writing and testing your first workflow.

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
2. Next, write your Python code. To get tokens and interact with Terra and/or TDR, see our [template Python script](python/template_script.py). It has a lot of the set-up required for writing a script to interact with these resources. Additionally, a good deal of functionality to interact with Terra and TDR can be found in the [utilities](python/utils_scream_test) directory. See Terra utilities [here](python/utils_scream_test/terra_utils/terra_util.py) and different types of TDR utilities located in the [TDR utilities directory](python/utils_scream_test/tdr_utils).
3. Now it's time for your WDL file. In this repository, most WDL files are simply "wrappers" for the Python code. All this means is that the WDLs don't contain much logic on their own - they're mostly one-task workflows that call out to Python scripts. You can see multiple examples [here](/wdl). As a general rule, the most of these WDLs can be copied over to create a new workflow, changing just the workflow name, inputs, and the Python script that's called. As a general rule, the inputs to your workflow are the same as the inputs that are required for your Python script. Additionally, the same Docker image can generally be used for all workflows (`us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest`). If your Python script contains optional inputs or flags, there are examples [here](https://github.com/broadinstitute/ops-terra-utils/blob/ad31bc643ddb3adbd6af9500f7e3e732d9cc5fa1/wdl/CopyDataset/CopyDataset.wdl#L51-L53) for optional inputs and examples [here](https://github.com/broadinstitute/ops-terra-utils/blob/ad31bc643ddb3adbd6af9500f7e3e732d9cc5fa1/wdl/CopyDataset/CopyDataset.wdl#L54-L55) for flags (the flags are passed in as Booleans to the WDL).
4. Once you have your Python script and WDL code ready, you can either test locally, or publish your workflow to Dockstore and run it via Terra. To run just your Python script locally, see [running locally directions](#running-code-locally). To run your entire workflow locally (including the WDL) there are some options outlined [here](https://github.com/broadinstitute/ops-terra-utils/blob/ad31bc643ddb3adbd6af9500f7e3e732d9cc5fa1/CONTRIBUTING.md#testing-wdls-locally). To publish in Dockstore and run test via Terra, go through the following steps:
    * Update the [.dockstore.yml](.dockstore.yml) to include your new workflow. The syntax should remain the same, just replace with the path to your new WDL file and the name of your new workflow.
    * Push all your local changes to your remote feature branch in GitHub. Once all your changes are pushed, navigate to the [GitHub action to build the docker image](https://github.com/broadinstitute/ops-terra-utils/actions/workflows/docker-BuildAndPush.yaml) and in the "Run workflow" drop down, select your feature branch and click the green "Run workflow" button. This will create the Docker image with your new Python script so it's available to use.
    * Next, navigate to [Dockstore](https://dockstore.org/my-workflows/github.com/broadinstitute/accessibility_peak_gene_predictor/peak_gene_predictor) (make sure you're logged in) and under the "More" dropdown with the gear icon, select "Discover Existing Dockstore Workflows". This may take a minute, but it will re-sync Dockstore to make your new WDL available. Once this finishes, click on the "Unpublished" tab, and select your workflow. Once you find your new workflow, select the 'Versions' tab and make sure your feature branch is selected. Then select "Publish". Note that Dockstore won't allow you to publish your workflow if your WDL has syntax errors. In this case, you'll see them reported in the "Files" tab.
    * Once your workflow is registered, navigate to Terra and either open an existing workspace or create a new one. Import your workflow and launch it! It will run via Terra and you'll see in status reported. You can debug using the links the execution buckets (all logs, stdout, and stderr files will be located in the execution directories).

## Running Code Locally
- To run a Python script locally you will need all required dependencies installed. It will be some subset, if not all, of [requirements.txt](requirements.txt).
```bash
python python/script_name.py --arg1 value --arg2 value
```
- Alternatively, if you're using Docker:
```bash
docker run -v $(pwd):/app us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest python /app/script_name.py --arg1 value --arg2 value
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
