# CreateFailedSampleSetsForDragen WDL

# WDL Input Overview

This WDL workflow runs the `create_failed_sample_sets_for_dragen.py` script to generate a TSV file of failed samples, batched by research project and sample set size and uploads to the workspace.

## Inputs Table
This workflow is designed to use `Run workflow with inputs defined by file paths` option. You can use `Run workflow(s) with inputs defined by data table` option if you set up data table specifically to use this WDL.

| Input Name             | Description                                                           | Type    | Required | Default                                                                                     |
|------------------------|-----------------------------------------------------------------------|---------|----------|---------------------------------------------------------------------------------------------|
| **workspace_name**     | The Terra workspace name.                                             | String  | Yes      | N/A                                                                                         |
| **billing_project**    | The Terra billing project.                                            | String  | Yes      | N/A                                                                                         |
| **sample_set_append**  | String to append to each sample set name.                             | String  | Yes      | N/A                                                                                         |
| **max_per_sample_set** | Maximum number of samples per sample set batch.                       | Int     | No       | 2000                                                                                        |
| **docker**             | (Optional) Custom Docker image to use.                                | String  | No       | us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest |
