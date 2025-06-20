# WDL Input Overview

This WDL script collects the status of Dragen reprocessing samples from BigQuery and uploads summary statistics (such as attempts, status, etc.) to a Terra workspace. It is useful for tracking the progress and outcomes of Dragen workflows for a set of samples.

## Inputs Table:
This workflow is designed to use `Run workflow(s) with inputs defined by data table` option.

| Input Name          | Description                                                                                            | Type     | Required | Default                                                                                     |
|---------------------|--------------------------------------------------------------------------------------------------------|----------|----------|---------------------------------------------------------------------------------------------|
| **billing_project** | The Terra billing project where metrics are currently stored.                                          | String   | Yes      | N/A                                                                                         |
| **workspace_name**  | The Terra workspace name where metrics are currently stored.                                           | String   | Yes      | N/A                                                                                         |
| **gcp_project**     | The GCP project where the Dragen workflows are running.                                                | String   | Yes      | N/A                                                                                         |
| **data_type**       | The data type to query. Should be `bge` or `wgs`.                                                      | String   | Yes      | N/A                                                                                         |
| **min_start_date**  | (Optional) Minimum start date for filtering samples. Format: `YYYY-MM-DD`.                             | String   | No       | N/A                                                                                         |
| **max_start_date**  | (Optional) Maximum start date for filtering samples. Format: `YYYY-MM-DD`.                             | String   | No       | N/A                                                                                         |
| **docker**          | (Optional) Docker image to use for the task.                                                           | String   | No       | us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest |

## Outputs Table:
This script does not generate direct outputs. However, logs will track the progress of workspace creation, file transfer, and metadata updates. The logs, which include any errors or warnings, can be reviewed in the stderr file for additional details about the process.
