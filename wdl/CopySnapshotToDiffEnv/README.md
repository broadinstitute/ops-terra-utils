# WDL Input Overview

This WDL script copies a TDR snapshot from one environment to another (e.g., from prod to dev or vice versa).

It will create temp workspace in other env, copy files into the bucket, create dataset in other env, and create a snapshot from that dataset.

## Inputs Table:
This workflow is designed to use `Run workflow with inputs defined by file paths` option. You can use `Run workflow(s) with inputs defined by data table` option if you set up a data table specifically to use this WDL.

| Input Name                | Description                                                              | Type    | Required | Default |
|---------------------------|--------------------------------------------------------------------------|---------|----------|---------|
| **billing_project**       | The Terra billing project for the destination workspace.                 | String  | Yes      | N/A     |
| **workspace_name**        | The name of the destination Terra workspace.                             | String  | Yes      | N/A     |
| **dataset_id**            | The TDR dataset ID containing the snapshot to be copied.                 | String  | Yes      | N/A     |
| **orig_env**              | The environment of the original dataset ("prod" or "dev").               | String  | Yes      | N/A     |
| **continue_if_exists**    | If true, continue if the workspace, dataset, or snapshot already exists. | Boolean | Yes      | N/A     |
| **delete_temp_workspace** | If true, delete any temporary workspace created during the process.      | Boolean | Yes      | N/A     |
| **verbose**               | If true, enable verbose logging for debugging.                           | Boolean | Yes      | N/A     |
| **new_billing_profile**   | The billing profile ID for the destination (required for dev→prod).      | String  | No       | N/A     |
| **service_account_json**  | Service account JSON file for authentication (optional).                 | File    | No       | N/A     |
| **docker**                | Custom Docker image to use (optional).                                   | String  | No       | N/A     |

## Outputs Table:
This script does not generate any outputs directly. However, logs will be provided to track the progress of the snapshot transfer and workspace setup. You can review the logs in the stderr file for information about the process and any issues encountered.
