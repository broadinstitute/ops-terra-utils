# WDL Input Overview

This WDL script copies a TDR snapshot from one environment to another (e.g., from prod to dev or vice versa).

It will create temp workspace in other env, copy files into the bucket, create dataset in other env, and create a snapshot from that dataset.

## Prerequisites
Need to give user/SA running the script `Storage Object User` permissions to write to the temp bucket as well as the general dev (`jade-k8-sa@broad-jade-dev.iam.gserviceaccount.com`) or prod (`datarepo-jade-api@terra-datarepo-production.iam.gserviceaccount.com`) service account and user/SA proxy account (from new dev) `Storage Object Viewer` for ingestion.

## Inputs Table:
This workflow is designed to use `Run workflow with inputs defined by file paths` option. You can use `Run workflow(s) with inputs defined by data table` option if you set up a data table specifically to use this WDL.

| Input Name                      | Description                                                                                                               | Type    | Required | Default |
|---------------------------------|---------------------------------------------------------------------------------------------------------------------------|---------|----------|---------|
| **temp_bucket**                 | GCP bucket to house temporary files.                                                                                      | String  | Yes      | N/A     |
| **workspace_name**              | The name of the destination Terra workspace.                                                                              | String  | Yes      | N/A     |
| **dataset_id**                  | The TDR dataset ID containing the snapshot to be copied.                                                                  | String  | Yes      | N/A     |
| **orig_env**                    | The environment of the original dataset ("prod" or "dev").                                                                | String  | Yes      | N/A     |
| **continue_if_exists**          | If true, continue if the workspace, dataset, or snapshot already exists.                                                  | Boolean | Yes      | N/A     |
| **delete_temp_workspace**       | If true, delete any temporary workspace created during the process.                                                       | Boolean | Yes      | N/A     |
| **verbose**                     | If true, enable verbose logging for debugging.                                                                            | Boolean | Yes      | N/A     |
| **delete_intermediate_files**   | If true, delete intermediate files after the transfer is complete.                                                        | Boolean | Yes      | N/A     |
| **owner_emails**                | csv list of emails to add as stewards of newly created datasets, and snapshots. Required when using service_account_json. | String  | No       | N/A     |
| **new_billing_profile**         | The billing profile ID for the destination (required for dev→prod).                                                       | String  | No       | N/A     |
| **service_account_json**        | Service account JSON file for authentication (optional).                                                                  | File    | No       | N/A     |
| **dest_dataset_name**           | Name for the destination dataset. Defaults to the original dataset name if not provided (optional).                       | String  | No       | N/A     |
| **snapshot_name**               | Name for the new snapshot. Defaults to the original snapshot name if not provided (optional).                             | String  | No       | N/A     |
| **docker**                      | Custom Docker image to use (optional).                                                                                    | String  | No       | N/A     |

## Outputs Table:
This script does not generate any outputs directly. However, logs will be provided to track the progress of the snapshot transfer and workspace setup. You can review the logs in the stderr file for information about the process and any issues encountered.
