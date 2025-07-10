# WDL Input Overview

This WDL script ingests Dragen workflow data for a set of samples into a Terra Data Repository (TDR) dataset from a Terra Workspace.

## Inputs Table:
This workflow is designed to use `Run workflow(s) with inputs defined by data table` option.

| Input Name                           | Description                                                                                             | Type    | Required | Default                                                                                     |
|--------------------------------------|---------------------------------------------------------------------------------------------------------|---------|----------|---------------------------------------------------------------------------------------------|
| **billing_project**                  | The Terra billing project where the TDR dataset is located.                                             | String  | Yes      | N/A                                                                                         |
| **workspace_name**                   | The Terra workspace name associated with the TDR dataset.                                               | String  | Yes      | N/A                                                                                         |
| **sample_set**                       | The name of the sample set to ingest from Dragen output.                                                | String  | Yes      | N/A                                                                                         |
| **target_table_name**                | The name of the TDR table to ingest data into.                                                          | String  | Yes      | N/A                                                                                         |
| **dataset_id**                       | The BigQuery dataset ID containing Dragen output.                                                       | String  | Yes      | N/A                                                                                         |
| **bulk_mode**                        | (Optional) Use bulk mode for ingest.                                                                    | Boolean | No       | false                                                                                       |
| **dry_run**                          | (Optional) If true, performs a dry run without ingesting data.                                          | Boolean | No       | false                                                                                       |
| **filter_entity_already_in_dataset** | (Optional) Filter out entities already present in the TDR dataset.                                      | Boolean | No       | false                                                                                       |
| **batch_size**                       | (Optional) Number of records to ingest per batch.                                                       | Int     | No       | 500                                                                                         |
| **waiting_time_to_poll**             | (Optional) Seconds to wait between polling for ingest completion.                                       | Int     | No       | 180                                                                                         |
| **update_strategy**                  | (Optional) Update strategy: `append`, `replace`, or `merge`.                                            | String  | No       | replace                                                                                     |
| **unique_id_field**                  | (Optional) Unique ID field for the TDR table.                                                           | String  | No       | sample_id                                                                                   |
| **table_name**                       | (Optional) Table name in the TDR dataset.                                                               | String  | No       | sample                                                                                      |
| **dragen_version**                   | (Optional) Dragen version string.                                                                       | String  | No       | 07.021.604.3.7.8                                                                            |
| **docker**                           | (Optional) Docker image to use for the task.                                                            | String  | No       | us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest |

## Outputs Table:
This script does not generate direct outputs. However, logs will track the progress of data ingestion, file transfer, and metadata updates. The logs, which include any errors or warnings, can be reviewed in the stderr file for additional details about the process.
