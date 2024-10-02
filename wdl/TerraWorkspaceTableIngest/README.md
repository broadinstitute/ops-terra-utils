# WDL Input Overview
This workflow automates the process of ingesting all metadata and related files from a Terra metadata table to a TDR dataset. The dataset must already exist in order for this workflow to be utilized. However, the target table schema does not need to exist in the dataset, this script will handle either creating a new table, or updating an existing one depending on the parameters provided.

## Inputs Table:
| Input Name                | Description                                                                                                                                                                                    | Type     | Required | Default                              |
|---------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|----------|--------------------------------------|
| **billing_project**       | The GCP billing project associated with the Terra workspace.                                                                                                                                   | String   | Yes      | N/A                                  |
| **workspace_name**        | The name of the GCP workspace to pull metadata and files from.                                                                                                                                 | String   | Yes      | N/A                                  |
| **dataset_id**            | The ID of the TDR dataset to ingest to.                                                                                                                                                        | String   | Yes      | N/A                                  |
| **terra_tables**          | The name(s) of the table(s) in the Terra metadata to ingest - comma separated with no spaces in between table names. All metadata and related files will be ingested in to the target dataset. | String   | Yes      | N/A                                  |
| **update_strategy**       | Specifies how to handle updates during ingestion. Options are `REPLACE`, `APPEND`, or `UPDATE`.                                                                                                | String   | No       | `REPLACE`                            |
| **records_to_ingest**     | Optional IDs to ingest from the metadata if not all rows are desired. Provide all IDs separated by commas with no spaces.                                                                      | String   | No       | N/A                                  |
| **bulk_mode**             | Enables bulk mode for faster ingestion of a large number of files.                                                                                                                             | Boolean  | Yes      | N/A                                  |
| **max_retries**           | The maximum number of retries for a failed request. Optional.                                                                                                                                  | Int      | No       | 5                                    |
| **max_backoff_time**      | The maximum backoff time, in seconds, for a failed request. Optional.                                                                                                                          | Int      | No       | 300                                  |

## Outputs Table:
This script does not generate any outputs directly. However, logs will be provided to track the progress of the file ingestion and metadata transfer. These logs will include details on ingestion status, any errors encountered, and retries if necessary. You can review the logs in the stderr file for detailed information.

## Key Points:
* Ingests metadata and files associated with a GCP Terra metadata table to an existing TDR dataset
* Supports bulk_mode for faster ingestion of large file sets, though it disables some safeguards.
* `update_strategy` defines how the ingestion handles conflicts with existing data (e.g., REPLACE, APPEND, or UPDATE)
* Provides flexibility for configuring batch sizes, retries, and backoff times for fault-tolerant ingestion
* Uses default values for retry and backoff timing but allows customization if needed
