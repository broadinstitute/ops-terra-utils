# WDL Input Overview

This WDL script automates the process of creating or obtaining a GCP dataset based on an AnVIL GCP workspace, followed by the ingestion of all relevant metadata and files into the dataset. The script offers several configuration options, including batch ingestion size, retries, backoff times, and the option to use bulk mode for larger file sets. Additionally, users can choose how updates are handled (e.g., REPLACE, APPEND, or UPDATE), and the dataset can be created within a specified TDR billing profile.

## Inputs Table:
| Input Name                 | Description                                                                                          | Type     | Required | Default                                                                                     |
|----------------------------|------------------------------------------------------------------------------------------------------|----------|----------|---------------------------------------------------------------------------------------------|
| **billing_project**         | The GCP billing project associated with the Terra workspace.                                          | String   | Yes      | N/A                                                                                         |
| **workspace_name**          | The name of the GCP workspace to pull metadata and files from.                                       | String   | Yes      | N/A                                                                                         |
| **phs_id**                  | The PHS ID associated with the data to be ingested.                                                  | String   | Yes      | N/A                                                                                         |
| **bulk_mode**               | Enables bulk mode for faster ingestion of a large number of files.                                   | Boolean  | Yes      | N/A                                                                                         |
| **docker_name**             | Specifies the Docker image to use for running the script.                                            | String   | Yes      | N/A                                                                                         |
| **dataset_name**            | The name for the new dataset. If not provided, a dataset name will be generated based on the workspace name. | String   | No       | Generated from workspace name                                                               |
| **update_strategy**         | Specifies how to handle updates during ingestion. Options are `REPLACE`, `APPEND`, or `UPDATE`.      | String   | No       | REPLACE                                                                                     |
| **tdr_billing_profile**     | The TDR billing profile ID for the dataset. Defaults to the AnVIL TDR billing profile if not provided. | String   | No       | `e0e03e48-5b96-45ec-baa4-8cc1ebf74c61` (AnVIL prod billing profile)                         |
| **file_ingest_batch_size**  | The number of files to ingest at a time. Optional.                                                   | Int      | No       | 500                                                                                         |
| **max_backoff_time**        | The maximum backoff time, in seconds, for a failed request. Optional.                                | Int      | No       | 300                                                                                         |
| **max_retries**             | The maximum number of retries for a failed request. Optional.                                        | Int      | No       | 5                                                                                           |

## Outputs Table:
This script does not generate any outputs directly. However, logs will be provided to track the progress of the dataset creation, file ingestion, and metadata transfer. These logs will include details on ingestion status, any errors encountered, and retries if necessary. You can review the logs in the stderr file for detailed information.

### Key Points:
- Creates or uses a dataset in a GCP workspace and ingests all metadata and files.
- Supports `bulk_mode` for faster ingestion of large file sets, though it disables some safeguards.
- `update_strategy` defines how the ingestion handles conflicts with existing data (e.g., REPLACE, APPEND, or UPDATE).
- Provides flexibility for configuring batch sizes, retries, and backoff times for fault-tolerant ingestion.
- Uses default values for retry and backoff timing but allows customization if needed.
