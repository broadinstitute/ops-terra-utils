# WDL Input Overview

This WDL script processes a specified table within a dataset, copying files to a temporary bucket (either passed in or derived from a Terra workspace) and then ingesting the renamed files back into the dataset. The renaming is based on the original file's basename from a specified column, changing it to a new file basename, while retaining the same file extension. The temporary files are cleaned up after ingestion, and the process is batched based on the user-defined batch size. Note that orphaned files will be left behind in the dataset.

## Inputs Table:
| Input Name                        | Description                                                                                          | Type    | Required | Default                                                                                     |
|-----------------------------------|------------------------------------------------------------------------------------------------------|---------|----------|---------------------------------------------------------------------------------------------|
| **dataset_id**                    | The unique identifier of the dataset.                                                                | String  | Yes      | N/A                                                                                         |
| **copy_and_ingest_batch_size**    | The number of rows to copy to the temp location and then ingest at a time.                           | Int     | Yes      | N/A                                                                                         |
| **workers**                       | The number of workers to parallelize the file copy and delete process.                               | Int     | Yes      | N/A                                                                                         |
| **original_file_basename_column** | The basename column which contains the original file basenames (e.g., `sample_id`).                  | String  | Yes      | N/A                                                                                         |
| **new_file_basename_column**      | The column which contains the new file basenames, replacing the original (e.g., `collab_sample_id`). | String  | Yes      | N/A                                                                                         |
| **dataset_table_name**            | The name of the table within the dataset that contains the file information.                         | String  | Yes      | N/A                                                                                         |
| **row_identifier**                | The unique identifier for rows within the table (e.g., `sample_id`).                                 | String  | Yes      | N/A                                                                                         |
| **billing_project**               | The billing project to be used if a temp bucket is not provided. Optional.                           | String  | No       | N/A                                                                                         |
| **workspace_name**                | The Terra workspace to be used if a temp bucket is not provided. Optional.                           | String  | No       | N/A                                                                                         |
| **report_updates_only**           | Set to true if just want to report how many rows and files will be updated. Optional                 | Boolean | No       | N/A                                                                                         |
| **temp_bucket**                   | The temporary bucket to copy files to for renaming. Used if workspace_name is not provided.          | String  | No       | N/A                                                                                         |
| **max_retries**                   | The maximum number of retries for a failed request. Optional.                                        | Int     | No       | 5                                                                                           |
| **max_backoff_time**              | The maximum backoff time, in seconds, for a failed request. Optional.                                | Int     | No       | 300                                                                                         |
| **docker**                        | Specifies a custom Docker image to use. Optional.                                                    | String  | No       | us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest |

## Outputs Table:
This script does not generate any outputs directly, but progress and results of the copy and ingestion operations will be logged. Any orphaned files left behind in the dataset will need to be handled separately. You can track the progress and results by reviewing the logs in the stderr file.

### Additional Notes:
- If both `billing_project` and `temp_bucket` are not provided, the script will use the `workspace_name` to copy the files temporarily.
- After the file renaming and ingestion process, temporary files will be cleaned up automatically.
