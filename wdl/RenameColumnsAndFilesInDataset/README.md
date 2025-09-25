# WDL Input Overview

This WDL updates all files and one column with an updated value within a dataset. The updated value comes from a Terra workspace outside the dataset. It will only rename files that are named `{column_to_update}.{extension}` to `{column_with_new_value.extension}`. To run this the dataset needs to have a primary key and the column to update cannot be the primary key.

There is some prep needed before the WDL can be run.

## Prep before running WDL
1. Create a snapshot of the dataset where you want to rename files and update values.
2. Import the snapshot into a new workspace.
3. Add column in new workspace table for new value you want to replace the old value with.
4. Make sure you are Custodian of dataset, member of TDR billing profile, and have write, computer, and share access on the workspace.
 ---
## Inputs Table:
This workflow is designed to use `Run workflow with inputs defined by file paths` option.

| Input Name                     | Description                                                                                | Type    | Required | Default                                                                                     |
|--------------------------------|--------------------------------------------------------------------------------------------|---------|----------|---------------------------------------------------------------------------------------------|
| **dataset_id**                 | The unique identifier of the dataset.                                                      | String  | Yes      | N/A                                                                                         |
| **column_to_update**           | The column to update and also the basename to look at for files. Cannot be the primary key | String  | Yes      | N/A                                                                                         |
| **column_with_new_value**      | The column in Terra workspace which contains the new column name/file basename.            | String  | Yes      | N/A                                                                                         |
| **table_name**                 | The name of the table within the dataset and Terra workspace.                              | String  | Yes      | N/A                                                                                         |
| **billing_project**            | The billing project where table is. Will also be used for temp bucket to store files.      | String  | No       | N/A                                                                                         |
| **workspace_name**             | The Terra workspace where table is. Will also be used for temp bucket to store files.      | String  | No       | N/A                                                                                         |
| **report_updates_only**        | Set to true if just want to report how many rows and files will be updated.                | Boolean | Yes      | N/A                                                                                         |
| **update_columns_only**        | Set to true if just want to update `column_to_update` and not update files.                | Boolean | No       | false                                                                                       |
| **max_retries**                | The maximum number of retries for a failed request. Optional.                              | Int     | No       | 5                                                                                           |
| **max_backoff_time**           | The maximum backoff time, in seconds, for a failed request. Optional.                      | Int     | No       | 300                                                                                         |
| **copy_and_ingest_batch_size** | The number of rows to copy to the temp location and then ingest at a time.                 | Int     | No       | 500                                                                                         |
| **docker**                     | Specifies a custom Docker image to use. Optional.                                          | String  | No       | us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest |

## Outputs Table:
This script does not generate any outputs directly, but progress and results of the copy and ingestion operations will be logged. Any orphaned files left behind in the dataset will need to be handled separately. You can track the progress and results by reviewing the logs in the stderr file.

### Additional Notes:
- After the file renaming and ingestion process, temporary files will be cleaned up automatically.
