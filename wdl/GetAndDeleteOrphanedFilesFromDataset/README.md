# WDL Input Overview

This WDL script searches through all tables of a specified dataset to collect file UUIDs that are referenced and then queries the dataset for all files within it. Any file UUID that exists in the dataset but is not referenced in a table is considered an orphaned file.

## Inputs Table:

| Input Name                  | Description                                                                 | Type     |
|-----------------------------|-----------------------------------------------------------------------------|----------|
| **dataset_id**               | The unique identifier of the dataset.                                       | String   |
| **delete_orphaned_files**    | If `true`, deletes all orphaned files; otherwise, only reports them.         | Boolean  |
| **max_retries**              | Optional. The maximum number of API retries before failing.                  | Int      |
| **max_backoff_time**         | Optional. The total amount of time allowed for retrying API calls.           | Int      |
| **batch_size_to_list_files** | Optional. Specifies the batch size when listing files.                       | Int      |
| **docker**                   | Optional. Allows the use of a different Docker image.                        | String   |

## Outputs table:
This script does not have any outputs. If you run this with `delete_orphaned_files` set to `false` then you can view the logs lines, including the orphaned uuids, in the stderr file.