# WDL Input Overview

This WDL script searches through all tables of a specified dataset to collect file UUIDs that are referenced and then queries the dataset for all files within it. Any file UUID that exists in the dataset but is not referenced in a table is considered an orphaned file.

## Inputs Table:
| Input Name                  | Description                                                                                                                                             | Type    | Required | Default |
|-----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|---------|----------|---------|
| **dataset_id**               | The unique identifier of the dataset.                                                                                                                   | String  | Yes      | N/A     |
| **delete_orphaned_files**    | If `true`, deletes all orphaned files; otherwise, only reports them.                                                                                    | Boolean | Yes      | N/A     |
| **max_retries**              | The maximum number of API retries before failing.                                                                                       | Int     | No       | 5       |
| **max_backoff_time**         | The total amount of time, in seconds, allowed for retrying API calls. Optional.                                                                         | Int     | No       | 300     |
| **batch_size_to_list_files** | Specifies the batch size when listing files.                                                                                             | Int     | No       | 20000   |
| **batch_size_to_delete_files** | Specifies the batch size to delete files. Will submit this many delete jobs at once and monitor until completion before moving to next batch. | Int     | No       | 100     |
| **docker**                   | Allows the use of a different Docker image.                                                                                                  | String  | No       | N/A     |

## Outputs table:
This script does not have any outputs. If you run this with `delete_orphaned_files` set to `false` then you can view the logs lines, including the orphaned uuids, in the stderr file.