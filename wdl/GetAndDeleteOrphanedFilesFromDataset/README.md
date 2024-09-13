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
