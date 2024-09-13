# WDL Input Overview

This WDL script searches through all tables of a specified dataset to collect file UUIDs that are referenced and then queries the dataset for all files within it. Any file UUID that exists in the dataset but is not referenced in a table is considered an orphaned file.

## Required Inputs:

- **String dataset_id**: The unique identifier of the dataset. This field is required to specify which dataset to process.

- **Boolean delete_orphaned_files**: A flag that, if set to `true`, will delete all orphaned files in the dataset. If set to `false`, the task will only report the orphaned file UUIDs without deleting them.

## Optional Inputs:

- **Int? max_retries**: The maximum number of times an API call should be retried before failing.

- **Int? max_backoff_time**: The total amount of time allowed for retrying API calls before failing.

- **Int? batch_size_to_list_files**: Specifies the batch size when listing files.

- **String? docker**: Allows the use of a different Docker image, if specified.