# WDL Input Overview

This WDL script processes a specified table within a dataset and re-ingests files by renaming them. The renaming is based on the original file's basename from a specified column and changes it to a new file basename, retaining the same extension.

## Inputs Table:
| Input Name                    | Description                                                                                                       | Type     | Required | Default                                                                                     |
|-------------------------------|-------------------------------------------------------------------------------------------------------------------|----------|----------|------------------------------------------------------------------------------------------------|
| **dataset_id**                 | The unique identifier of the dataset.                                                                             | String   | Yes      | N/A                                                                                          |
| **original_file_basename_column** | The column name that contains the original file basenames.                                                      | String   | Yes      | N/A                                                                                          |
| **new_file_basename_column**   | The column name that contains the new file basenames.                                                             | String   | Yes      | N/A                                                                                          |
| **dataset_table_name**         | The name of the table within the dataset that contains the file information.                                       | String   | Yes      | N/A                                                                                          |
| **row_identifier**             | A unique identifier for rows within the table, used to distinguish and re-ingest files.                           | String   | Yes      | N/A                                                                                          |
| **batch_size**                 | Specifies the batch size when processing file renaming. Optional.                                                 | Int      | No       | 500                                                                                          |
| **max_retries**                | The maximum number of API retries before failing. Optional.                                                       | Int      | No       | 5                                                                                            |
| **max_backoff_time**           | The total amount of time, in seconds, allowed for retrying API calls. Optional.                                   | Int      | No       | 300                                                                                          |
| **docker**                     | Specifies a custom Docker image to use. Optional.                                                                 | String   | No       | us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest   |

## Outputs Table:
This script does not generate any outputs directly, but file renaming operations will be logged. You can track the progress and results by reviewing the logs in the stderr file.
