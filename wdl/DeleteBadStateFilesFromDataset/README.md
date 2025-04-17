# WDL Input Overview

This WDL script takes attempts to list all the files in a dataset. If any files in a bad state are encountered, they are deleted.

## Inputs Table:
 This workflow is designed to use `Run workflow with inputs defined by file paths` option. You can use `Run workflow(s) with inputs defined by data table` option if you set up data table specifically to use this wdl.

| Input Name               | Description                                                                                                                                                                                        | Type   | Required | Default                                                                                       |
|--------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|----------|-----------------------------------------------------------------------------------------------|
| **dataset_id**           | The TDR dataset ID.                                                                                                                                                                                | String | Yes      | N/A                                                                                           |
| **file_query_limit**     | The number of file records to include in a batch.                                                                                                                                                  | Int    | No       | 20000                                                                                         |
| **docker**               | Specifies a custom Docker image to use. Optional.                                                                                                                                                  | String | No       | us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest   |

## Outputs Table:
This script does not generate any outputs directly. However, logs will be provided to track the progress of the file listing and deleting process, including any retries or errors encountered. You can review the logs in the stderr file for details.
