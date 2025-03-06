# WDL Input Overview

This WDL Cleans up staging workspace by deleting all files in the workspace that are not referenced in the TDR dataset. This workflow will ignore files that include `/CleanUpStagingWorkspace/` in the path so that it does not delete itself.

## Notes
* This is ONLY to be used for self-hosted datasets. If the dataset is not self-hosted this will fail.

## Inputs Table:
| Input Name               | Description                                                                                  | Type    | Required | Default |
|--------------------------|----------------------------------------------------------------------------------------------|---------|----------|---------|
| **dataset_id**           | dataset id that is linked to staging workspace                                               | String  | Yes      | N/A     |
| **billing_project**      | Billing project of the staging workspace.                                                    | String  | Yes      | N/A     |
| **workspace_name**       | Workspace name of the staging workspace.                                                     | String  | Yes      | N/A     |
| **output_file**          | Output file to put all files that should be deleted.                                         | Boolean | Yes      | N/A     |
| **run_deletes**          | If set to false it will only report what files should be deleted                             | Boolean | Yes      | N/A     |
| **file_paths_to_ignore** | comma seperated list of file paths to ignore (recursively) when looking for files to delete. | String  | No       | N/A     |
| **docker**               | Specifies a custom Docker image to use. Optional.                                            | String  | No       | N/A     |


## Outputs Table:
This script outputs a file with all files that should be deleted.
