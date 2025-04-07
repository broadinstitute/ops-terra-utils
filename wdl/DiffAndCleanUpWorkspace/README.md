# WDL Input Overview

This gets you a list of files in both the dataset and the workspace as well as files that are just in the workspace and not the dataset. This can also delete the files in either one of those lists. This workflow will ignore files that include `/DiffAndCleanUpWorkspace/` in the path so that it does not delete itself.

## Notes
* If a dataset is self-hosted then you CANNOT delete files that are in both the dataset and the workspace. This is because the dataset is just referencing the files in the workspace.

## Inputs Table:
| Input Name                   | Description                                                                                                                                                                                                                                     | Type   | Required | Default |
|------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|----------|---------|
| **dataset_id**               | dataset id that is linked to staging workspace                                                                                                                                                                                                  | String | Yes      | N/A     |
| **billing_project**          | Billing project of the staging workspace.                                                                                                                                                                                                       | String | Yes      | N/A     |
| **workspace_name**           | Workspace name of the staging workspace.                                                                                                                                                                                                        | String | Yes      | N/A     |
| **cloud_directory**          | GCP cloud directory to write the fofns for files in just the workspace and files in both the workspace and the dataset.                                                                                                                         | String | Yes      | N/A     |
| **delete_from_workspace**    | Options are 'workspace_only_file' or 'files_in_both'. workspace_only_file will clean up files in Terra workspace where they do not exist in dataset. files_in_both will clean up files already ingested. This is NOT an option for self hosted. | String | No       | N/A     |
| **google_project**           | Google project to use for gcp operations. Will be needed if workspace bucket is requester pays                                                                                                                                                  | String | No       | N/A     |
| **file_paths_to_ignore**     | comma seperated list of file paths to ignore (recursively) when looking for files to delete.                                                                                                                                                    | String | No       | N/A     |
| **docker**                   | Specifies a custom Docker image to use. Optional.                                                                                                                                                                                               | String | No       | N/A     |


## Outputs Table:
This script outputs a file with all files that should be deleted.
