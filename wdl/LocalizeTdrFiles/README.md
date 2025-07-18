# WDL Input Overview

This WDL script creates a new workspace that is nearly identical to the source workspace. It updates all metadata to point towards the new bucket and copies all files into this new bucket. Note that if metadata contains references to other tables, they may not transfer correctly and might appear as dictionaries or JSON in the new workspace.
This WDL copies files from a TDR bucket to 
>[!CAUTION]
>Library attributes are no longer supported with Terra. If your source workspace has library attributes, they will
> not be copied to the cloned workspace.

>[!IMPORTANT]
> **If you are NOT an OWNER of the original workspace, use the `do_not_update_acls` option.** If you do not use it, and
> you are not an OWNER, this script will run into issues when trying to get ACLs of the source workspace.

## Inputs Table:
This workflow is designed to use `Run workflow with inputs defined by file paths` option. You can use `Run workflow(s) with inputs defined by data table` option if you set up data table specifically to use this wdl.

| Input Name                     | Description                                                                                                                                                                                                  | Type    | Required | Default                                                                                     |
|--------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|----------|---------------------------------------------------------------------------------------------|
| **data**                       | The billing project for the workspace that contains the data table.                                                                                                                                          | String  | Yes      | N/A                                                                                         |
| **data_index**                 | The name of the workspace that contains the data table.                                                                                                                                                      | String  | Yes      | N/A                                                                                         | | String  | Yes      | N/A                                                                                         |
| **entity**                     | If `true`, allows the script to proceed without failing if the destination workspace already exists.                                                                                                         | Boolean | Yes      | N/A                                                                                         |
| **workspace_bucket**           | If you want to skip updating ACLs to match source workspace. If you don't have OWNER access of source workspace you HAVE to use this option or it will fail.                                                 | Boolean | Yes      | N/A                                                                                         |
| **subdirectory_name**          | When used, workflow will check write permissions on destination bucket every 30 minutes for 5 hours total before exiting. Useful for when permissions were newly added and could take some time to propagate | Boolean | Yes      | N/A                                                                                         |
| **update_data_tables**         | Skips initial check to see if files already copied over. If this is the first time you are copying over files it will save time to set to this to `true`                                                     | Boolean | Yes      | N/A                                                                                         |
| **billing_project**            | The number of worker threads to use for the file transfer. Optional.                                                                                                                                         | Int     | No       | 10                                                                                          |
| **workspace_name**             | A comma-separated list of file extensions to ignore during the file copy process. Optional. Do not include spaces (i.e. ".txt,.tsv")                                                                         | String  | No       | N/A                                                                                         |
| **data_table_name**            | Specifies a custom Docker image to use. Optional.                                                                                                                                                            | String  | No       | us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest |
| **new_data_column_name**       | How much memory given to task                                                                                                                                                                                | Int     | No       | 8                                                                                           |
| **new_data_index_column_name** | Can be used if you want to batch up how much files at a time to copy over. If not used will do it all in one batch.                                                                                          | Int     | No       | N/A                                                                                         |
| **max_permissions_wait_time**  | Optional total time to wait for permissions to propagate. Defaults to 5 hours. Won't run for more than 5 hours total.                                                                                        | Int     | No       | 5                                                                                           |


## Outputs Table:
This script does not generate direct outputs. However, logs will track the progress of workspace creation, file transfer, and metadata updates. The logs, which include any errors or warnings, can be reviewed in the stderr file for additional details about the process.
