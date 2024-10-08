# WDL Input Overview

This WDL script creates a new workspace that is nearly identical to the source workspace. It updates all metadata to point towards the new bucket and copies all files into this new bucket. Note that if metadata contains references to other tables, they may not transfer correctly and might appear as dictionaries or JSON in the new workspace.

## Inputs Table:

| Input Name                 | Description                                                                                                        | Type    | Required | Default |
|----------------------------|--------------------------------------------------------------------------------------------------------------------|---------|----------|---------|
| **source_billing_project** | The billing project for the source workspace.                                                                      | String  | Yes      | N/A     |
| **source_workspace_name**  | The name of the source workspace that will be copied.                                                              | String  | Yes      | N/A     |
| **dest_billing_project**   | The billing project for the new destination workspace.                                                             | String  | Yes      | N/A     |
| **dest_workspace_name**    | The name of the new workspace to be created.                                                                       | String  | Yes      | N/A     |
| **allow_already_created**  | If `true`, allows the script to proceed without failing if the destination workspace already exists.               | Boolean | Yes      | N/A     |
| **workers**                | The number of worker threads to use for the file transfer. Optional.                                               | Int     | No       | 10      |
| **extensions_to_ignore**   | A comma-separated list of file extensions to ignore during the file copy process. Optional.                        | String  | No       | N/A     |
| **docker_name**            | Specifies a custom Docker image to use. Optional.                                                                  | String  | No       | N/A     |
| **memory_gb**              | How much memory given to task                                                                                      | Int     | No       | 8       |
| **batch_size**             | Can be used if you want to batch up how much files at a time to copy over. If not used will do it all in one batch | Int     | No       | N/A     |

## Outputs Table:
This script does not generate direct outputs. However, logs will track the progress of workspace creation, file transfer, and metadata updates. The logs, which include any errors or warnings, can be reviewed in the stderr file for additional details about the process.
