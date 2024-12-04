# WDL Input Overview

This WDL script creates a new workspace that is nearly identical to the source workspace. It updates all metadata and copies all files to the GCP external_bucket passed in. Note that if metadata contains references to other tables, they may not transfer correctly and might appear as dictionaries or JSON in the new workspace.

## Notes

*  **If you are NOT an OWNER of the original workspace use do_not_update_acls option.** If you do not use it, and you are not an OWNER, this script will run into issues when trying get ACLs of the source workspace.
* Make sure your Terra "Proxy Group" has full access to the external_bucket.

## Inputs Table:

| Input Name                 | Description                                                                                                                                                     | Type     | Required | Default                                                                                     |
|----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|----------|---------------------------------------------------------------------------------------------|
| **source_billing_project** | The billing project for the source workspace.                                                                                                                   | String   | Yes      | N/A                                                                                         |
| **source_workspace_name**  | The name of the source workspace that will be copied.                                                                                                           | String   | Yes      | N/A                                                                                         |
| **dest_billing_project**   | The billing project for the new destination workspace.                                                                                                          | String   | Yes      | N/A                                                                                         |
| **external_bucket**        | External GCP bucket to copy files to and reference in metadata. Should be like gs://bucket/                                                                     | String   | Yes      | N/A                                                                                         |
| **dest_workspace_name**    | The name of the new workspace to be created.                                                                                                                    | String   | Yes      | N/A                                                                                         |
| **allow_already_created**  | If `true`, allows the script to proceed without failing if the destination workspace already exists.                                                            | Boolean  | Yes      | N/A                                                                                         |
| **rsync_workspace**        | If you would like to use rsync for copy instead of gcloud libraries. Can be quicker if large copies.                                                            | Boolean  | Yes      | N/A                                                                                         |
| **do_not_update_acls**     | If you want to skip updating ACLs to match source workspace. If you don't have OWNER access of source workspace you HAVE to use this option or it will fail.    | Boolean  | Yes      | N/A                                                                                         |
| **workers**                | The number of worker threads to use for the file transfer. Only used if not rsyncing. Optional.                                                                 | Int      | No       | 10                                                                                          |
| **extensions_to_ignore**   | A comma-separated list of file extensions to ignore during the file copy process. Only used if not rsyncing. Optional. Do not include spaces (i.e. ".txt,.tsv") | String   | No       | N/A                                                                                         |
| **docker**                 | Specifies a custom Docker image to use. Optional.                                                                                                               | String   | No       | us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest |
| **memory_gb**              | How much memory given to task                                                                                                                                   | Int      | No       | 8                                                                                           |
| **batch_size**             | Can be used if you want to batch up how much files at a time to copy over. If not used will do it all in one batch. Only used if not rsyncing                   | Int      | No       | N/A                                                                                         |

## Outputs Table:
This script does not generate direct outputs. However, logs will track the progress of workspace creation, file transfer, and metadata updates. The logs, which include any errors or warnings, can be reviewed in the stderr file for additional details about the process.