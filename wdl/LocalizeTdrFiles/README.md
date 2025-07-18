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

| Input Name                     | Description                                                                                                                 | Type          | Required | Default |
|--------------------------------|-----------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------|
| **input_data**                 | Array of files to be copied, e.g. .cram, .bam, .vcf.gz.                                                                     | Array[String] | Yes      | N/A     |
| **input_data_index**           | Array of index files for `input_data` to be copied, e.g. .crai, .bai, .vcf.gz.tbi.                                          | Array[String] | Yes      | N/A     | | String  | Yes      | N/A                                                                                         |
| **input_entity_id**            | Array of entity IDs, e.g. sample_id.                                                                                        | Array[String] | Yes      | N/A     |
| **workspace_bucket**           | The workspace bucket path, e.g. "gs://fc-1a2b3c.."                                                                          | String        | Yes      | N/A     |
| **subdirectory_name**          | The name of new directory to be created in the workspace bucket where files are copied to.                                  | String        | Yes      | N/A     |
| **update_data_tables**         | If `true`, the data table will be updated with new columns containing the paths of the copied files. If `false`             | Boolean       | Yes      | true    |
| **billing_project**            | The billing project of the workspace.                                                                                       | String        | No       | N/A     |
| **workspace_name**             | The name of the workspace.                                                                                                  | String        | No       | N/A     |
| **data_table_name**            | The name of the data table.                                                                                                 | String        | No       | N/A     |
| **new_data_column_name**       | The name to be used for the new column containing the localized `input_data` file paths, e.g. "new_cram_paths".             | String        | No       | N/A     |
| **new_data_index_column_name** | The name to be used for the new column containing the localized `input_data_index` files paths, e.g. "new_cram_index_paths" | String        | No       | N/A     |

## Outputs Table:
This script does not generate direct outputs. However, logs will track the progress of workspace creation, file transfer, and metadata updates. The logs, which include any errors or warnings, can be reviewed in the stderr file for additional details about the process.
