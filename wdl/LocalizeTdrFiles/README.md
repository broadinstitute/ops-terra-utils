# WDL Input Overview

This WDL script copies paired files (e.g. CRAM and CRAI or VCF and index) into a Terra workspace bucket and optionally updates the corresponding workspace data table with the new file paths.

## Inputs Table:
This workflow is designed to use `Run workflow(s) with inputs defined by data table` option.

| Input Name                    | Description                                                                                                                                                                                    | Type          | Required | Default |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|---------|
| **primary_files**             | Array of primary files to be copied, e.g. .cram, .bam, .vcf.gz.                                                                                                                                | Array[String] | Yes      | N/A     |
| **secondary_files**           | Array of secondary files to be copied, e.g. .crai, .bai, .vcf.gz.tbi.                                                                                                                          | Array[String] | Yes      | N/A     | | String  | Yes      | N/A                                                                                         |
| **entity_ids**                | Array of entity IDs, e.g. sample_id.                                                                                                                                                           | Array[String] | Yes      | N/A     |
| **workspace_bucket**          | The workspace bucket path, e.g. "gs://fc-1a2b3c.."                                                                                                                                             | String        | Yes      | N/A     |
| **subdirectory_name**         | The name of new directory to be created in the workspace bucket where files are copied to.                                                                                                     | String        | Yes      | N/A     |
| **update_data_tables**        | If `true`, the data table will be updated with new columns containing the paths of the copied files. Otherwise, the user can inspect the output tsv file and manually upload to the workspace. | Boolean       | No       | true    |
| **billing_project**           | The billing project of the workspace.                                                                                                                                                          | String        | Yes      | N/A     |
| **workspace_name**            | The name of the workspace.                                                                                                                                                                     | String        | Yes      | N/A     |
| **data_table_name**           | The name of the data table.                                                                                                                                                                    | String        | Yes      | N/A     |
| **new_primary_column_name**   | The name to be used for the new column containing the localized `primary_files` file paths, e.g. "new_cram_paths".                                                                             | String        | Yes      | N/A     |
| **new_secondary_column_name** | The name to be used for the new column containing the localized `secondary_files` file paths, e.g. "new_cram_index_paths"                                                                      | String        | Yes      | N/A     |

## Outputs Table:
This WDL generates a TSV file containing the following columns and its corresponding values: entity_ids, new_primary_column_name, new_secondary_column_name.