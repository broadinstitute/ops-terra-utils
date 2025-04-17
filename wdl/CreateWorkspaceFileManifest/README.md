# WDL Input Overview

This WDL lists all files located in the google bucket associated with a given terra workspace, and uploads metadata for these files into the terra workspace into the file_metadata table.

## Inputs Table
 This workflow is designed to use `Run workflow with inputs defined by file paths` option. You can use `Run workflow(s) with inputs defined by data table` option if you set up data table specifically to use this wdl.

| Input Name                 | Description                                                                                                                                                                                                                                                             | Type   | Required  | Default   |
|----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|-----------|-----------|
| **billing_profile**        | The billing profile associated with this workspace.                                                                                                                                                                                                                     | String | Yes       | N/A       |
| **workspace_name**         | Workspace name.                                                                                                                                                                                                                                                         | String | Yes       | N/A       |
| **extension_exclude_list** | List of file extensions to be excluded from search when listing files from this bucket. Will ignore any files that match an extension in this list. Do not include spaces in between values (i.e. ".tsv,.txt"). This and extension_include_list are mutually exclusive. | String | No        | N/A       |
| **extension_include_list** | List of file extensions to include when listing files from this bucket. Will ignore any files that do not match extensions in this list. Do not include spaces in between values (i.e. ".tsv,.txt"). This and extension_exclude_list are mutually exclusive             | String | No        | N/A       |
| **strings_to_exclude**     | List of string to exclude from listed files from this bucket. Will ignore any files that contain strings. (i.e. "/submissions/,/idats/"). Can be used with extension include/exclude lists                                                                              | String | No        | N/A       |

## Outputs Table

This workflow once completed will populate the file_metadata table under the data section of the terra workspace specified in the inputs.
