# WDL Input Overview

This WDL lists all files located in the google bucket associated with a given terra workspace, and uploads metadata for these files into the terra workspace into the file_metadata table.

## Inputs Table

| Input Name | Description | Type | Required | Default |
|------------------------------------------|------------------------------------------------------------------------------------------------------------------------|---------|----------|---------|
| **billing_profile**| The billing profile associated with this workspace.| String  | Yes | N/A |
| **workspace_name**| Workspace name.| String  | Yes | N/A |
| **extension_exclude_list**| List of file extensions to be excluded from search when listing files from this bucket. Will ignore any files that match an extension in this list. | String  | No | N/A |
| **extension_include_list**| List of file extensions to include when listing files from this bucket. Will ignore any files that do not match extensions in this list. | String  | No | N/A |

## Outputs Table

This workflow once completed will populate the file_metadata table under the data section of the terra workspace specified in the inputs.
