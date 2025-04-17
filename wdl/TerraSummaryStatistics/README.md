# WDL Input Overview

This WDL script creates a tsv that has summary statistics for a workspace. You may provide expected values so that the script can check if the workspace has the expected values.

## Inputs Table:
 This workflow is designed to use `Run workflow with inputs defined by file paths` option. You can use `Run workflow(s) with inputs defined by data table` option if you set up data table specifically to use this wdl.

| Input Name               | Description                                                                        | Type   | Required | Default                                                                                     |
|--------------------------|------------------------------------------------------------------------------------|--------|----------|---------------------------------------------------------------------------------------------|
| **billing_project**      | billing project                                                                    | String | Yes      | N/A                                                                                         |
| **workspace_name**       | workspace name.                                                                    | String | Yes      | N/A                                                                                         |
| **data_dictionary_file** | File that can be used to validate expected matches what is found. More info below. | File   | No       | N/A                                                                                         |
| **docker**               | Specifies a custom Docker image to use. Optional.                                  | String | No       | us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest |

# Data dictionary file

This optional file can be used to provide expected information on the columns in the workspace. This file should have the following headers:

#### Headers
- table_name
- column_name
- label
- multiple_values_allowed
- description
- data_type
- primary_key
- refers_to_column - column in another table that this column references
- allowed_values_pattern - list of acceptable values for column
- allowed_values_list - regex to match column values against
- required

You do not need to include every table/column pair from the Terra Workspace.

#### Data Types

Below are the possible data types that can be used in the data dictionary file and what they map to:

| Input DTs | TDR DTs  |
|-----------|----------|
| boolean   | boolean  |
| date      | date     |
| datetime  | datetime |
| float     | float64  |
| int       | int64    |
| string    | string   |
| fileref   | fileref  |

## Outputs Table:
| Output Name            | Description                                |
|------------------------|--------------------------------------------|
| **summary_statistics** | Tsv with summary statistics for workspace. |
