# WDL Input Overview

This WDL accepts either a GCS path to a TSV file or a combination of a billing project and workspace to generate a TDR
schema JSON based on the provided metadata. Note that this script does not interact with TDR. Its purpose is to
demonstrate what the resulting schema would look like if the data were imported into TDR.

## Notes
* Please provide _either_ the `input_metadata_tsv` OR a combination of `billing_project`, `workspace_name`, and
  `terra_table_names`.

## Inputs Table:

| Input Name                         | Description                                                                                                                                                                                               | Type      | Required | Default                                                                                     |
|------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|----------|---------------------------------------------------------------------------------------------|
| **input_metadata_tsv**             | A GSC path to the TSV file containing metadata (if not using a workspace as input). Must start with 'gs://'                                                                                               | String    | No       | N/A                                                                                         |
| **billing_project**                | The workspace billing project (if not using the TSV)                                                                                                                                                      | String    | No       | N/A                                                                                         |
| **workspace_name**                 | The workspace name (if not using the TSV)                                                                                                                                                                 | String    | No       | N/A                                                                                         |
| **terra_table_names**              | Comma separate list of Terra table names to generate JSONs for. Do not include spaces between entries (i.e. use the following format: "table1,table2")                                                    | String    | No       | N/A                                                                                         |
| **force_disparate_rows_to_string** | If rows of a column are of different data types, setting this to True will force them all to be strings in the resulting TDR schema JSON. The same option will be available when importing data into TDR. | Boolean   | Yes      | N/A                                                                                         |
| **docker**                         | Specifies a custom Docker image to use. Optional.                                                                                                                                                         | String    | No       | us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest |


## Outputs Table:
| Output Name           | Description                                                                                                                                                     |
|-----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **tdr_schema_json**   | The path to the GSC file containing the schema JSON. If multiple Terra tables were provided as input, all tables will be included in the same output JSON file. |
