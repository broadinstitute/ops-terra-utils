# RecreateAnvilMetadata WDL

This WDL workflow runs the `recreate_anvil_metadata.py` script to regenerate and upload Terra workspace tables from a TDR dataset and workspace bucket.

## Inputs

| Name                   | Type      | Required | Description                                                        |
|------------------------|-----------|----------|--------------------------------------------------------------------|
| workspace_name         | String    | Yes      | The name of the Terra workspace                                    |
| billing_project        | String    | Yes      | The Terra billing project name                                     |
| dataset_id             | String    | Yes      | The TDR dataset ID to recreate metadata for                        |
| force                  | Boolean   | No       | Force upload if tables already exist in the workspace (default: no) |
| tables_to_ignore       | String    | No       | Comma-separated list of table names to ignore                      |
| table_prefix_to_ignore | String    | No       | Ignore tables with this prefix                                     |

## Outputs

| Name           | Type | Description                |
|----------------|------|----------------------------|
| script_stdout  | File | Standard output of script  |
| script_stderr  | File | Standard error of script   |

## Usage

This workflow is intended to be run in a Cromwell-compatible environment. It will call the Python script with the provided arguments and upload the processed tables to the specified Terra workspace.

## Example

```
inputs.json:
{
  "RecreateAnvilMetadata.workspace_name": "my-workspace",
  "RecreateAnvilMetadata.billing_project": "my-billing-project",
  "RecreateAnvilMetadata.dataset_id": "my-dataset-id",
  "RecreateAnvilMetadata.force": true,
  "RecreateAnvilMetadata.tables_to_ignore": "table1,table2",
  "RecreateAnvilMetadata.table_prefix_to_ignore": "tmp_"
}
```

Run with:

```
cromwell run wdl/RecreateAnvilMetadata/RecreateAnvilMetadata.wdl --inputs inputs.json
```
