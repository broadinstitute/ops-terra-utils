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
