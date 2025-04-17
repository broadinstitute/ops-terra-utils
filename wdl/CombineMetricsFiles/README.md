# WDL Input Overview

This WDL copies will combine multiple metrics files into one file while adding an identifier column. The identifier column can be specified or the metrics file name will be used as the identifier. Whichever identifier is used it must be unique. The output file will be saved to a specified GCP path.

An example of the inputs -> output is shown below:

Inputs would be
```
metric_name1,metric_value1
metric_name2,metric_value2
```
```
metric_namex,metric_valuex
metric_namey,metric_valuey
metric_namez,metric_valuez
```
Outputs would be (assuming identifier is `sample_a` and `sample_b`)
```
sample_a,metric_name1,metric_value1
sample_a,metric_name2,metric_value2
sample_b,metric_namex,metric_valuex
sample_b,metric_namey,metric_valuey
sample_b,metric_namez,metric_valuez
```

## Inputs Table:
| Input Name                | Description                                                                                                                                                                                     | Type   | Required | Default |
|---------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|----------|---------|
| **billing_project**       | Billing project for Terra workspace.                                                                                                                                                            | String | Yes      | N/A     |
| **workspace_name**        | Workspace name for Terra workspace.                                                                                                                                                             | String | Yes      | N/A     |
| **table_name**            | Terra table name where metadata is that has metrics files and potentially identifier column.                                                                                                    | String | Yes      | N/A     |
| **metrics_file_column**   | Name of the column where metrics files are listed.                                                                                                                                              | String | Yes      | N/A     |
| **output_gcp_path**       | The output file that contains the combined metrics.                                                                                                                                             | String | Yes      | N/A     |
| **identifier_column**     | What to use as the identifier in the combined metrics. Will be extra column put in the front. If NOT used then the metrics file name will be used as identifier. The identifier must be unique. | String | No       | N/A     |
| **docker**                | Specifies a custom Docker image to use. Optional.                                                                                                                                               | String | No       | N/A     |


## Outputs Table:
| Input Name                  | Description              | Type   |
|-----------------------------|--------------------------|--------|
| **combined_metrics**        | Combined metrics file    | String |
