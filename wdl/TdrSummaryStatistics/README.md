# WDL Input Overview

This WDL script creates a tsv that has summary statistics for either a TDR dataset or snapshot. **ONE OF SNAPSHOT OR DATASET NEEDS TO BE PROVIDED.**

## Inputs Table:
 This workflow is designed to use `Run workflow with inputs defined by file paths` option. You can use `Run workflow(s) with inputs defined by data table` option if you set up data table specifically to use this wdl.

| Input Name      | Description                                              | Type   | Required | Default                                                                                     |
|-----------------|----------------------------------------------------------|--------|----------|---------------------------------------------------------------------------------------------|
| **dataset_id**  | The TDR dataset ID. Cannot be provided with snapshot_id. | String | No       | N/A                                                                                         |
| **snapshot_id** | The TDR snapshot ID. Cannot be provided with dataset_id. | String | No       | N/A                                                                                         |
| **docker**      | Specifies a custom Docker image to use. Optional.        | String | No       | us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest |

## Outputs Table:

| Output Name            | Description                                                    |
|------------------------|----------------------------------------------------------------|
| **summary_statistics** | Tsv with summary statistics for either dataset or snapshot.    |
