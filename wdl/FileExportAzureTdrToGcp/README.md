# WDL Input Overview

This workflow performs the following operations:

1. Lists the files from the specified dataset using the list dataset files TDR api endpoint.
2. Download each file to the container running this workflow and then uploads it to the specied GCP bucket.
    1. during this step logging information is collected validation purposes after completion.

## Inputs Table
 This workflow is designed to use `Run workflow with inputs defined by file paths` option. You can use `Run workflow(s) with inputs defined by data table` option if you set up data table specifically to use this wdl.

| Input Name                | Description                                                                                                                        |   Type   | Required | Default |
|:--------------------------|:-----------------------------------------------------------------------------------------------------------------------------------|:--------:|:--------:|:-------:|
| **export_type**           | Endpoint type from TDR to export files from, dataset is currently only supported.                                                  | String   |   True   |   N/A   |
| **target_id**             | UUID of dataset to query                                                                                                           |  String  |   True   |   N/A   |
| **bucket_id**             | Workspace bucket to export files to.                                                                                               |  String  |   True   |   N/A   |
| **bucket_output_path**    | Path to export files into within the workspace bucket e.g. gs://{bucket_uuid}/{bucket_output_path}                                 |  String  |  False   |   N/A   |
| **retain_path_structure** | Modifies export path to copy files to. Using path attribute from TDR to retain the path structure that a given file presently has. |   Bool   |  False   |  False  |

## Outputs Table

This script generates a manifest file, tracking the process and status of each file transfer and can be found in the copy_manifest.csv. Additional logs can be found in the stderr file for information about the transfer process and further details.
