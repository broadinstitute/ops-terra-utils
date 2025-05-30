# WDL Input Overview
This workflow will copy metadata and files from a tsv into a Terra workspace. All GCP files linked from the sheet will be copied to the workspace bucket and entries from the TSV that contained a link to a file path will be updated to point to the new file locations.

## Prerequisites
* Make sure that your proxy service account has access to the source files in the tsv

## Key Points
* The table created will be the same name as the id_column
* The id_column must be unique, be in every column, and only contain alphanumeric characters, underscores, dashes, and periods.

## Inputs Table:
 This workflow is designed to use `Run workflow with inputs defined by file paths` option. You can use `Run workflow(s) with inputs defined by data table` option if you set up data table specifically to use this wdl.

| Input Name              | Description                                                                                                                                                                                                                                   | Type    | Required | Default                                                                                     |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|----------|---------------------------------------------------------------------------------------------|
| **billing_project**     | The Terra billing project.                                                                                                                                                                                                                    | String  | Yes      | N/A                                                                                         |
| **workspace_name**      | The GCP Workspace ingest the metadata and files into.                                                                                                                                                                                         | String  | Yes      | N/A                                                                                         |
| **metrics_tsv**         | Source tsv                                                                                                                                                                                                                                    | File    | Yes      | N/A                                                                                         |
| **flatten_path**        | Use if you want all paths to be in same directory. If not used it will mantain current path as source files (with bucket updated)                                                                                                             | Boolean | Yes      | N/A                                                                                         |
| **id_column**           | Column to be used as the primary key in the Terra table. Must be present in the intput tsv.                                                                                                                                                   | String  | Yes      | N/A                                                                                         |
| **skip_upload_columns** | Pass in comma seperated list (no spaces) of columns you do not want to try copying files in from. This is only helpful if there is columns WITH file paths in it that you do NOT want copied in                                               | String  | No       | N/A                                                                                         |
| **subdir**              | Subdirectory to put files into in new bucket. If flatten path is used all files will be directory in this directory. If flatten is not used then the path structure will stay intact, but all paths will start with `gs://{bucket}/{subdir}/` | String  | No       | N/A                                                                                         |
| **docker**              | Specifies a custom Docker image to use. Optional.                                                                                                                                                                                             | String  | No       | us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest |


## Outputs Table:
This script does not generate any outputs directly. However, logs will be provided to track the progress of the file ingestion and metadata transfer. These logs will include details on ingestion status, any errors encountered, and retries if necessary. You can review the logs in the stderr file for detailed information.
