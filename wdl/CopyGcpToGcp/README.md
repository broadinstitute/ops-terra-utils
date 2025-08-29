# WDL Input Overview

This WDL script gcp files to another gcp location. You can use EITHER fofn fo files to copy or a whole bucket as the source files. You CANNOT supply both.

It will ensure that all output files have different destination paths so nothing overwrites each other.

Make sure user running this has both read access to the source files and write access to the destination path

## Inputs Table:
 This workflow is designed to use `Run workflow with inputs defined by file paths` option. You can use `Run workflow(s) with inputs defined by data table` option if you set up data table specifically to use this wdl.

| Input Name             | Description                                                                                                                       | Type    | Required | Default                                                                                       |
|------------------------|-----------------------------------------------------------------------------------------------------------------------------------|---------|----------|-----------------------------------------------------------------------------------------------|
| **destination_path**   | Path where files should be copied. Could be just a bucket, like gs://bucket/, or a subdirectory like gs://bucket/path/to/copy/.   | String  | Yes      | N/A                                                                                           |
| **preserve_structure** | Set to true if you want directory structure maintained. If not used all files will go to top level of destination_path.           | Boolean | Yes      | N/A                                                                                           |
| **source_bucket**      | CANNOT be used with source_fofn. Source bucket where all files to copy live                                                       | String  | No       | N/A                                                                                           |
| **source_fofn**        | CANNOT be used with source_bucket. A fofn where each file you want to copy over is on a new line. Must be at accessible gcp path. | String  | No       | N/A                                                                                           |
| **google_project**     | Google project to be used when copying files from or to a requester pays bucket                                                   | String  | No       | N/A                                                                                           |
| **docker**             | Specifies a custom Docker image to use. Optional.                                                                                 | String  | No       | "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest" |


## Outputs Table:
This script does not generate any outputs directly. However, logs will be provided to track the progress of the dataset transfer, including details about dataset creation, table confirmation, and data ingestion. You can review the logs in the stderr file for information about the transfer process and the status of the ingestion jobs.
