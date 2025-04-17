# WDL Input Overview

This WDL creates the md5sum of a GCP object. It calculates the md5 by streaming the file and does not download it locally.

## Inputs Table:
 This workflow is designed to use `Run workflow(s) with inputs defined by data table` option.

| Input Name                | Description                                                                                      | Type    | Required | Default                                                                                     |
|---------------------------|--------------------------------------------------------------------------------------------------|---------|----------|---------------------------------------------------------------------------------------------|
| **gcp_file_path**         | Path to the GCP file.                                                                            | String  | Yes      | N/A                                                                                         |
| **create_cloud_md5_file** | If `true`, then copy up md5 file to sit next to gcp_file_path. Will be named {gcp_file_path}.md5 | Boolean | Yes      | N/A                                                                                         |
| **md5_format**            | Can either get md5 as hex (what md5sum returns) or base63 (what gcp stores).                     | String  | No       | hex                                                                                         |
| **docker**                | Allows the use of a different Docker image.                                                      | String  | No       | us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest |
| **memory_gb**             | GB of memory to use when streaming and calculating the md5.                                      | Int     | No       | 4                                                                                           |

## Outputs table:
| Output Name    | Description       | Type    |
|----------------|-------------------|---------|
| **md5_hash**   | md5 hash of file  | String  |                                                                                     |
