# WDL Input Overview

This WDL re-uploads a gcp object so that the md5 will be stored in its metadata. To do the re-upload it uses gcloud storage cp with daisy chain mode.

## Inputs Table:
| Input Name                  | Description                                                                     | Type   | Required | Default                                                                                     |
|-----------------------------|---------------------------------------------------------------------------------|--------|----------|---------------------------------------------------------------------------------------------|
| **gcp_file_path**           | Path to the GCP file.                                                           | String | Yes      | N/A                                                                                         |
| **requester_pays_project**  | Can pass in google project to use and charge if bucket is set to requester pays | String | No       | N/A                                                                                         |
| **docker**                  | Allows the use of a different Docker image.                                     | String | No       | us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest |
| **memory_gb**               | GB of memory to use                                                             | Int    | No       | 2                                                                                           |
| **disk_size**               | GB of disk to use                                                               | Int    | No       | 2                                                                                           |

## Output:
There is no output of this workflow
