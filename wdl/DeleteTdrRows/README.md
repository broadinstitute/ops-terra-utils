# WDL Input Overview
This WDl will delete rows from a table in a TDR. You can specify the table and the rows to delete. Optionally can delete files linked to the rows.

If data / files being deleted are STILL part of active snapshot you will run into issues. Make sure to delete the snapshots first.

## Inputs Table:
| Input Name               | Description                                                       | Type          | Required | Default                                                                                       |
|--------------------------|-------------------------------------------------------------------|---------------|----------|-----------------------------------------------------------------------------------------------|
| **dataset_id**           | dataset id where table exists                                     | String        | Yes      | N/A                                                                                           |
| **tdr_table_name**       | Table name in dataset                                             | String        | Yes      | N/A                                                                                           |
| **ids_to_delete**        | list of ids to look for and delete in table                       | Array[String] | Yes      | N/A                                                                                           |
| **id_column_name**       | Name of column where ids exist                                    | String        | Yes      | N/A                                                                                           |
| **delete_files**         | Use if want to delete files that are referenced in rows to delete | Boolean       | Yes      | N/A                                                                                           |
| **docker**               | Specifies a custom Docker image to use. Optional.                 | String        | No       | "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest" |


## Outputs Table:
This script does not generate any outputs directly. However, logs will be provided to track the progress of the dataset transfer, including details about dataset creation, table confirmation, and data ingestion. You can review the logs in the stderr file for information about the transfer process and the status of the ingestion jobs.
