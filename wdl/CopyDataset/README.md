# WDL Input Overview

This WDL script copies a dataset. It either uses an existing dataset or creates a new dataset in the requested billing profile for the destination. The script ensures that the required tables are created or confirmed in the new dataset and then ingests all metadata and files from the original dataset into the new one. The original dataset will not be deleted after the process. If tables already exist in destination dataset there is no check to see if schema matches the source dataset table.

It can take from 12-24 hours for TDR SA ingest account permissions to propagate to the dataset. If fails with permission error, wait 24 hours and try again.

## Inputs Table:
 This workflow is designed to use `Run workflow with inputs defined by file paths` option. You can use `Run workflow(s) with inputs defined by data table` option if you set up data table specifically to use this wdl.

| Input Name                               | Description                                                                                                             | Type    | Required | Default |
|------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|---------|----------|---------|
| **new_billing_profile**                  | The billing profile ID where the dataset will be transferred.                                                           | String  | Yes      | N/A     |
| **orig_dataset_id**                      | The ID of the original dataset that will be transferred.                                                                | String  | Yes      | N/A     |
| **bulk_mode**                            | If `true`, enables bulk ingestion mode for faster transfers.                                                            | Boolean | Yes      | N/A     |
| **new_dataset_name**                     | The name for the new dataset. Cannot be the same as original.                                                           | String  | Yes      | N/A     |
| **filter_out_entity_already_in_dataset** | Use if you data might already be ingested and you want to skip any rows where the id already exists in the dest dataset | Boolean | Yes      | N/A     |
| **continue_if_exists**                   | If `true`, the script will continue even if the dataset already exists. If `false`, it will raise an error.             | Boolean | Yes      | N/A     |
| **ingest_batch_size**                    | The batch size for ingesting data into the new dataset. Optional.                                                       | Int     | No       | 500     |
| **update_strategy**                      | Specifies how to handle updates. Default is `REPLACE`. Optional.                                                        | String  | No       | REPLACE |
| **waiting_time_to_poll**                 | The time, in seconds, to wait between polling the status of the ingest job. Optional.                                   | Int     | No       | 120     |
| **docker**                               | Specifies a custom Docker image to use. Optional.                                                                       | String  | No       | N/A     |


## Outputs Table:
This script does not generate any outputs directly. However, logs will be provided to track the progress of the dataset transfer, including details about dataset creation, table confirmation, and data ingestion. You can review the logs in the stderr file for information about the transfer process and the status of the ingestion jobs.
