# WDL Input Overview
This workflow submits batch jobs to start a DRAGEN pipeline on a set of CRAM files.

## Inputs Table:
 This workflow is designed to use `Run workflow(s) with inputs defined by data table` option.

| Input Name                   | Description                                                                           | Type          | Required | Default                                                                                     |
|------------------------------|---------------------------------------------------------------------------------------|---------------|----------|---------------------------------------------------------------------------------------------|
| **research_project**         | RP for samples. Used for output bucket                                                | String        | Yes      | N/A                                                                                         |
| **ref_trigger**              | Ref trigger                                                                           | String        | Yes      | N/A                                                                                         |
| **ref_dragen_config**        | Config json for dragen.                                                               | String        | Yes      | N/A                                                                                         |
| **ref_batch_config**         | Batch json config file                                                                | String        | Yes      | N/A                                                                                         |
| **output_bucket**            | GCP bucket for output. Do not include `gs://` or `/` at the end.                      | String        | Yes      | N/A                                                                                         |
| **project_id**               | GCP project where batch jobs will run.                                                | String        | Yes      | N/A                                                                                         |
| **data_type**                | Data type, like WGS or BGE.                                                           | String        | Yes      | N/A                                                                                         |
| **dragen_version**           | What version of Dragen. Used just for path where to put batch_config and ref_trigger. | String        | Yes      | N/A                                                                                         |
| **cram_paths**               | List of cram paths.                                                                   | Array[String] | Yes      | N/A                                                                                         |
| **sample_ids**               | List of sample ids                                                                    | Array[String] | Yes      | N/A                                                                                         |

## Outputs Table:
| Output Name         | Description                                        | Type   |
|---------------------|----------------------------------------------------|--------|
| **batch_config**    | Path to batch_config                               | File   |
| **dragen_config**   | Path to dragen_config                              | File   |
| **sample_manifest** | Path to sample_manifest.                           | File   |
| **status**          | Status of sample set. Will always be "Kicked off". | String |
