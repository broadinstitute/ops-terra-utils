# WDL Input Overview
This workflow ingests one or more tabular files (`.tsv` or `.csv`) stored in GCS into tables in an existing TDR dataset. It is the file-based counterpart to `TerraWorkspaceTableToTDRIngest`: use it when the data is too large (or otherwise impractical) to load into a Terra workspace data table first. It performs the same steps as that workflow (validating headers, creating/updating the TDR table schema, optionally truncating or filtering existing rows, and batch ingesting rows and referenced files), but starts from the file instead of a workspace table.

The dataset must already exist. The target table does not need to exist; it will be created (or checked against the existing schema) based on the file contents.

## Ingesting multiple files
`input_files`, `target_table_names` and (optionally) `primary_keys` are matched up **by position**: the second file is ingested into the second table name using the second primary key, and so on. Each file is then processed one at a time, going through the full process of validating, creating/updating the table schema, and ingesting. All other inputs (`update_strategy`, `bulk_mode`, `batch_size`, `records_to_ingest`, etc.) apply to every table.
* There must be exactly one entry in `target_table_names` per input file, and the table names must be unique (the workflow fails if the same table name is listed more than once).
* `primary_keys` can be left out entirely. If provided it must have one entry per file; use an empty string `""` for any file that should use its first column (e.g. `["id", "", "sample_id"]`).
* The list lengths, file paths/extensions and table names (including uniqueness) are all checked before anything is downloaded or ingested. The contents of each file (headers, primary key values) are validated when that file's turn comes, so a problem in a later file is only found after earlier tables have been ingested. If any file fails, the workflow stops and the remaining tables are not ingested.
* `records_to_ingest` is applied to every table, so it is mostly useful when ingesting a single table or when the primary key values are shared across files.
* Values must not contain commas, since the lists are passed to the script as comma-separated strings.
* To load a single table, pass single-element arrays (e.g. `["gs://bucket/table.tsv"]`).

Example inputs for loading three tables, where the second table uses its first column as the primary key:
```json
{
    "TerraTableDataFileToTDRIngest.input_files": ["gs://bucket/a.tsv", "gs://bucket/b.tsv", "gs://bucket/c.csv"],
    "TerraTableDataFileToTDRIngest.target_table_names": ["table_a", "table_b", "table_c"],
    "TerraTableDataFileToTDRIngest.primary_keys": ["a_id", "", "c_id"]
}
```

## File requirements
* The first row must be a header row. Headers must follow TDR rules (start with a letter, contain only letters, numbers and underscores, max 63 characters).
* The delimiter is inferred from the file extension: `.csv` is comma-delimited and `.tsv` is tab-delimited. Any other extension causes the workflow to fail.
* The primary key is the first column unless a primary key is provided for the file in `primary_keys`. Primary key values must be non-empty and unique.
* All values are read as text and converted to types for schema inference: empty values become null, whole numbers become integers, decimals become floats, `true`/`false` become booleans, everything else is a string. Values with leading zeros (e.g. `007`) stay strings. Columns with mixed types are an error unless `force_disparate_rows_to_string` is true (the default).
* Values starting with `gs://` are treated as file references and the files will be ingested into TDR.
* The TDR dataset's ingest service account must have read access to the bucket(s) holding any referenced files. The account running the workflow must have read access to the input files.

## Inputs Table:
| Input Name                          | Description                                                                                                                                                                                                                                         | Type          | Required | Default                     |
|-------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|----------|-----------------------------|
| **input_files**                     | GCS paths (`gs://...`) of the tsv or csv files to ingest.                                                                                                                                                                                           | Array[String] | Yes      | N/A                         |
| **dataset_id**                      | The ID of the TDR dataset to ingest to.                                                                                                                                                                                                             | String        | Yes      | N/A                         |
| **target_table_names**              | Names of the tables in the TDR dataset to ingest into. Matched to `input_files` by position, so must have the same number of entries.                                                                                                               | Array[String] | Yes      | N/A                         |
| **primary_keys**                    | Columns to use as the TDR primary keys. Matched to `input_files` by position; if provided must have one entry per file (use `""` to default that file).                                                                                             | Array[String] | No       | First column in each file   |
| **bulk_mode**                       | Enables bulk mode for faster ingestion of a large number of files.                                                                                                                                                                                  | Boolean       | Yes      | N/A                         |
| **check_existing_ingested_files**   | If used will download all files for the dataset and replace file paths with existing UUIDs. Drastically improves ingest speed if files already existed. Only works if tdr 'path' ingested previously is original gs path with 'gs://' stripped out. | Boolean       | Yes      | N/A                         |
| **filter_existing_ids**             | If used will check table before ingesting to see if unique id already exists and skip ingesting that row if it does.                                                                                                                                | Boolean       | Yes      | N/A                         |
| **all_fields_non_required**         | If true, all fields in the table will be set to non-required besides the primary key.                                                                                                                                                               | Boolean       | Yes      | N/A                         |
| **update_strategy**                 | Specifies how to handle updates during ingestion. Options are `REPLACE`, `APPEND`, or `UPDATE`.                                                                                                                                                     | String        | No       | `REPLACE`                   |
| **records_to_ingest**               | Optional primary key values to ingest if not all rows are desired. Provide all IDs separated by commas with no spaces.                                                                                                                              | String        | No       | N/A                         |
| **max_retries**                     | The maximum number of retries for a failed request.                                                                                                                                                                                                 | Int           | No       | 5                           |
| **max_backoff_time**                | The maximum backoff time, in seconds, for a failed request.                                                                                                                                                                                         | Int           | No       | 300                         |
| **batch_size**                      | Number of rows to ingest at a time.                                                                                                                                                                                                                 | Int           | No       | 500                         |
| **force_disparate_rows_to_string**  | If rows of a column are of different data types, setting this to true will force them all to be strings in the resulting TDR table schema.                                                                                                          | Boolean       | No       | true                        |
| **trunc_and_reload**                | If true, will soft-delete all rows in the target TDR table before ingesting (NOTE this deletes ALL records in the table).                                                                                                                           | Boolean       | Yes      | N/A                         |
| **ignore_existing_schema_mismatch** | If true, will ignore schema mismatch between the file and an existing TDR table and attempt to ingest anyway, forcing the data to match the TDR schema.                                                                                             | Boolean       | No       | false                       |
| **docker**                          | Optional override of the docker image.                                                                                                                                                                                                              | String        | No       | ops_terra_utils_slim:latest |

## Outputs Table:
This workflow does not generate any outputs directly. Logs in the stderr file track ingest progress, errors and retries.
