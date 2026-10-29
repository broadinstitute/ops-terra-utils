# WDL Input Overview

This WDL validates a staging workspace. It creates a tsv with summary statistics for the workspace tables (optionally checked against expected values from a data dictionary), and a markdown validation report covering the workspace bucket and the files referenced in the tables:

- Path to the summary statistics file, whether a data dictionary was provided (and the path to the copy of it), table, row and column counts (including file reference columns), and the columns flagged as needing attention
- Count/percent of files with no MD5 in the GCS metadata
- Count/percent of files without an index file: BAMs (`.bai`/`.csi`), CRAMs (`.crai`), VCFs and GVCFs (`.vcf.gz`, `.g.vcf.gz`, `.gvcf.gz`, `.vcf.bgz`; `.tbi`/`.csi`). An index can be named `<file>.bai` or `<file without extension>.bai`. Uncompressed `.vcf` files are not checked
- Count/percent of files in the workspace bucket not referenced anywhere in the tables (this includes anything Terra writes to the bucket, such as `submissions/`)
- Distribution of files in the workspace bucket by extension (count and total size)
- Files referenced in the tables that are not found. References to the workspace bucket that are not found do not exist in the bucket and are listed in a detail tsv. References to other buckets are listed per bucket with the number of files referenced, and are checked for existence and for an MD5 in the GCS metadata (buckets that cannot be accessed are noted in the report and the rest of the checks still run)
- Other: number of paths referenced more than once and number of zero-byte files

Lists of the individual files behind each section (for example every file missing an MD5) are written to separate tsv files.

## Inputs Table:
 This workflow is designed to use `Run workflow with inputs defined by file paths` option. You can use `Run workflow(s) with inputs defined by data table` option if you set up data table specifically to use this wdl.

| Input Name               | Description                                                                        | Type   | Required | Default                                                                                     |
|--------------------------|------------------------------------------------------------------------------------|--------|----------|---------------------------------------------------------------------------------------------|
| **billing_project**      | billing project                                                                    | String | Yes      | N/A                                                                                         |
| **workspace_name**       | workspace name.                                                                    | String | Yes      | N/A                                                                                         |
| **output_dir** | GCP directory (must start with `gs://`) the outputs are copied to. The validation report refers to the output files at this location. | String | Yes | N/A |
| **data_dictionary_file** | File that can be used to validate expected matches what is found. More info below. | File   | No       | N/A                                                                                         |
| **memory_gb**            | Memory for the task. Increase for very large workspaces/buckets.                   | Int    | No       | 8                                                                                           |
| **disk_size_gb**         | Disk size for the task.                                                            | Int    | No       | 50                                                                                          |
| **docker**               | Specifies a custom Docker image to use. Optional.                                  | String | No       | us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest |

# Data dictionary file

This optional file can be used to provide expected information on the columns in the workspace. This file should have the following headers:

#### Headers
- table_name
- column_name
- label
- multiple_values_allowed
- description
- data_type
- primary_key
- refers_to_column - column in another table that this column references
- allowed_values_pattern - list of acceptable values for column
- allowed_values_list - regex to match column values against
- required

You do not need to include every table/column pair from the Terra Workspace.

#### Data Types

Below are the possible data types that can be used in the data dictionary file and what they map to:

| Input DTs | TDR DTs  |
|-----------|----------|
| boolean   | boolean  |
| date      | date     |
| datetime  | datetime |
| float     | float64  |
| int       | int64    |
| string    | string   |
| fileref   | fileref  |

## Outputs Table:
All outputs are copied to `output_dir`, and the outputs below are the final `gs://` paths of the copied files.

| Output Name                 | Description                                                                                                       |
|-----------------------------|-------------------------------------------------------------------------------------------------------------------|
| **summary_statistics**      | Path to tsv with summary statistics for workspace.                                                                |
| **validation_report**       | Path to markdown report of the validation checks above.                                                           |
| **data_dictionary_file_path** | Path to the copy of the data dictionary file in `output_dir`. Empty if no data dictionary file was provided. |
| **validation_detail_files** | Paths to tsvs listing the individual files behind each report section (sections with nothing to list get no file). |
