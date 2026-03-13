# WDL Input Overview

This workflow takes in samples and their gvcf locations, copies the gvcfs to a new location (right next to the
original input gvcf) with a new extension, and outputs a new sample mapping file with the new gvcf paths.
This workflow is for specifically copying gvcfs with the extension `.gvcf.gz` to instead have the `.g.vcf.gz` extension.
This is for gvcfs that are being used in older joint calling workflows that require the `.g.vcf.gz` extension.

This is meant to be run on a SET of samples (i.e., if you have 100 samples that require gvcfs renaming, you should only
be running one workflow). To achieve this, create a sample set in Terra with all the samples that require
gvcf renaming.

# Inputs Table:
This workflow is designed to use the `Run workflow(s) with inputs defined by data table` option. For example,
you'll need a `sample` table that contains (at the minimum) columns for the sample names and gvcf paths. In this
example, assume the headers for these columns are `sample_name` and `gvcf_path`. You can also have other columns in
this table, but they won't be used in the workflow. Next, create a `sample_set` that contains a sample set name and
all the samples that require gvcf renaming. Assuming you follow this naming convention, when configuring these workflow
inputs, the `gvcf_file_paths` input will be defined as `this.samples.gvcf_path` and the `sample_names` will be
defined as `this.samples.sample_name`. Take note that though the table containing the sample metadata is called
`sample`, the argument input is plural (`this.samples`). The `sample_name_map` is a string that will be used to name
the output file that is generated.

| Input Name          | Description                                                                                 | Type          | Required | Default                                                                                       |
|---------------------|---------------------------------------------------------------------------------------------|---------------|----------|-----------------------------------------------------------------------------------------------|
| **gvcf_file_paths** | An array of strings pointing to current gvcf locations in GCP.                              | Array[String] | Yes      | N/A                                                                                           |
| **sample_name_map** | The name of the output sample map file.                                                     | String        | Yes      | N/A                                                                                           |
| **sample_names**    | An array of strings of sample names. Sample names should correspond to the gvcf file paths. | Array[String] | Yes      | N/A                                                                                           |
| **docker**          | Docker file. Default is used if not provided.                                               | String        | No       | `us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest` |


# Outputs Table:
To have the new sample map written to the sample set table, you must define the output in the workflow outputs
section. This can be configured as `this.sample_map`, for example, which will write the output to a column named
`sample_map` in the sample set table. The value of this argument will be the location of the output sample map file.

| Output Name       | Description                                                                                                                                  | Type          |
|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| **sample_map**    | The location of the output sample map file. This file contains the sample names and their new gvcf paths. This file does not contain headers | String        |
