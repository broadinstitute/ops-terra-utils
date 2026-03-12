# WDL Input Overview

This workflow takes in samples and their gvcf locations, copies the gvcfs to a new location with a new extension,
and outputs a new sample mapping file with the new gvcf paths.

This is meant to be run on a SET of samples (i.e., if you have 100 samples that require gvcfs renaming, you should only
be running one workflow). To achieve this, create a sample set in Terra with all the samples that require
gvcf renaming.

# Inputs Table:
This workflow is designed to use the `Run workflow(s) with inputs defined by data table` option. For example,
you'll need a `sample` table that contains (at the minimum) the sample names and gvcf paths. In this example, assume
the headers for these columns are `sample_name` and `gvcf_path`. You can also have other columns in this table, but
they won't be used in the workflow. Next, create a `sample_set` that contains a sample set name and all the samples
that require gvcf renaming. When configuring these workflow inputs, the `gvcf_file_paths` input will be defined as
`this.samples.gvcf_file_path` and the `sample_names` will be defined as `this.samples.sample_name`. The `sample_name_map`
is a string that will be used to name the output file that is generated.
