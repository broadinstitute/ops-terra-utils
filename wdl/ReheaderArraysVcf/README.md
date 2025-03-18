# WDL Input Overview

This WDL script re-headers a VCF file using picard tools. This can be used when a sample name has changed, and the VCF
header needs to be updated to replace the old sample alias with the new sample alias. Note that this can only be
used on single-sample VCF (your workflow will fail if provided with a multi-sample vcf, for example the output of
joint calling). The original sample alias doesn't need to be provided. It will be extracted automatically from the
VCF input.

## Inputs Table:
| Input Name               | Description                        | Type   | Required | Default  |
|--------------------------|------------------------------------|--------|----------|----------|
| **input_vcf**            | The path to the original VCF file. | File   | Yes      | N/A      |
| **new_sample_alias**     | The NEW sample alias.              | String | Yes      | N/A      |
| **chipwell_barcode**     | The sample's chipwell barcode.     | String | Yes      | N/A      |

## Outputs Table:
| Output Name                 | Description                                                | Type |
|-----------------------------|------------------------------------------------------------|------|
| **reheadered_vcf**          | The path to the new re-headered vcf                        | File |
| **reheadered_vcf_index**    | The path to the new index file associated with the new vcf | File |
