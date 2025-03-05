# WDL Input Overview

This WDL script re-headers a BAM file using samtools. This can be used when a sample name has changed, and the BAM
header needs to be updated to replace the old sample alias with the new sample alias.

## Inputs Table:
| Input Name           | Description                                                                                                                                                                      | Type   | Required | Default  |
|----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|----------|----------|
| **input_bam**        | The path to the original BAM file.                                                                                                                                               | File   | Yes      | N/A      |
| **old_sample**       | The OLD sample alias.                                                                                                                                                            | String | Yes      | N/A      |
| **new_sample**       | The NEW sample alias.                                                                                                                                                            | String | Yes      | N/A      |

## Outputs Table:
| Output Name                 | Description                                              | Type |
|-----------------------------|----------------------------------------------------------|------|
| **reheadered_bam_path**     | The path to the new re-headered bam                      | File |
| **reheadered_bai_path**     | The path to the new bai file associated with the new bam | File |
| **reheadered_bam_md5_path** | The path to the new md5 file associated with the new bam | File |
