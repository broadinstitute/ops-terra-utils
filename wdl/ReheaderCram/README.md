# WDL Input Overview

This WDL script re-headers a CRAM file using samtools. This can be used when a sample name has changed, and the CRAM
header needs to be updated to replace the old sample alias with the new sample alias.

## Inputs Table:
| Input Name          | Description                                                                                                                                                                      | Type   | Required | Default  |
|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|----------|----------|
| **input_cram**      | The path to the original CRAM file.                                                                                                                                              | File   | Yes      | N/A      |
| **old_sample**      | The OLD sample alias.                                                                                                                                                            | String | Yes      | N/A      |
| **new_sample**      | The NEW sample alias.                                                                                                                                                            | String | Yes      | N/A      |

## Outputs Table:
| Output Name                  | Description                                               | Type |
|------------------------------|-----------------------------------------------------------|------|
| **reheadered_cram_path**     | The path to the new re-headered cram                      | File |
| **reheadered_crai_path**     | The path to the new bai file associated with the new cram | File |
| **reheadered_cram_md5_path** | The path to the new md5 file associated with the new cram | File |
