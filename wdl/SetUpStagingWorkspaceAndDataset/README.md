# WDL Input Overview
This workflow automates the process of setting up a staging workspace in Terra and a staging dataset in TDR. The name of the dataset will be used to generate the workspace name.


## Inputs Table:
| Input Name                | Description                                                                        | Type    | Required | Default                                                                                     |
|---------------------------|------------------------------------------------------------------------------------|---------|----------|---------------------------------------------------------------------------------------------|
| **dataset_name**          | The name of the TDR dataset which will be created.                                 | String  | Yes      | N/A                                                                                         |
| **tdr_billing_profile**   | The billing profile of the TDR dataset that will be created.                       | String  | Yes      | N/A                                                                                         |
| **terra_billing_project** | The billing project to use for the target Terra workspace.                         | String  | Yes      | N/A                                                                                         |
| **controlled_access**     | Whether the Terra workspace/TDR dataset should have controlled access              | Boolean | Yes      | N/A                                                                                         |
| **phs_id**                | The PHS id if it exists. Optional.                                                 | String  | No       | N/A                                                                                         |
| **resource_owners**       | Comma separate list of resource owner(s). At least one resource owner is required. | String  | Yes      | N/A                                                                                         |
| **resource_members**      | Comma separate list of resource members (if they exist). Optional.                 | String  | No       | N/A                                                                                         |
| **continue_if_exists**    | Whether to continue even if the workspace/dataset already exists.                  | Boolean | Yes      | N/A                                                                                         |
| **current_user_email**    | The email of the current user (used for removing current user from workspace)      | String  | Yes      | N/A                                                                                         |
| **dbgap_consent_code**    | The dbGaP consent code (if it exists). Optional.                                   | String  | No       | N/A                                                                                         |
| **duos_identifier**       | The DUOS identifier (if it exists). Optional                                       | String  | No       | N/A                                                                                         |
| **wdls_to_import**        | A comma separate list of WDLs to import. Optional.                                 | String  | No       | N/A                                                                                         |
| **notebooks_to_import**   | A comma separate list of notebooks to import. Optional.                            | String  | No       | N/A                                                                                         |
| **is_anvil**              | Whether the workspace is meant for AnVIL use.                                      | Boolean | Yes      | N/A                                                                                         |
| **docker**                | The docker image                                                                   | String  | No       | us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest |

## Outputs Table:
This script does not generate any direct outputs. But the target workspace and dataset will be generated if the workflow completes successfully. The logs, which include any errors or warnings, can be reviewed in the stderr file for additional details about the process.
