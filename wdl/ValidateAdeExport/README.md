# WDL Input Overview

This WDL validates that exporting a TDR snapshot through the AnVIL Data Explorer (ADE) into a Terra workspace is
faithful. It exports the snapshot to a new workspace the same way a user does (Azul PFB manifest, then a Terra
import), and then compares the workspace to the snapshot:

- **Tables**:
  - Every non-empty snapshot table must exist in the workspace with the same name and the same row count.
  - The workspace must not have tables that are not in the snapshot.
  - A workspace table must have at least as many columns as the snapshot table has non-empty columns (Terra drops
    empty columns on import, and may add columns).
  - The TDR system table `datarepo_row_ids`, the `duos_dataset_registration` table added by the import, and empty
    snapshot tables are not compared.
- **Authorization domain**: the workspace's authorization domain must match the snapshot's.

Before anything is created, the workflow also checks that the Azul token is valid and has at least 30 minutes left,
that Azul is not (re)indexing (a manifest requested during indexing can be empty), and that the snapshot is open
access unless `allow_controlled_access` is set.

Each validation that fails is added to a list that is written to one output file. If every validation passes the
validation workspace is deleted. If any validation fails, or `keep_workspace` is set, the workspace is kept so it can
be investigated. Note that a workspace is created for every run, and importing a very large snapshot can take hours
(Terra cancels imports that take much longer than 6 hours).

## Inputs Table:
 This workflow is designed to use `Run workflow with inputs defined by file paths` option. You can use `Run workflow(s) with inputs defined by data table` option if you set up data table specifically to use this wdl.

| Input Name                  | Description                                                                                                                                                            | Type    | Required | Default                                                                                     |
|-----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|----------|---------------------------------------------------------------------------------------------|
| **snapshot_id**             | TDR snapshot to export and validate.                                                                                                                                   | String  | Yes      | N/A                                                                                         |
| **billing_project**         | Terra billing project the validation workspace is created in.                                                                                                          | String  | Yes      | N/A                                                                                         |
| **azul_token**              | File containing only an Azul access token. Azul does not accept regular gcloud tokens, so copy the token from the Azul Swagger UI (it must have at least 30 minutes left). | File    | Yes      | N/A                                                                                         |
| **allow_controlled_access** | Allow snapshots that are not open access (consent code `NRES`). Controlled access data requires approval.                                                              | Boolean | No       | false                                                                                       |
| **keep_workspace**          | Keep the validation workspace even if all validations pass. It is always kept if a validation fails.                                                                   | Boolean | No       | false                                                                                       |
| **docker**                  | Specifies a custom Docker image to use. Optional.                                                                                                                      | String  | No       | us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest |

This workflow only runs against prod.

## Outputs Table:

| Output Name              | Description                                                                                                                       |
|--------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| **validation_passed**    | `true` if every validation passed (the failures file is empty).                                                                   |
| **validation_failures**  | Text file with one line for each validation that failed. It is empty if all validations passed.                                   |
| **kept_workspace_name**  | Name of the validation workspace (in `billing_project`) if it was kept. Empty if the workspace was deleted.                      |
