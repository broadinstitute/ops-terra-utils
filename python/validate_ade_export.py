"""
Validate that exporting a TDR snapshot through the AnVIL Data Explorer (ADE) into a Terra workspace is faithful.

The snapshot is exported to a new workspace with the same steps a user takes in the ADE (Azul PFB manifest ->
Terra import). The workspace is then compared to the snapshot:
  - Tables: every non-empty snapshot table must exist in the workspace with the same row count, the workspace
    must not have extra tables, and a workspace table must have at least as many columns as the snapshot table has
    non-empty columns.
  - Authorization domain: the workspace's auth domain must match the snapshot's.

Every failed validation is added to a list that is written to validation_failures.txt (empty if all passed). The
workspace is deleted when everything passes, and kept for investigation otherwise (or if --keep_workspace is set).

To run locally do python3 /path/to/validate_ade_export.py --snapshot_id <id> --billing_project <project>
--azul_token_file <file>. The Azul token file must contain only the token, copied from the Azul Swagger UI.
"""
import json
import logging
import re
import time
from argparse import ArgumentParser, Namespace
from datetime import datetime

from ops_utils.azul_utils import AzulClient
from ops_utils.bq_utils import BigQueryUtil
from ops_utils.request_util import GET, POST, RunRequest
from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.terra_util import SAM_LINK, TerraWorkspace
from ops_utils.token_util import Token
from ops_utils.vars import APPLICATION_JSON

FAILURES_FILE = "validation_failures.txt"
WORKSPACE_NAME_FILE = "workspace_name.txt"
OPEN_ACCESS_CONSENT_CODE = "NRES"
# TDR-only table that is never exported to the workspace
TDR_SYSTEM_TABLES = {"datarepo_row_ids"}
# Table the import adds to the workspace that has no counterpart in the snapshot
IMPORT_ADDED_TABLES = {"duos_dataset_registration"}
# Terra cancels imports that run for much longer than 6 hours
IMPORT_TIMEOUT_SECONDS = 6 * 60 * 60
IMPORT_POLL_SECONDS = 30
AZUL_TOKEN_MIN_SECONDS = 30 * 60

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(description="Validate a TDR snapshot exported through the AnVIL Data Explorer")
    parser.add_argument("--snapshot_id", required=True, help="TDR snapshot to export and validate")
    parser.add_argument("--billing_project", required=True,
                        help="Terra billing project the validation workspace is created in")
    parser.add_argument("--azul_token_file", required=True, help="File containing an Azul access token")
    parser.add_argument("--allow_controlled_access", action="store_true",
                        help="Allow snapshots that are not open access (consent code NRES)")
    parser.add_argument("--keep_workspace", action="store_true",
                        help="Keep the validation workspace even if all validations pass")
    return parser.parse_args()


def get_snapshot_tables(bq: BigQueryUtil, project: str, dataset: str) -> dict[str, dict]:
    """Get the row count and number of non-empty columns of every table in the snapshot's BigQuery dataset."""
    columns = bq.query_table(
        f"SELECT table_name, column_name, data_type FROM `{project}.{dataset}`.INFORMATION_SCHEMA.COLUMNS"
    )
    columns_by_table: dict[str, list[dict]] = {}
    for column in columns:
        columns_by_table.setdefault(column["table_name"], []).append(column)

    tables = {}
    for table, table_columns in columns_by_table.items():
        # A value is non-empty if it is not null, or for arrays if it has at least one element
        counts = ", ".join(
            f"COUNTIF(ARRAY_LENGTH(`{c['column_name']}`) > 0) AS c{i}" if c["data_type"].startswith("ARRAY")
            else f"COUNTIF(`{c['column_name']}` IS NOT NULL) AS c{i}"
            for i, c in enumerate(table_columns)
        )
        row = bq.query_table(f"SELECT COUNT(*) AS row_count, {counts} FROM `{project}.{dataset}.{table}`")[0]
        tables[table] = {
            "rows": row["row_count"],
            "columns": sum(1 for i in range(len(table_columns)) if row[f"c{i}"] > 0),
        }
    return tables


def compare_tables(snapshot_tables: dict[str, dict], workspace_tables: dict[str, dict]) -> list[str]:
    """
    Compare the tables of the snapshot and the workspace. Each table is `{"rows": int, "columns": int}`.

    Empty snapshot tables are not expected in the workspace (Terra does not import them), and so are the
    snapshot's non-empty columns, which is why the workspace may not have fewer columns than the snapshot.
    """
    ignored = TDR_SYSTEM_TABLES | IMPORT_ADDED_TABLES
    snapshot = {n: t for n, t in snapshot_tables.items() if n not in ignored and t["rows"] > 0}
    workspace = {n: t for n, t in workspace_tables.items() if n not in ignored}

    failures = []
    for name in sorted(snapshot.keys() | workspace.keys()):
        if name not in workspace:
            failures.append(f"Table {name} is in the snapshot ({snapshot[name]['rows']} rows) but not the workspace")
        elif name not in snapshot:
            failures.append(f"Table {name} is in the workspace but not in the snapshot (or it is empty there)")
        else:
            if snapshot[name]["rows"] != workspace[name]["rows"]:
                failures.append(
                    f"Table {name} has {snapshot[name]['rows']} rows in the snapshot "
                    f"but {workspace[name]['rows']} in the workspace"
                )
            if workspace[name]["columns"] < snapshot[name]["columns"]:
                failures.append(
                    f"Table {name} has {snapshot[name]['columns']} non-empty columns in the snapshot "
                    f"but only {workspace[name]['columns']} in the workspace"
                )
    return failures


def import_pfb(terra: TerraWorkspace, request_util: RunRequest, signed_url_to_import: str) -> None:
    """Import a PFB into the workspace and wait for the import to finish. Raises if it fails or times out."""
    import_url = f"{terra.terra_link}/workspaces/{terra.billing_project}/{terra.workspace_name}/importJob"
    body = {"url": signed_url_to_import, "filetype": "pfb", "options": None}
    job_id = request_util.run_request(
        uri=import_url, method=POST, content_type=APPLICATION_JSON, data=json.dumps(body)
    ).json()["jobId"]
    logging.info(f"Import job {job_id} submitted")

    deadline = time.monotonic() + IMPORT_TIMEOUT_SECONDS
    while True:
        job = request_util.run_request(uri=f"{import_url}/{job_id}", method=GET).json()
        logging.info(f"Import job {job_id} status: {job.get('status')}")
        if job.get("status") == "Done":
            return
        if job.get("status") == "Error":
            raise RuntimeError(f"Import job {job_id} failed: {job}")
        if time.monotonic() > deadline:
            raise TimeoutError(f"Import job {job_id} did not finish within {IMPORT_TIMEOUT_SECONDS} seconds")
        time.sleep(IMPORT_POLL_SECONDS)


if __name__ == '__main__':
    args = get_args()
    failures: list[str] = []
    workspace_to_clean_up = None

    try:
        request_util = RunRequest(token=Token())
        azul_token = Token(token_file=args.azul_token_file)
        azul = AzulClient(request_util=RunRequest(token=azul_token))
        tdr = TDR(request_util=request_util)
        bq = BigQueryUtil()

        # Fail before anything is created if the token will not work or expires during the run
        azul.validate_token(azul_token.get_token(), min_remaining_seconds=AZUL_TOKEN_MIN_SECONDS)
        azul.check_not_reindexing()

        snapshot = tdr.get_snapshot_info(  # type: ignore[union-attr]
            snapshot_id=args.snapshot_id, info_to_include=["ACCESS_INFORMATION"]
        ).json()
        consent_code = snapshot.get("consentCode")
        if consent_code != OPEN_ACCESS_CONSENT_CODE and not args.allow_controlled_access:
            raise RuntimeError(
                f"Snapshot has consent code {consent_code!r}, not {OPEN_ACCESS_CONSENT_CODE!r} (open access). "
                "Set allow_controlled_access to validate it"
            )

        bq_info = snapshot["accessInformation"]["bigQuery"]
        bq_project, bq_dataset = bq_info["projectId"], bq_info["datasetName"]
        snapshot_tables = get_snapshot_tables(bq, bq_project, bq_dataset)
        dataset_ids = bq.query_table(f"SELECT dataset_id FROM `{bq_project}.{bq_dataset}.anvil_dataset`")
        if len(dataset_ids) != 1:
            raise RuntimeError(f"Expected one row in anvil_dataset, found {len(dataset_ids)}")
        signed_url = azul.request_pfb_manifest(dataset_ids[0]["dataset_id"])

        snapshot_name = re.sub(r"[^A-Za-z0-9_-]+", "_", snapshot["name"]).strip("_")
        workspace = TerraWorkspace(
            billing_project=args.billing_project,
            workspace_name=f"qc_ade_{snapshot_name}_{datetime.now().strftime('%Y%m%d%H%M')}",
            request_util=request_util
        )
        workspace.create_workspace(attributes={"description": f"ADE export validation of {args.snapshot_id}"})
        workspace_to_clean_up = workspace
        import_pfb(workspace, request_util, signed_url.reveal())

        workspace_tables = {
            name: {"rows": info["count"], "columns": len(info["attributeNames"])}
            for name, info in workspace.get_workspace_entity_info(use_cache=False).json().items()
        }
        failures.extend(compare_tables(snapshot_tables, workspace_tables))

        expected_auth_domain = request_util.run_request(
            uri=f"{SAM_LINK}/resources/v2/datasnapshot/{args.snapshot_id}/authDomain", method=GET
        ).json()
        actual_auth_domain = [
            group["membersGroupName"]
            for group in workspace.get_workspace_info().json()["workspace"]["authorizationDomain"]
        ]
        if set(expected_auth_domain) != set(actual_auth_domain):
            failures.append(
                f"Auth domain of the workspace {sorted(actual_auth_domain)} does not match "
                f"the snapshot's {sorted(expected_auth_domain)}"
            )
    except Exception as e:
        failures.append(f"Validation stopped early: {e!r}")

    kept_workspace_name = ""
    if workspace_to_clean_up:
        if failures or args.keep_workspace:
            kept_workspace_name = workspace_to_clean_up.workspace_name
            logging.info(f"Keeping workspace {args.billing_project}/{kept_workspace_name}")
        else:
            try:
                workspace_to_clean_up.delete_workspace()
                logging.info("All validations passed. Deleted the validation workspace")
            except Exception as e:
                failures.append(f"Could not delete workspace {workspace_to_clean_up.workspace_name}: {e!r}")
                kept_workspace_name = workspace_to_clean_up.workspace_name

    with open(FAILURES_FILE, "w") as f:
        f.write("".join(f"{failure}\n" for failure in failures))
    with open(WORKSPACE_NAME_FILE, "w") as f:
        f.write(kept_workspace_name)

    if failures:
        logging.warning(f"{len(failures)} validation failure(s):\n" + "\n".join(failures))
    else:
        logging.info("All validations passed")
