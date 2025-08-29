import logging
import sys
from argparse import ArgumentParser, Namespace

from ops_utils.tdr_utils.renaming_util import GetRowAndFileInfoForReingest, BatchCopyAndIngest
from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.tdr_utils.tdr_ingest_utils import GetPermissionsForWorkspaceIngest
from ops_utils.token_util import Token
from ops_utils.request_util import RunRequest
from ops_utils.terra_util import TerraWorkspace
from ops_utils.vars import ARG_DEFAULTS

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

UPDATE_STRATEGY = 'merge'
WORKERS = ARG_DEFAULTS["multithread_workers"]
COPY_AND_INGEST_BATCH_SIZE = 500


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="Rename one column in a dataset and reingest the files with the new column name")
    parser.add_argument("-i", "--dataset_id", required=True)
    parser.add_argument(
        "-c",
        "--copy_and_ingest_batch_size",
        type=int,
        required=False,
        default=500,
        help="The number of rows to copy to temp location and then ingest at a time. "
             f"Defaults to {COPY_AND_INGEST_BATCH_SIZE}"
    )
    parser.add_argument(
        "-cu",
        "--column_to_update",
        required=True,
        help="The tdr column to update which you want to values renamed. Also what it searches "
             "basename in files to update for. This cannot be the same as a tables primary key"
    )
    parser.add_argument(
        "-nc",
        "--column_with_new_value",
        required=True,
        help="The column value to update the column_to_update with. Also what it replaces the basename in files with"
    )
    parser.add_argument(
        "-t",
        "--table_name",
        required=True,
        help="The name of the table in TDR and Terra. Should be the same"
    )
    parser.add_argument(
        "-b",
        "--billing_project",
        required=False,
        help="The billing project to copy files to and where metadata with metrics + new file names will be stored"
    )
    parser.add_argument(
        "-wn",
        "--workspace_name",
        required=False,
        help="The billing project to copy files to and where metadata with metrics + new file names will be stored"
    )
    parser.add_argument(
        "--max_retries",
        required=False,
        default=ARG_DEFAULTS["max_retries"],
        help="The maximum number of retries for a failed request. " +
             f"Defaults to {ARG_DEFAULTS['max_retries']} if not provided"
    )
    parser.add_argument(
        "--max_backoff_time",
        required=False,
        default=ARG_DEFAULTS["max_backoff_time"],
        help="The maximum backoff time for a failed request (in seconds). " +
             f"Defaults to {ARG_DEFAULTS['max_backoff_time']} seconds if not provided"
    )
    parser.add_argument(
        "--report_updates_only",
        action="store_true",
        help="Use this option if you only want to report what updates would be made without actually making them"
    )
    parser.add_argument(
        "--update_columns_only",
        action="store_true",
        help="Use this option if you do not want to reingest files, only update the column values in the table"
    )
    parser.add_argument(
        "--service_account_json",
        "-saj",
        type=str,
        help="Path to the service account JSON file. If not provided, will use the default credentials."
    )
    return parser.parse_args()


class GetDataSetInfo:
    def __init__(self, tdr: TDR, dataset_id: str, table_name: str):
        self.tdr = tdr
        self.dataset_id = dataset_id
        self.table_name = table_name

    def run(self) -> dict:
        dataset_info = tdr.get_dataset_info(dataset_id=self.dataset_id).json()
        tdr_table_info = tdr.get_table_schema_info(
            dataset_id=self.dataset_id,
            table_name=self.table_name,
            dataset_info=dataset_info
        )
        primary_keys = tdr_table_info["primaryKey"]
        if not primary_keys:
            logging.error(f"Primary key not found for table {self.table_name}. Needs to exist for updates")
            sys.exit(1)
        # Only support one primary key for now
        primary_key = primary_keys[0]
        tdr_table_metrics = tdr.get_dataset_table_metrics(dataset_id=self.dataset_id, target_table_name=self.table_name)
        files_info = tdr.create_file_dict(dataset_id=self.dataset_id, limit=ARG_DEFAULTS['batch_size_to_list_files'])
        return {
            "primary_key": primary_key,
            "tdr_table_metrics": tdr_table_metrics,
            "files_info": files_info,
            "dataset_info": dataset_info,
            "tdr_table_info": tdr_table_info
        }


class GetWorkspaceInfo:
    def __init__(self, terra: TerraWorkspace, table_name: str, dataset_info: dict):
        self.terra = terra
        self.table_name = table_name
        self.dataset_info = dataset_info

    def run(self) -> tuple[list[dict], str]:
        terra_table_metrics = self.terra.get_gcp_workspace_metrics(entity_type=self.table_name)
        terra_bucket = f"gs://{self.terra.get_workspace_bucket()}/"
        # Make sure dataset SA has permissions to bucket
        GetPermissionsForWorkspaceIngest(
            terra_workspace=self.terra,
            dataset_info=self.dataset_info,
            added_to_auth_domain=True
        ).run()
        return terra_table_metrics, terra_bucket


class GetMatchingRows:
    def __init__(
            self,
            terra_metrics: list[dict],
            tdr_table_metrics: list[dict],
            primary_key: str,
            column_with_new_value: str,
            column_to_update: str
    ):
        self.terra_metrics = terra_metrics
        self.tdr_table_metrics = tdr_table_metrics
        self.primary_key = primary_key
        self.column_with_new_value = column_with_new_value
        self.column_to_update = column_to_update

    def _create_terra_new_value_dict(self) -> dict:
        terra_new_value_dict = {}
        terra_primary_key = f"tdr:{self.primary_key}"
        tdr_primary_key_values = [row[self.primary_key] for row in self.tdr_table_metrics]
        for row in self.terra_metrics:
            row_metrics = row['attributes']
            if row_metrics[terra_primary_key] in tdr_primary_key_values:
                terra_new_value_dict[row_metrics[terra_primary_key]] = row_metrics[self.column_with_new_value]
            else:
                logging.warning(
                    f"Expected {terra_primary_key} {row_metrics[terra_primary_key]} not found in TDR metrics")
        return terra_new_value_dict

    def run(self) -> list[dict]:
        matching_rows = []
        terra_new_value_dict = self._create_terra_new_value_dict()
        # Loop through tdr table metrics
        for row in self.tdr_table_metrics:
            new_value = terra_new_value_dict.get(row[self.primary_key])
            # If new value is found and it is different from the current value
            if new_value and new_value != row[self.column_to_update]:
                # Add new value to tdr row
                row[self.column_with_new_value] = terra_new_value_dict[row[self.primary_key]]
                matching_rows.append(row)
        return matching_rows


if __name__ == '__main__':
    args = get_args()
    dataset_id = args.dataset_id
    copy_and_ingest_batch_size = args.copy_and_ingest_batch_size
    column_to_update = args.column_to_update
    column_with_new_value = args.column_with_new_value
    table_name = args.table_name
    billing_project = args.billing_project
    workspace_name = args.workspace_name
    max_retries = args.max_retries
    max_backoff_time = args.max_backoff_time
    report_updates_only = args.report_updates_only
    update_columns_only = args.update_columns_only
    service_account_json = args.service_account_json

    # Initialize TDR classes
    token = Token(service_account_json=service_account_json)
    request_util = RunRequest(
        token=token, max_retries=max_retries, max_backoff_time=max_backoff_time)
    tdr = TDR(request_util=request_util)
    terra = TerraWorkspace(
        request_util=request_util,
        billing_project=billing_project,
        workspace_name=workspace_name
    )

    dataset_dict = GetDataSetInfo(
        tdr=tdr,
        dataset_id=dataset_id,
        table_name=table_name
    ).run()
    # Get all the info out of dict
    primary_key = dataset_dict["primary_key"]
    tdr_table_metrics = dataset_dict["tdr_table_metrics"]
    files_info = dataset_dict["files_info"]
    dataset_info = dataset_dict["dataset_info"]
    tdr_table_info = dataset_dict["tdr_table_info"]

    if primary_key == column_to_update:
        logging.error(f"Primary key {primary_key} cannot be the same as the column to update {column_to_update}")
        sys.exit(1)

    terra_metrics, workspace_bucket = GetWorkspaceInfo(
        terra=terra,
        table_name=table_name,
        dataset_info=dataset_info
    ).run()

    tdr_rows_to_update = GetMatchingRows(
        terra_metrics=terra_metrics,
        tdr_table_metrics=tdr_table_metrics,
        primary_key=primary_key,
        column_with_new_value=column_with_new_value,
        column_to_update=column_to_update
    ).run()

    rows_to_reingest, files_to_copy_to_temp = GetRowAndFileInfoForReingest(
        table_schema_info=tdr_table_info,
        files_info=files_info,
        table_metrics=tdr_rows_to_update,
        row_identifier=primary_key,
        temp_bucket=workspace_bucket,
        update_original_column=column_to_update,
        column_update_only=update_columns_only,
        original_column=column_to_update,
        new_column=column_with_new_value
    ).get_new_copy_and_ingest_list()

    rows_not_updated = len(terra_metrics) - len(rows_to_reingest)
    if rows_not_updated:
        logging.info(f"{rows_not_updated} rows from Terra will not be updated in TDR")
    # Updates are logged in GetRowAndFileInfoForReingest
    if not report_updates_only:
        BatchCopyAndIngest(
            rows_to_ingest=rows_to_reingest,
            tdr=tdr,
            target_table_name=table_name,
            update_strategy=UPDATE_STRATEGY,
            workers=WORKERS,
            dataset_id=dataset_id,
            copy_and_ingest_batch_size=copy_and_ingest_batch_size,
            row_files_to_copy=files_to_copy_to_temp,
            wait_time_to_poll=ARG_DEFAULTS["waiting_time_to_poll"]
        ).run()
