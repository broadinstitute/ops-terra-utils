import logging
import sys
from argparse import ArgumentParser, Namespace

from utils.tdr_utils.renaming_util import GetRowAndFileInfoForReingest, BatchCopyAndIngest
from utils.tdr_utils.tdr_api_utils import TDR
from utils.tdr_utils.tdr_ingest_utils import GetPermissionsForWorkspaceIngest
from utils.token_util import Token
from utils.requests_utils.request_util import RunRequest
from utils.terra_utils.terra_util import TerraWorkspace
from utils import GCP, ARG_DEFAULTS

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

CLOUD_TYPE = GCP
# Different then usual because we want to merge the new files with the old ones
UPDATE_STRATEGY = 'merge'


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="Copy and Rename files to workspace or bucket and reingest with new name")
    parser.add_argument("-i", "--dataset_id", required=True)
    parser.add_argument(
        "-c",
        "--copy_and_ingest_batch_size",
        type=int,
        required=True,
        help="The number of rows to copy to temp location and then ingest at a time."
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        help="How wide you want the copy of files to on prem", required=True
    )
    parser.add_argument(
        "-o",
        "--original_file_basename_column",
        required=True,
        help="The basename column which you want to rename. ie 'sample_id'"
    )
    parser.add_argument(
        "-n",
        "--new_file_basename_column",
        required=True,
        help="The new basename column which you want the old one replace with. ie 'collab_sample_id'"
    )
    parser.add_argument(
        "-t",
        "--dataset_table_name",
        required=True,
        help="The name of the table in TDR"
    )
    parser.add_argument(
        "-ri",
        "--row_identifier",
        required=True,
        help="The unique identifier for the row in the table. ie 'sample_id'"
    )
    parser.add_argument(
        "-b",
        "--billing_project",
        required=False,
        help="The billing project to copy files to. Used if temp_bucket is not provided"
    )
    parser.add_argument(
        "-wn",
        "--workspace_name",
        required=False,
        help="The workspace to copy files to. Used if temp_bucket is not provided"
    )
    parser.add_argument(
        "-tb",
        "--temp_bucket",
        help="The bucket to copy files to for rename. Used if workspace_name is not provided"
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
    return parser.parse_args()


class GetTempBucket:
    def __init__(
            self,
            temp_bucket: str,
            billing_project: str,
            workspace_name: str,
            dataset_info: dict,
            request_util: RunRequest
    ):
        self.temp_bucket = temp_bucket
        self.billing_project = billing_project
        self.workspace_name = workspace_name
        self.dataset_info = dataset_info
        self.request_util = request_util

    def run(self) -> str:
        # Check if temp_bucket is provided
        if not self.temp_bucket:
            if not self.billing_project or not self.workspace_name:
                logging.error(
                    "If temp_bucket is not provided, billing_project and workspace_name must be provided")
                sys.exit(1)
            else:
                terra_workspace = TerraWorkspace(
                    billing_project=self.billing_project,
                    workspace_name=self.workspace_name,
                    request_util=self.request_util
                )
                temp_bucket = f'gs://{terra_workspace.get_workspace_bucket()}/'
                # Make sure workspace is set up for ingest
                GetPermissionsForWorkspaceIngest(
                    terra_workspace=terra_workspace,
                    dataset_info=self.dataset_info,
                    added_to_auth_domain=True
                ).run()
        else:
            if billing_project or workspace_name:
                logging.error(
                    "If temp_bucket is provided, billing_project and workspace_name must not be provided")
                sys.exit(1)
            logging.info(
                f"""Using temp_bucket: {self.temp_bucket}. Make sure {self.dataset_info['ingestServiceAccount']}
                 has read permission to bucket"""
            )
        return temp_bucket


if __name__ == '__main__':
    args = get_args()
    dataset_id = args.dataset_id
    max_retries = args.max_retries
    max_backoff_time = args.max_backoff_time
    original_file_basename_column = args.original_file_basename_column
    new_file_basename_column = args.new_file_basename_column
    dataset_table_name = args.dataset_table_name
    copy_and_ingest_batch_size = args.copy_and_ingest_batch_size
    row_identifier = args.row_identifier
    billing_project = args.billing_project
    workspace_name = args.workspace_name
    temp_bucket = args.temp_bucket
    workers = args.workers
    report_updates_only = args.report_updates_only

    # Initialize TDR classes
    token = Token(cloud=CLOUD_TYPE)
    request_util = RunRequest(
        token=token, max_retries=max_retries, max_backoff_time=max_backoff_time)
    tdr = TDR(request_util=request_util)

    # Get dataset info
    dataset_info = tdr.get_dataset_info(dataset_id=dataset_id)

    # Get temp bucket
    temp_bucket = GetTempBucket(
        temp_bucket=temp_bucket,
        billing_project=billing_project,
        workspace_name=workspace_name,
        dataset_info=dataset_info,
        request_util=request_util
    ).run()

    # Get schema info for table
    table_schema_info = tdr.get_table_schema_info(
        dataset_id=dataset_id, table_name=dataset_table_name)

    # Get all dict of all files where key is uuid
    files_info = tdr.create_file_dict(dataset_id=dataset_id, limit=ARG_DEFAULTS['batch_size_to_list_files'])

    # Get all metrics for table
    dataset_metrics = tdr.get_dataset_table_metrics(
        dataset_id=dataset_id, target_table_name=dataset_table_name)

    # Get information on files that need to be reingested
    rows_to_reingest, row_files_to_copy = GetRowAndFileInfoForReingest(
        table_schema_info=table_schema_info,
        files_info=files_info,
        table_metrics=dataset_metrics,
        row_identifier=row_identifier,
        original_column=original_file_basename_column,
        new_column=new_file_basename_column,
        temp_bucket=temp_bucket
    ).get_new_copy_and_ingest_list()

    if report_updates_only:
        logging.info("Reporting updates only. Exiting.")
        sys.exit(0)

    if rows_to_reingest:
        BatchCopyAndIngest(
            rows_to_ingest=rows_to_reingest,
            tdr=tdr,
            target_table_name=dataset_table_name,
            cloud_type=CLOUD_TYPE,
            update_strategy=UPDATE_STRATEGY,
            workers=workers,
            dataset_id=dataset_id,
            copy_and_ingest_batch_size=copy_and_ingest_batch_size,
            row_files_to_copy=row_files_to_copy
        ).run()
    else:
        logging.info("No files to re-ingest. Exiting.")
