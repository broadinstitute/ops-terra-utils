import json
import logging
import sys
import os
from argparse import ArgumentParser, Namespace
from typing import Optional, Tuple
from utils.tdr_util import TDR, BatchIngest, GetPermissionsForWorkspaceIngest
from utils.token_util import Token
from utils.request_util import RunRequest
from utils.terra_util import TerraWorkspace
from utils.gcp_utils import GCPCloudFunctions, COPY
from utils import GCP

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

CLOUD_TYPE = GCP
MAX_RETRIES = 5
MAX_BACKOFF_TIME = 5 * 60
BATCH_SIZE_TO_LIST_FILES = 25000
INGEST_BATCH_SIZE = 500
WAITING_TIME_TO_POLL = 30


def get_args() -> Namespace:
    parser = ArgumentParser(description="Copy and Rename files to workspace or bucket and reingest with new name")
    parser.add_argument("-i", "--dataset_id", required=True)
    parser.add_argument("-w", "--workers", type=int, help="How wide you want the copy of files to on prem", required=True)
    parser.add_argument("-o", "--original_file_basename_column", required=True, help=f"The basename column which you want to rename. ie 'sample_id'")
    parser.add_argument("-n", "--new_file_basename_column", required=True, help=f"The new basename column which you want the old one replace with. ie 'collab_sample_id'")
    parser.add_argument("-t", "--dataset_table_name", required=True, help="The name of the table in TDR")
    parser.add_argument("-ri", "--row_identifier", required=True, help="The unique identifier for the row in the table. ie 'sample_id'")
    parser.add_argument("-b", "--billing_project", required=False, help="The billing project to copy files to. Used if temp_bucket is not provided")
    parser.add_argument("-wn", "--workspace_name", required=False, help="The workspace to copy files to. Used if temp_bucket is not provided")
    parser.add_argument("-tb", "--temp_bucket", help="The bucket to copy files to for rename. Used if workspace_name is not provided")
    parser.add_argument("--max_retries", required=False, default=MAX_RETRIES,
                        help=f"The maximum number of retries for a failed request. Defaults to {MAX_RETRIES} if not provided")
    parser.add_argument(
        "--max_backoff_time",
        required=False,
        default=MAX_BACKOFF_TIME,
        help=f"The maximum backoff time for a failed request (in seconds). Defaults to 300 seconds if not provided"
    )
    parser.add_argument(
        "--batch_size",
        required=False,
        default=INGEST_BATCH_SIZE,
        help=f"The number of files to ingest at a time. Defaults to {INGEST_BATCH_SIZE} if not provided")
    return parser.parse_args()


class GetRowAndFileInfoForReingest:
    def __init__(self, table_schema_info: dict, files_info: dict, table_metrics: list[dict], og_file_basename_column: str,
                 new_file_basename_column: str, row_identifier: str, temp_bucket: str):
        self.table_schema_info = table_schema_info
        self.files_info = files_info
        self.table_metrics = table_metrics
        self.og_file_basename_column = og_file_basename_column
        self.new_file_basename_column = new_file_basename_column
        self.row_identifier = row_identifier
        self.total_files_to_reingest = 0
        self.rows_to_reingest = []
        self.temp_bucket = temp_bucket

    def create_row_dict(self, row_dict: dict, file_ref_columns: list[str]) -> Tuple[Optional[list[dict]], Optional[list[dict]]]:
        """Go through each row and check each cell if it is a file and if it needs to be reingested.
        If so, create a new row dict with the new file path."""
        reingest_row = False
        # Create new dictionary for ingest just the row identifier so can merge with right row later
        new_row_dict = {self.row_identifier: row_dict[self.row_identifier]}
        # Create list of all files for copy to temp location
        temp_copy_list = []
        # Get basename to replace
        og_basename = row_dict[self.og_file_basename_column]
        new_basename = row_dict[self.new_file_basename_column]
        for column_name in row_dict:
            # Check if column is a fileref
            if column_name in file_ref_columns:
                # Get full file info for that cell
                file_info = self.files_info.get(row_dict[column_name])
                # real file path
                access_url = file_info['fileDetail']['accessUrl']
                # path used in metadata, not real file path
                tdr_file_path = file_info['path']
                # Updated file name
                new_file_name = os.path.basename(access_url).replace(og_basename, new_basename)
                temp_path = os.path.join(self.temp_bucket, new_file_name)
                updated_tdr_metadata_path = os.path.join(os.path.dirname(tdr_file_path), new_file_name)
                # Check if access_url starts with og basename and then .
                if os.path.basename(access_url).startswith(f"{og_basename}."):
                    self.total_files_to_reingest += 1
                    # Add to ingest row dict
                    new_row_dict[column_name] = {
                        "sourcePath": temp_path,
                        # Create new target path with updated basename
                        "targetPath": updated_tdr_metadata_path,
                    }
                    # Add to copy list to temp location from access url
                    temp_copy_list.append(
                        {
                            "source_file": access_url,
                            "full_destination_path": temp_path
                        }
                    )
                    # Set reingest row to True because files need to be updated
                    reingest_row = True
        if reingest_row:
            return new_row_dict, temp_copy_list
        else:
            return None, None

    def get_new_copy_and_ingest_list(self) -> Tuple[list[dict], list[dict]]:
        rows_to_reingest = []
        files_to_copy_to_temp = []
        # Get all columns in table that are filerefs
        file_ref_columns = [col['name'] for col in self.table_schema_info['columns'] if col['datatype'] == 'fileref']
        for row_dict in self.table_metrics:
            new_row_dict, temp_copy_list = self.create_row_dict(row_dict, file_ref_columns)
            # If there is something to copy and update
            if new_row_dict and temp_copy_list:
                rows_to_reingest.append(new_row_dict)
                files_to_copy_to_temp.extend(temp_copy_list)
        logging.info(f"Total rows to re-ingest: {len(self.rows_to_reingest)}")
        logging.info(f"Total files to copy and re-ingest: {self.total_files_to_reingest}")
        return rows_to_reingest, files_to_copy_to_temp


def get_temp_bucket(temp_bucket: str, billing_project: str, workspace_name: str, dataset_info: dict) -> str:
    # Check if temp_bucket is provided
    if not temp_bucket:
        if not billing_project or not workspace_name:
            logging.error("If temp_bucket is not provided, billing_project and workspace_name must be provided")
            sys.exit(1)
        else:
            terra_workspace = TerraWorkspace(billing_project=billing_project, workspace_name=workspace_name,
                                             request_util=request_util)
            temp_bucket = f'gs://{terra_workspace.get_workspace_bucket()}/'
            # Make sure workspace is set up for ingest
            GetPermissionsForWorkspaceIngest(
                terra_workspace=terra_workspace,
                dataset_info=dataset_info,
                added_to_auth_domain=True
            ).run()
    else:
        if billing_project or workspace_name:
            logging.error("If temp_bucket is provided, billing_project and workspace_name must not be provided")
            sys.exit(1)
        logging.info(
            f"Using temp_bucket: {temp_bucket}. Make sure {dataset_info['ingestServiceAccount']} has read permission to bucket")
    return temp_bucket

if __name__ == '__main__':
    args = get_args()
    dataset_id = args.dataset_id
    max_retries = args.max_retries
    max_backoff_time = args.max_backoff_time
    original_file_basename_column = args.original_file_basename_column
    new_file_basename_column = args.new_file_basename_column
    dataset_table_name = args.dataset_table_name
    batch_size = args.batch_size
    row_identifier = args.row_identifier
    billing_project = args.billing_project
    workspace_name = args.workspace_name
    temp_bucket = args.temp_bucket
    workers = args.workers

    # Initialize TDR classes
    token = Token(cloud=CLOUD_TYPE)
    request_util = RunRequest(token=token, max_retries=max_retries, max_backoff_time=max_backoff_time)
    tdr = TDR(request_util=request_util)

    # Get dataset info
    dataset_info = tdr.get_dataset_info(dataset_id=dataset_id)

    # Get temp bucket
    temp_bucket = get_temp_bucket(temp_bucket, billing_project, workspace_name, dataset_info)

    # Get schema info for table
    table_schema_info = tdr.get_table_schema_info(dataset_id=dataset_id, table_name=dataset_table_name)

    # Get all dict of all files where key is uuid
    files_info = tdr.create_file_dict(dataset_id=dataset_id, limit=1000)

    # Get all metrics for table
    dataset_metrics = tdr.get_data_set_table_metrics(dataset_id=dataset_id, target_table_name=dataset_table_name)

    # Get information on files that need to be reingested
    rows_to_reingest, files_to_copy = GetRowAndFileInfoForReingest(
        table_schema_info=table_schema_info,
        files_info=files_info,
        table_metrics=dataset_metrics,
        row_identifier=row_identifier,
        og_file_basename_column=original_file_basename_column,
        new_file_basename_column=new_file_basename_column,
        temp_bucket=temp_bucket
    ).get_new_copy_and_ingest_list()

    # Copy files to temp bucket
    GCPCloudFunctions().move_or_copy_multiple_files(
        # Create dict with new names for copy of files to temp bucket
        files_to_move=files_to_copy,
        action=COPY,
        workers=workers,
        max_retries=5
    )

    # Batch ingest new rows from temp bucket
    BatchIngest(
        ingest_metadata=rows_to_reingest,
        tdr=tdr,
        target_table_name=dataset_table_name,
        dataset_id=dataset_id,
        batch_size=batch_size,
        cloud_type=CLOUD_TYPE,
        update_strategy='merge',
        bulk_mode=False,
        file_list_bool=False,
        skip_reformat=True,
        waiting_time_to_poll=WAITING_TIME_TO_POLL
    ).run()










