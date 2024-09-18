import logging
import sys
import os
from argparse import ArgumentParser, Namespace
from typing import Optional, Tuple

from utils.tdr_util import TDR, StartAndMonitorIngest, GetPermissionsForWorkspaceIngest
from utils.token_util import Token
from utils.request_util import RunRequest
from utils.terra_util import TerraWorkspace
from utils.gcp_utils import GCPCloudFunctions
from utils import GCP

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

CLOUD_TYPE = GCP
MAX_RETRIES = 5
MAX_BACKOFF_TIME = 5 * 60
WAITING_TIME_TO_POLL = 30
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
        default=MAX_RETRIES,
        help=f"The maximum number of retries for a failed request. Defaults to {MAX_RETRIES} if not provided"
    )
    parser.add_argument(
        "--max_backoff_time",
        required=False,
        default=MAX_BACKOFF_TIME,
        help="The maximum backoff time for a failed request (in seconds). Defaults to 300 seconds if not provided"
    )
    return parser.parse_args()


class GetRowAndFileInfoForReingest:
    def __init__(
            self,
            table_schema_info: dict,
            files_info: dict,
            table_metrics: list[dict],
            og_file_basename_column: str,
            new_file_basename_column: str,
            row_identifier: str,
            temp_bucket: str
    ):
        self.table_schema_info = table_schema_info
        self.files_info = files_info
        self.table_metrics = table_metrics
        self.og_file_basename_column = og_file_basename_column
        self.new_file_basename_column = new_file_basename_column
        self.row_identifier = row_identifier
        self.total_files_to_reingest = 0
        self.rows_to_reingest: list = []
        self.temp_bucket = temp_bucket

    def _create_paths(self, file_info: dict, og_basename: str, new_basename: str) -> Tuple[str, str, str]:
        # Access url is the full path to the file in TDR
        access_url = file_info["fileDetail"]["accessUrl"]
        # Get basename of file
        file_name = os.path.basename(access_url)
        # Replace basename with new basename
        new_file_name = file_name.replace(
            f'{og_basename}.', f'{new_basename}.')
        # get tdr path. Not real path, just the metadata
        tdr_file_path = file_info["path"]
        # Create full path to updated tdr metadata file path
        updated_tdr_metadata_path = os.path.join(
            os.path.dirname(tdr_file_path), new_file_name)
        access_url_without_bucket = access_url.split('gs://')[1]
        temp_path = os.path.join(self.temp_bucket, os.path.dirname(
            access_url_without_bucket), new_file_name)
        return temp_path, updated_tdr_metadata_path, access_url

    def _create_row_dict(
            self, row_dict: dict, file_ref_columns: list[str]
    ) -> Tuple[Optional[dict], Optional[list[dict]]]:
        """Go through each row and check each cell if it is a file and if it needs to be re-ingested.
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
                # Get potential temp path, updated tdr metadata path, and access url for file
                temp_path, updated_tdr_metadata_path, access_url = self._create_paths(
                    file_info, og_basename, new_basename  # type: ignore[arg-type]
                )
                # Check if access_url starts with og basename and then .
                if os.path.basename(access_url).startswith(f"{og_basename}."):
                    self.total_files_to_reingest += 1
                    # Add to ingest row dict to ingest from temp location with updated name
                    new_row_dict[column_name] = {
                        # temp path is the full path to the renamed file in the temp bucket
                        "sourcePath": temp_path,
                        # New target path with updated basename
                        "targetPath": updated_tdr_metadata_path,
                    }
                    # Add to copy list for copying and renaming file currently in TDR
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

    def get_new_copy_and_ingest_list(self) -> Tuple[list[dict], list[list]]:
        rows_to_reingest = []
        files_to_copy_to_temp = []
        # Get all columns in table that are filerefs
        file_ref_columns = [
            col['name'] for col in self.table_schema_info['columns'] if col['datatype'] == 'fileref']
        for row_dict in self.table_metrics:
            new_row_dict, temp_copy_list = self._create_row_dict(
                row_dict, file_ref_columns)
            # If there is something to copy and update
            if new_row_dict and temp_copy_list:
                rows_to_reingest.append(new_row_dict)
                files_to_copy_to_temp.append(temp_copy_list)
        logging.info(f"Total rows to re-ingest: {len(self.rows_to_reingest)}")
        logging.info(
            f"Total files to copy and re-ingest: {self.total_files_to_reingest}")
        return rows_to_reingest, files_to_copy_to_temp


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


class BatchCopyAndIngest:
    def __init__(
            self,
            rows_to_ingest: list[dict],
            tdr: TDR,
            target_table_name: str,
            cloud_type: str,
            update_strategy: str,
            workers: int,
            dataset_id: str,
            copy_and_ingest_batch_size: int,
            row_files_to_copy: list[list[dict]]
    ) -> None:
        self.rows_to_ingest = rows_to_ingest
        self.tdr = tdr
        self.target_table_name = target_table_name
        self.dataset_id = dataset_id
        self.cloud_type = cloud_type
        self.update_strategy = update_strategy
        self.workers = workers
        self.copy_and_ingest_batch_size = copy_and_ingest_batch_size
        self.row_files_to_copy = row_files_to_copy

    def run(self) -> None:
        # Batch through rows to copy files down and ingest so if script fails partway through large
        # copy and ingest it will have copied over and ingested some of the files already
        logging.info(
            f"""Batching {len(self.rows_to_ingest)} total rows into batches of {self.copy_and_ingest_batch_size} for
            copying to temp location and ingest"""
        )
        total_batches = len(self.rows_to_ingest) // self.copy_and_ingest_batch_size + 1
        gcp_functions = GCPCloudFunctions()
        for i in range(0, len(self.rows_to_ingest), self.copy_and_ingest_batch_size):
            batch_number = i // self.copy_and_ingest_batch_size + 1
            logging.info(
                f"Starting batch {batch_number} of {total_batches} for copy to temp and ingest")
            ingest_metadata_batch = self.rows_to_ingest[i:i +
                                                        self.copy_and_ingest_batch_size]
            files_to_copy_batch = self.row_files_to_copy[i:i +
                                                         self.copy_and_ingest_batch_size]
            # files_to_copy_batch will be a list of lists of dicts, so flatten it
            files_to_copy = [
                file_dict for sublist in files_to_copy_batch for file_dict in sublist]

            # Copy files to temp bucket
            gcp_functions.multithread_copy_of_files_with_validation(
                # Create dict with new names for copy of files to temp bucket
                files_to_move=files_to_copy,
                workers=self.workers,
                max_retries=5
            )

            # Ingest renamed files into dataset
            StartAndMonitorIngest(
                tdr=self.tdr,
                ingest_records=ingest_metadata_batch,
                target_table_name=self.target_table_name,
                dataset_id=self.dataset_id,
                load_tag=f"{self.target_table_name}_re-ingest",
                bulk_mode=False,
                update_strategy=UPDATE_STRATEGY,
                waiting_time_to_poll=WAITING_TIME_TO_POLL
            ).run()

            # Delete files from temp bucket
            # Create list of files in temp location to delete. Full destination path is the temp location from copy
            files_to_delete = [file_dict['full_destination_path']
                               for file_dict in files_to_copy]
            gcp_functions.delete_multiple_files(
                # Create list of files in temp location to delete. Full destination path is the temp location from copy
                files_to_delete=files_to_delete,
                workers=self.workers,
            )


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
    files_info = tdr.create_file_dict(dataset_id=dataset_id, limit=1000)

    # Get all metrics for table
    dataset_metrics = tdr.get_data_set_table_metrics(
        dataset_id=dataset_id, target_table_name=dataset_table_name)

    # Get information on files that need to be reingested
    rows_to_reingest, row_files_to_copy = GetRowAndFileInfoForReingest(
        table_schema_info=table_schema_info,
        files_info=files_info,
        table_metrics=dataset_metrics,
        row_identifier=row_identifier,
        og_file_basename_column=original_file_basename_column,
        new_file_basename_column=new_file_basename_column,
        temp_bucket=temp_bucket
    ).get_new_copy_and_ingest_list()

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
