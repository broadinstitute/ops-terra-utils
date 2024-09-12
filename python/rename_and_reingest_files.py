import json
import logging
import sys
import os
from argparse import ArgumentParser, Namespace
from typing import Optional
from utils.tdr_util import TDR, BatchIngest
from utils.token_util import Token
from utils.request_util import RunRequest
from utils import GCP

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

CLOUD_TYPE = GCP
MAX_RETRIES = 2
MAX_BACKOFF_TIME = .5 * 60
BATCH_SIZE_TO_LIST_FILES = 25000
INGEST_BATCH_SIZE = 500
WAITING_TIME_TO_POLL = 30


def get_args() -> Namespace:
    parser = ArgumentParser(description="Renamed files within a dataset and reingest them")
    parser.add_argument("-i", "--dataset_id", required=True)
    parser.add_argument("-o", "--original_file_basename_column", required=True, help=f"The basename column which you want to rename. ie 'sample_id'")
    parser.add_argument("-n", "--new_file_basename_column", required=True, help=f"The new basename column which you want the old one replace with. ie 'collab_sample_id'")
    parser.add_argument("-t", "--dataset_table_name", required=True, help="The name of the table in TDR")
    parser.add_argument("-ri", "--row_identifier", required=True, help="The unique identifier for the row in the table. ie 'sample_id'")
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


class CreateIngestMetadata:
    def __init__(self, table_schema_info: dict, files_info: dict, table_metrics: list[dict], og_file_basename_column: str, new_file_basename_column: str, row_identifier: str):
        self.table_schema_info = table_schema_info
        self.files_info = files_info
        self.table_metrics = table_metrics
        self.og_file_basename_column = og_file_basename_column
        self.new_file_basename_column = new_file_basename_column
        self.row_identifier = row_identifier
        self.total_files_to_reingest = 0
        self.rows_to_reingest = []

    def create_row_dict(self, row_dict: dict, file_ref_columns: list[str]) -> Optional[dict]:
        """Go through each row and check each cell if it is a file and if it needs to be reingested.
        If so, create a new row dict with the new file path."""
        reingest_row = False
        # Create new dictionary with just the row identifier
        new_row_dict = {self.row_identifier: row_dict[self.row_identifier]}
        # Get basename to replace
        og_basename = row_dict[self.og_file_basename_column]
        new_basename = row_dict[self.new_file_basename_column]
        for column_name in row_dict:
            # Check if column is a fileref
            if column_name in file_ref_columns:
                # Get full file info for that cell
                file_info = self.files_info.get(row_dict[column_name])
                # Check if file starts with og basename and then .
                if os.path.basename(file_info['path']).startswith(f"{og_basename}."):
                    self.total_files_to_reingest += 1
                    # Create new file reference
                    new_row_dict[column_name] = {
                        "sourcePath": file_info['fileDetail']['accessUrl'],
                        # Create new target path with updated basename
                        "targetPath": file_info['path'].replace(og_basename, new_basename),
                    }
                    # Set reingest row to True because files need to be updated
                    reingest_row = True
        if reingest_row:
            return new_row_dict
        else:
            return None

    def get_new_ingest_list(self) -> list[dict]:
        # Get all columns in table that are filerefs
        file_ref_columns = [col['name'] for col in self.table_schema_info['columns'] if col['datatype'] == 'fileref']
        for row_dict in self.table_metrics:
            new_row_dict = self.create_row_dict(row_dict, file_ref_columns)
            if new_row_dict:
                self.rows_to_reingest.append(new_row_dict)
        logging.info(f"Total rows to re-ingest: {len(self.rows_to_reingest)}")
        logging.info(f"Total files to re-ingest: {self.total_files_to_reingest}")
        return self.rows_to_reingest


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

    # Initialize TDR classes
    token = Token(cloud=CLOUD_TYPE)
    request_util = RunRequest(token=token, max_retries=max_retries, max_backoff_time=max_backoff_time)
    tdr = TDR(request_util=request_util)

    # Get schema info for table
    table_schema_info = tdr.get_table_schema_info(dataset_id=dataset_id, table_name=dataset_table_name)

    # Get all dict of all files where key is uuid
    files_info = tdr.create_file_dict(dataset_id=dataset_id, limit=1000)

    # Get all metrics for table
    dataset_metrics = tdr.get_data_set_table_metrics(dataset_id=dataset_id, target_table_name=dataset_table_name)

    # Create new ingest list
    rows_to_reingest = CreateIngestMetadata(
        table_schema_info=table_schema_info,
        files_info=files_info,
        table_metrics=dataset_metrics,
        row_identifier=row_identifier,
        og_file_basename_column=original_file_basename_column,
        new_file_basename_column=new_file_basename_column
    ).get_new_ingest_list()

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










