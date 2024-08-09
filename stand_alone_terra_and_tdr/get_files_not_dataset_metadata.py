
from utils import TDR, RunRequest, Token
import logging
import json
import sys

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

DATASET_ID = "0d1c9aea-e944-4d19-83c3-8675f6aa062a"
CLOUD_TYPE = "gcp"
MAX_RETRIES = 1
MAX_BACKOFF_TIME = 10
BATCH_SIZE_TO_LIST_FILES = 25000

if __name__ == "__main__":
    # Initialize the Terra and TDR classes
    token = Token(cloud=CLOUD_TYPE)
    request_util = RunRequest(token=token, max_retries=MAX_RETRIES, max_backoff_time=MAX_BACKOFF_TIME)
    tdr = TDR(request_util=request_util)
    # Get all file uuids from metadata
    all_metadata_dataset_file_uuids = tdr.get_data_set_file_uuids_from_metadata(dataset_id=DATASET_ID)
    # Get all files for dataset
    files_info = tdr.get_data_set_files(dataset_id=DATASET_ID, limit=BATCH_SIZE_TO_LIST_FILES)
    file_uuids = [file_dict['fileId'] for file_dict in files_info]

    # Find any file uuids that are unique in file_uuids and all_dataset_file_uuids and return them and where they came from
    unique_file_uuids = set(file_uuids) - set(all_metadata_dataset_file_uuids)
    if unique_file_uuids:
        # Only uuids where it is listed in dataset but not referenced in metadata
        file_uuids_only = [
            file_uuid
            for file_uuid in unique_file_uuids
            if file_uuid in set(file_uuids)
        ]
        # Only uuids where it is listed in metadata but not in dataset file list
        metadata_uuids_only = [
            file_uuid
            for file_uuid in unique_file_uuids
            if file_uuid in set(all_metadata_dataset_file_uuids)
        ]
        if file_uuids_only:
            uuid_str = '\n'.join(file_uuids_only)
            logging.info(f"Unique file uuids only in files and not referenced:\n{uuid_str}")
        if metadata_uuids_only:
            uuid_str = '\n'.join(metadata_uuids_only)
            logging.info(f"Unique file uuids only in metadata and not in files:\n{uuid_str}")
        logging.info(f"Total unique file uuids count: {len(unique_file_uuids)}")
    else:
        logging.info("No unique file uuids found")


