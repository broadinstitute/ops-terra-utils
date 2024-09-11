import logging
from argparse import ArgumentParser

from utils.tdr_util import TDR
from utils.request_util import RunRequest
from utils.token_util import Token
from utils.general_utils import GCP

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

CLOUD_TYPE = GCP
MAX_RETRIES = 5
MAX_BACKOFF_TIME = 5 * 60
BATCH_SIZE_TO_LIST_FILES = 25000


def get_args():
    parser = ArgumentParser(description="Get files that are not in the dataset metadata")
    parser.add_argument("--dataset_id", required=True)
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
        help=f"The maximum backoff time for a failed request (in seconds). Defaults to {MAX_BACKOFF_TIME} seconds if not provided"
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    dataset_id = args.dataset_id
    max_retries = args.max_retries
    max_backoff_time = args.max_backoff_time

    # Initialize the Terra and TDR classes
    token = Token(cloud=CLOUD_TYPE)
    request_util = RunRequest(token=token, max_retries=max_retries, max_backoff_time=max_backoff_time)
    tdr = TDR(request_util=request_util)
    # Get all file uuids from metadata
    all_metadata_dataset_file_uuids = tdr.get_data_set_file_uuids_from_metadata(dataset_id=dataset_id)
    # Get all files for dataset
    files_info = tdr.get_data_set_files(dataset_id=dataset_id, limit=BATCH_SIZE_TO_LIST_FILES)
    file_uuids = [file_dict['fileId'] for file_dict in files_info]

    # Find any file uuids that are unique in file_uuids and all_dataset_file_uuids and return them and where
    # they came from
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
