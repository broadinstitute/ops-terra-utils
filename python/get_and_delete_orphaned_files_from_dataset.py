import logging
from argparse import ArgumentParser

from utils.tdr_util import TDR
from utils.request_util import RunRequest
from utils.token_util import Token
from utils import GCP

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

CLOUD_TYPE = GCP
MAX_RETRIES = 5
MAX_BACKOFF_TIME = 5 * 60
BATCH_SIZE_TO_LIST_FILES = 20000


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
    parser.add_argument(
        "--delete_orphaned_files",
        action="store_true",
        help="Delete files that are not in the dataset metadata but exist in dataset"
    )
    parser.add_argument(
        "--batch_size_to_list_files",
        action="store",
        type=int,
        default=BATCH_SIZE_TO_LIST_FILES,
        help=f"The batch size to query files in the dataset. Defaults to {BATCH_SIZE_TO_LIST_FILES}"
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

    # Find any file uuids that exist in the dataset but not in the metadata
    orphaned_file_uuids = list(set(file_uuids) - set(all_metadata_dataset_file_uuids))
    if orphaned_file_uuids:
        uuid_str = '\n'.join(orphaned_file_uuids)
        logging.info(f"Below are the {len(orphaned_file_uuids)} orphaned file UUIDs:\n{uuid_str}")
        if args.delete_orphaned_files:
            logging.info("Deleting orphaned files")
            for file_uuid in orphaned_file_uuids:
                tdr.delete_files(file_ids=orphaned_file_uuids, dataset_id=dataset_id)
        else:
            logging.info("To delete orphaned files, run the script with --delete_orphaned_files flag")
    else:
        logging.info("No orphaned files found")
