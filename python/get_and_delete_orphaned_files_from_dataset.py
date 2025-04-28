import logging
import argparse

from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils.vars import ARG_DEFAULTS

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Get files that are not in the dataset metadata")
    parser.add_argument("--dataset_id", "-i", required=True)
    parser.add_argument(
        "--max_retries",
        required=False,
        default=ARG_DEFAULTS['max_retries'],
        help="The maximum number of retries for a failed request. " +
             f"Defaults to {ARG_DEFAULTS['max_retries']}"
    )
    parser.add_argument(
        "--max_backoff_time",
        required=False,
        default=ARG_DEFAULTS['max_backoff_time'],
        help="The maximum backoff time for a failed request (in seconds). " +
             f"Defaults to {ARG_DEFAULTS['max_backoff_time']}"
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
        default=ARG_DEFAULTS['batch_size_to_list_files'],
        help=f"The batch size to query files in the dataset. Defaults to {ARG_DEFAULTS['batch_size_to_list_files']}"
    )
    parser.add_argument(
        "--batch_size_to_delete_files",
        action="store",
        type=int,
        default=ARG_DEFAULTS['batch_size_to_delete_files'],
        help=f"The batch size to submit all delete jobs together and wait until all have completed before moving to "
             f"next batch. Defaults to {ARG_DEFAULTS['batch_size_to_delete_files']}"
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    dataset_id = args.dataset_id
    max_retries = args.max_retries
    max_backoff_time = args.max_backoff_time
    batch_size_to_list_files = args.batch_size_to_list_files
    batch_size_to_delete_files = args.batch_size_to_delete_files

    # Initialize the Terra and TDR classes
    token = Token()
    request_util = RunRequest(token=token, max_retries=max_retries, max_backoff_time=max_backoff_time)
    tdr = TDR(request_util=request_util)
    # Get all file uuids from metadata
    all_metadata_dataset_file_uuids = tdr.get_dataset_file_uuids_from_metadata(dataset_id=dataset_id)
    # Get all files for dataset
    files_info = tdr.get_dataset_files(dataset_id=dataset_id, limit=batch_size_to_list_files)
    file_uuids = [file_dict["fileId"] for file_dict in files_info]

    # Find any file uuids that exist in the dataset but not in the metadata
    orphaned_file_uuids = list(set(file_uuids) - set(all_metadata_dataset_file_uuids))
    if orphaned_file_uuids:
        uuid_str = "\n".join(orphaned_file_uuids)
        logging.info(
            f"Below are the {len(orphaned_file_uuids)} orphaned file UUIDs:\n{uuid_str}")
        if args.delete_orphaned_files:
            logging.info("Deleting orphaned files")
            tdr.delete_files(
                file_ids=orphaned_file_uuids,
                dataset_id=dataset_id,
                batch_size_to_delete_files=batch_size_to_delete_files
            )
        else:
            logging.info("To delete orphaned files, run the script with --delete_orphaned_files flag")
    else:
        logging.info("No orphaned files found")
