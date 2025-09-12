import logging
from argparse import ArgumentParser, Namespace

from ops_utils.request_util import RunRequest
from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.token_util import Token

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    """Parse CLI args for deleting dataset files and related snapshots."""
    parser = ArgumentParser(description="Delete dataset files by ID")
    parser.add_argument("-id", "--dataset_id", required=True)
    parser.add_argument(
        "-f",
        "--file_list",
        required=True,
        help="Path to file with file UUIDs (one per line)",
    )
    parser.add_argument(
        "--service_account_json",
        "-saj",
        type=str,
        help=(
            "Path to service account JSON. Uses default "
            "credentials if omitted."
        ),
    )
    parser.add_argument(
        "--dry_run",
        "-n",
        action="store_true",
        help=(
            "Do not perform deletions; log actions that would be taken."
        ),
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()
    service_account_json = args.service_account_json

    token = Token(service_account_json=service_account_json)
    request_util = RunRequest(token=token)
    tdr = TDR(request_util=request_util, dry_run=args.dry_run)
    file_list = args.file_list

    with open(file_list, 'r') as f:
        file_ids = {line.strip() for line in f}

    if not file_ids:
        logging.info("No file ids provided; nothing to delete")
    else:
        logging.info(f"Found {len(file_ids)} file ids in {file_list} to delete")
        tdr.delete_files_and_snapshots(dataset_id=args.dataset_id, file_ids=file_ids)
