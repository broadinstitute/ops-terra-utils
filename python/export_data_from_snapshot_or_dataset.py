import argparse
import logging
import os.path
from argparse import ArgumentParser
from collections import Counter

from utils.copy_dataset_or_snapshot_files import CopyDatasetOrSnapshotFiles
from ops_utils.gcp_utils import GCPCloudFunctions
from ops_utils.token_util import Token
from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.request_util import RunRequest
from ops_utils.vars import ARG_DEFAULTS

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> argparse.Namespace:
    parser = ArgumentParser(description="Download data from an existing snapshot to a Google bucket")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--snapshot_id", required=False)
    group.add_argument("--dataset_id", required=False)
    parser.add_argument("--output_bucket", required=True)
    parser.add_argument(
        "--download_type",
        choices=["flat", "structured"],
        help="""How you'd like your downloaded data to be structured in the output bucket. 'flat' indicates that all
        files will be downloaded to the root of your bucket. 'structured' indicates that the original file path
        structure will be maintained.""",
        required=True
    )
    parser.add_argument(
        "--max_backoff_time",
        required=False,
        default=ARG_DEFAULTS["max_backoff_time"],
        help="The maximum backoff time for a failed request (in seconds). " +
             f"Defaults to {ARG_DEFAULTS['max_backoff_time']} seconds if not provided"
    )
    parser.add_argument(
        "--max_retries",
        required=False,
        default=ARG_DEFAULTS["max_retries"],
        help="The maximum number of retries for a failed request. " +
             f"Defaults to {ARG_DEFAULTS['max_retries']} if not provided"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="If set, will print additional information during the download process"
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()
    snapshot_id = args.snapshot_id
    dataset_id = args.dataset_id
    output_bucket = args.output_bucket
    download_type = args.download_type
    max_backoff_time = args.max_backoff_time
    max_retries = args.max_retries
    verbose = args.verbose

    if not (snapshot_id or dataset_id):
        raise Exception("Either snapshot id OR dataset id are required. Received neither")

    token = Token()
    request_util = RunRequest(token=token, max_retries=max_retries, max_backoff_time=max_backoff_time)
    tdr = TDR(request_util=request_util)

    CopyDatasetOrSnapshotFiles(
        tdr=tdr,
        snapshot_id=snapshot_id,
        dataset_id=dataset_id,
        output_bucket=output_bucket,
        download_type=download_type,
        gcp_functions=GCPCloudFunctions(),
        verbose=verbose
    ).run()
