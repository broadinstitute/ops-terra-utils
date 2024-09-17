"""Delete a dataset in TDR"""


import logging
from argparse import ArgumentParser, Namespace

from utils.tdr_util import TDR
from utils.request_util import RunRequest
from utils.token_util import Token
from utils import GCP

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

CLOUD_TYPE = GCP


def get_args() -> Namespace:
    parser = ArgumentParser(description="Delete a snapshot in TDR")
    parser.add_argument("--snapshot_id", required=True)
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()
    snapshot_id = args.snapshot_id

    token = Token(cloud=CLOUD_TYPE)
    request_util = RunRequest(token=token)
    tdr = TDR(request_util=request_util)
    tdr.delete_snapshot(snapshot_id=snapshot_id)
