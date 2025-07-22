"""Make snapshot public in TDR"""
import logging
from argparse import ArgumentParser, Namespace

from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(description="Make snapshot public in TDR")
    parser.add_argument("--snapshot_id", "-i", required=True)
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()
    snapshot_id = args.snapshot_id

    token = Token()
    request_util = RunRequest(token=token)
    tdr = TDR(request_util=request_util)
    logging.info(f"Making snapshot {snapshot_id} public in TDR")
    tdr.make_snapshot_public(snapshot_id=snapshot_id)
    logging.info(f"Snapshot {snapshot_id} is now public in TDR")
