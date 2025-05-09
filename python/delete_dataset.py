"""Delete a dataset in TDR"""
import logging
from argparse import ArgumentParser, Namespace

from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(description="Delete a dataset in TDR")
    parser.add_argument("--dataset_id", "-i", required=True)
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()
    dataset_id = args.dataset_id

    token = Token()
    request_util = RunRequest(token=token)
    tdr = TDR(request_util=request_util)
    tdr.delete_dataset(dataset_id=dataset_id)
