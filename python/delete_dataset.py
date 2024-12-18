"""Delete a dataset in TDR"""


import logging
from argparse import ArgumentParser, Namespace

from utils.tdr_utils.tdr_api_utils import TDR
from utils.requests_utils.request_util import RunRequest
from utils.token_util import Token
from utils import GCP

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

CLOUD_TYPE = GCP


def get_args() -> Namespace:
    parser = ArgumentParser(description="Delete a dataset in TDR")
    parser.add_argument("--dataset_id", "-i", required=True)
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()
    dataset_id = args.dataset_id

    token = Token(cloud=CLOUD_TYPE)
    request_util = RunRequest(token=token)
    tdr = TDR(request_util=request_util)
    tdr.delete_dataset(dataset_id=dataset_id)
