import json
import logging
from argparse import ArgumentParser, Namespace


from typing import Optional
from utils.tdr_utils.tdr_api_utils import TDR
from utils.request_util import RunRequest
from utils.token_util import Token
from utils.bq_utils import BigQueryUtil
from utils import GCP


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(description="description of script")
    mutually_exclusive_group = parser.add_mutually_exclusive_group(required=True)
    mutually_exclusive_group.add_argument("--dataset_id", "-d")
    mutually_exclusive_group.add_argument("--snapshot_id", "-s")
    return parser.parse_args()


class GetAssetInfo:
    def __init__(self, tdr: TDR, dataset_id: Optional[str], snapshot_id: Optional[str]):
        self.tdr = tdr
        self.dataset_id = dataset_id
        self.snapshot_id = snapshot_id

    def _get_dataset_info(self) -> dict:
        dataset_info = self.tdr.get_dataset_info(
            dataset_id=self.dataset_id,
            info_to_include=["SCHEMA", "ACCESS_INFORMATION"]
        )
        return {
            "bq_project": dataset_info["accessInformation"]["bigQuery"]["projectId"],
            "bq_schema": dataset_info["accessInformation"]["bigQuery"]["datasetName"],
            "tables": dataset_info["schema"]["tables"],
            "relationships": dataset_info["schema"]["relationships"]
        }

    def _get_snapshot_info(self) -> dict:
        snapshot_info = self.tdr.get_snapshot_info(
            snapshot_id=self.snapshot_id,
            info_to_include=["TABLES", "RELATIONSHIPS", "ACCESS_INFORMATION"]
        )
        return {
            "bq_project": snapshot_info["accessInformation"]["bigQuery"]["projectId"],
            "bq_schema": snapshot_info["accessInformation"]["bigQuery"]["datasetName"],
            "tables": snapshot_info["tables"],
            "relationships": snapshot_info["relationships"]
        }

    def run(self):
        if self.dataset_id:
            return self._get_dataset_info()
        if self.snapshot_id:
            return self._get_snapshot_info()


if __name__ == '__main__':
    args = get_args()
    dataset_id = args.dataset_id
    snapshot_id = args.snapshot_id

    token = Token(cloud=GCP)
    request_util = RunRequest(token=token)
    tdr = TDR(request_util=request_util)

    asset_info_dict = GetAssetInfo(tdr=tdr, dataset_id=dataset_id, snapshot_id=snapshot_id).run()
    print(asset_info_dict["bq_project"])
    print(asset_info_dict["bq_schema"])
    print(asset_info_dict["tables"])
    big_query_util = BigQueryUtil()
