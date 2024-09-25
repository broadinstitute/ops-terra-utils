import argparse
import logging
import os.path
from argparse import ArgumentParser
from collections import Counter

from utils.gcp_utils import GCPCloudFunctions
from utils.token_util import Token
from utils.tdr_util import TDR
from utils.request_util import RunRequest
from utils import GCP

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


CLOUD_TYPE = GCP
MAX_RETRIES = 5
MAX_BACKOFF_TIME = 5 * 60
MULTITHREAD_WORKERS = 10


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
        default=MAX_BACKOFF_TIME,
        help=f"The maximum backoff time for a failed request (in seconds). Defaults to {MAX_BACKOFF_TIME} seconds if "
             f"not provided"
    )
    parser.add_argument(
        "--max_retries",
        required=False,
        default=MAX_RETRIES,
        help=f"The maximum number of retries for a failed request. Defaults to {MAX_RETRIES} if not provided"
    )
    return parser.parse_args()


class SourceDestinationMapping:
    def __init__(self, file_metadata: list[dict], output_bucket: str, download_type: str):
        self.file_metadata = file_metadata
        self.output_bucket = output_bucket
        self.download_type = download_type

    @staticmethod
    def _validate_file_destinations_unique(mapping: list[dict]) -> None:
        all_destination_paths = [a["full_destination_path"] for a in mapping]
        file_counts = Counter(all_destination_paths)
        duplicates = [file for file, count in file_counts.items() if count > 1]
        if duplicates:
            formatted_duplicates = "\n".join(duplicates)
            raise Exception(
                f"""Not all destinations were unique. If you selected 'flat' as the download type, try re-running with
                    'structured' instead. Below is a list of files that were duplicated:\n{formatted_duplicates}"""
            )

    def get_source_and_destination_paths(self) -> list[dict]:
        mapping = []
        output = self.output_bucket if self.output_bucket.startswith("gs://") else f"gs://{self.output_bucket}"

        for file in self.file_metadata:
            source = file["fileDetail"]["accessUrl"]
            if download_type == "flat":
                destination = os.path.join(output, os.path.basename(source).lstrip("/"))
            else:
                destination = os.path.join(output, file["path"].lstrip("/"))
            mapping.append(
                {
                    "source_file": source,
                    "full_destination_path": destination,
                }
            )

        self._validate_file_destinations_unique(mapping)
        return mapping


if __name__ == '__main__':
    args = get_args()
    snapshot_id = args.snapshot_id
    dataset_id = args.dataset_id
    output_bucket = args.output_bucket
    download_type = args.download_type
    max_backoff_time = args.max_backoff_time
    max_retries = args.max_retries

    if not (snapshot_id or dataset_id):
        raise Exception("Either snapshot id OR dataset id are required. Received neither")

    token = Token(cloud=CLOUD_TYPE)
    request_util = RunRequest(token=token, max_retries=max_retries, max_backoff_time=max_backoff_time)
    tdr = TDR(request_util=request_util)

    if snapshot_id:
        file_metadata = tdr.get_files_from_snapshot(snapshot_id=snapshot_id)
    else:
        file_metadata = tdr.get_data_set_files(dataset_id=dataset_id)

    # get the source and destination mapping and validate that all output paths are unique
    mapping = SourceDestinationMapping(
        file_metadata=file_metadata,
        output_bucket=output_bucket,
        download_type=download_type
    ).get_source_and_destination_paths()

    GCPCloudFunctions().multithread_copy_of_files_with_validation(
        files_to_move=mapping, workers=MULTITHREAD_WORKERS, max_retries=max_retries
    )
