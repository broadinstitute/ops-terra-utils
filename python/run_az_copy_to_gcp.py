import logging

from utils.terra_utils.terra_util import Terra
from utils.tdr_utils.tdr_api_utils import TDR
from utils.requests_utils.request_util import RunRequest
from utils.csv_util import Csv
from utils.token_util import Token
from utils.gcp_utils import GCPCloudFunctions
from utils import GCP
import json
import subprocess
from argparse import ArgumentParser, Namespace
from datetime import datetime, timedelta, timezone
import os
import re

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(description="Run azcopy of files from Azure dataset to GCP bucket")
    parser.add_argument(
        "--tsv",
        "-t",
        help="tsv where headers are az_path, dataset_id, and destination_path",
        required=True
    )
    parser.add_argument(
        "--time_before_reload",
        "-r",
        help="Time in minutes before token reload",
        type=int,
        default=30
    )
    parser.add_argument(
        "--az_path",
        "-a",
        help="Path to azcopy executable"
    )
    return parser.parse_args()


class CopyFile:
    def __init__(self, az_path: str, time_before_reload: int, dataset_tokens: dict, file_row: dict, gcp_util: GCPCloudFunctions):
        self.az_path = az_path
        self.time_before_reload = time_before_reload
        self.dataset_tokens = dataset_tokens
        self.file_row = file_row
        self.gcp_util = gcp_util

    def _should_reload(self, expiry_time_str: str) -> bool:
        # Parse expiry time string to a datetime object
        expiry_time = datetime.strptime(expiry_time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

        # Get current UTC time
        current_time = datetime.now(timezone.utc)

        # Check if expired or will expire in 10 minutes # current_time >= expiry_time or
        return (expiry_time - current_time) <= timedelta(minutes=self.time_before_reload)

    def _validate_copy(self, target_url: str, source_url: str, copy_results: subprocess.CompletedProcess) -> None:
        if copy_results.returncode != 0 or not self.gcp_util.check_file_exists(target_url):
            # Regular expression to find the log file path
            match = re.search(r"Log file is located at: (\S+)", copy_results.stdout)
            if match:
                log_file_path = match.group(1)
                gcp_util.upload_blob(source_file=log_file_path, destination_path=f'{target_url}.azcopy_log.txt')
                logging.info(f"Uploaded log file to {target_url}.azcopy_log.txt")
            else:
                logging.warning("Could not find log file path in azcopy output")

            logging.error(f"Error copying {source_url} to {target_url}")
            raise Exception(f"Error copying {source_url} to {target_url}")

    def run(self) -> None:
        az_path = row["az_path"]
        dataset_id = row["dataset_id"]
        target_url = row["target_url"]
        # if no token for dataset or has already expired or will expire soon
        if (
                dataset_id not in self.dataset_tokens
                or self._should_reload(expiry_time_str=self.dataset_tokens[dataset_id]['expiry_time'])
        ):
            dataset_tokens[dataset_id] = tdr.get_sas_token(dataset_id=dataset_id)
        signed_source_url = az_path + "?" + dataset_tokens[dataset_id]['sas_token']
        azcopy_command = [
            self.az_path,
            "copy",
            signed_source_url,
            target_url
        ]
        result = subprocess.run(azcopy_command, env=os.environ, capture_output=True, text=True)
        logging.info(f'stdout for {signed_source_url} to {target_url}: {result.stdout}')
        logging.info(f'stderr for {signed_source_url} to {target_url}: {result.stderr}')
        self._validate_copy(target_url, signed_source_url, result)
        logging.info(f"Successfully copied {signed_source_url} to {target_url}")


if __name__ == '__main__':
    args = get_args()
    tsv = args.tsv
    time_before_reload = args.time_before_reload
    az_path = args.az_path

    token = Token(cloud=GCP)
    request_util = RunRequest(token=token)
    terra = Terra(request_util=request_util)
    tdr = TDR(request_util=request_util)

    tsv_contents = Csv(tsv).create_list_of_dicts_from_tsv()
    pet_account_json = terra.get_pet_account_json()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"] = json.dumps(pet_account_json)

    dataset_tokens: dict = {}
    logging.info(f"Starting azcopy for {len(tsv_contents)} files")
    gcp_util = GCPCloudFunctions()
    files_copied = 0
    for row in tsv_contents:
        CopyFile(
            az_path=az_path,
            time_before_reload=time_before_reload,
            dataset_tokens=dataset_tokens,
            file_row=row,
            gcp_util=gcp_util
        ).run()
        files_copied += 1
        logging.info(f"Files copied: {files_copied} / {len(tsv_contents)}")
