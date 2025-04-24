import logging
from ops_utils.terra_util import Terra
from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.request_util import RunRequest
from ops_utils.csv_util import Csv
from ops_utils.token_util import Token
from ops_utils.gcp_utils import GCPCloudFunctions
from ops_utils.vars import GCP
import json
import subprocess
from argparse import ArgumentParser, Namespace
from datetime import datetime, timedelta, timezone
import os
import time
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
    parser.add_argument(
        "--log_dir",
        "-l",
        help="GCP log directory where to put logs for failed copies"
    )
    return parser.parse_args()


class CopyFile:
    def __init__(
            self,
            az_path: str,
            time_before_reload: int,
            dataset_tokens: dict,
            file_row: dict,
            log_dir: str,
            gcp_util: GCPCloudFunctions
    ):
        self.az_path = az_path
        self.time_before_reload = time_before_reload
        self.dataset_tokens = dataset_tokens
        self.file_row = file_row
        self.log_dir = log_dir
        self.gcp_util = gcp_util

    def _should_reload(self, expiry_time_str: str) -> bool:
        # Parse expiry time string to a datetime object
        expiry_time = datetime.strptime(expiry_time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

        # Get current UTC time
        current_time = datetime.now(timezone.utc)

        # Check if expired or will expire in 10 minutes # current_time >= expiry_time or
        return (expiry_time - current_time) <= timedelta(minutes=self.time_before_reload)

    def _validate_az_copy(self, local_path: str, source_url: str, copy_results: subprocess.CompletedProcess) -> None:
        if copy_results.returncode != 0 or not os.path.isfile(local_path):
            # Regular expression to find the log file path
            match = re.search(r"Log file is located at: (\S+)", copy_results.stdout)
            if match:
                log_file_path = match.group(1)
                log_copy_path = os.path.join(self.log_dir, f"{local_path}.azcopy_log.txt")
                gcp_util.upload_blob(source_file=log_file_path, destination_path=log_copy_path)
                logging.info(f"Uploaded log file to {log_copy_path}")
            else:
                logging.warning("Could not find log file path in azcopy output")

            logging.error(f"Error copying {source_url} to {local_path}")
            raise Exception(f"Error copying {source_url} to {local_path}")

    def _run_az_copy_local(self, signed_source_url: str, target_url: str) -> str:
        local_path = os.path.basename(target_url)
        azcopy_command = [
            self.az_path,
            "copy",
            signed_source_url,
            local_path
        ]
        result = subprocess.run(azcopy_command, env=os.environ, capture_output=True, text=True)
        logging.info(f'stdout for {signed_source_url} to {local_path}: {result.stdout}')
        self._validate_az_copy(local_path, signed_source_url, result)
        logging.info(f"Successfully copied {signed_source_url} to {local_path}")
        return local_path

    @staticmethod
    def _already_copied(target_url: str, bytes: int) -> bool:
        logging.info(f"Checking if {target_url} has already been copied.")
        if gcp_util.check_file_exists(target_url):
            if gcp_util.get_filesize(target_url) == int(bytes):
                return True
        return False

    def run(self) -> None:
        az_path = row["az_path"]
        dataset_id = row["dataset_id"]
        target_url = row["target_url"]
        bytes = row["bytes"]
        if self._already_copied(target_url, bytes):
            logging.info(f"Skipping {target_url} as it has already been copied.")
            return
        # if no token for dataset or has already expired or will expire soon
        if (
                dataset_id not in self.dataset_tokens
                or self._should_reload(expiry_time_str=self.dataset_tokens[dataset_id]['expiry_time'])
        ):
            dataset_tokens[dataset_id] = tdr.get_sas_token(dataset_id=dataset_id)
        signed_source_url = az_path + "?" + dataset_tokens[dataset_id]['sas_token']
        logging.info(f"Copying {signed_source_url} to {os.path.basename(target_url)}")
        local_file = self._run_az_copy_local(signed_source_url, target_url)
        logging.info(f"Uploading {local_file} to {target_url}")
        gcp_util.upload_blob(source_file=local_file, destination_path=target_url)
        logging.info(
            f"Successfully copied {signed_source_url} to local path and then uploaded to {target_url}."
            " Removing local file.")
        os.remove(local_file)


if __name__ == '__main__':
    args = get_args()
    tsv = args.tsv
    time_before_reload = args.time_before_reload
    az_path = args.az_path
    log_dir = args.log_dir

    token = Token(cloud=GCP)
    request_util = RunRequest(token=token)
    terra = Terra(request_util=request_util)
    tdr = TDR(request_util=request_util)

    tsv_contents = Csv(tsv).create_list_of_dicts_from_tsv()
    pet_account_json = terra.get_pet_account_json()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"] = json.dumps(pet_account_json)

    dataset_tokens: dict = {}
    gcp_util = GCPCloudFunctions()
    files_copied = 0
    for row in tsv_contents:
        start_time = time.time()
        CopyFile(
            az_path=az_path,
            time_before_reload=time_before_reload,
            dataset_tokens=dataset_tokens,
            file_row=row,
            gcp_util=gcp_util,
            log_dir=log_dir
        ).run()
        # Log time taken to copy or confirm file copied
        end_time = time.time()
        file_gb_size = int(row['bytes']) / (1024 ** 3)
        total_time = (end_time - start_time) / 60
        logging.info(
            f"Time taken to copy or confirm {row['az_path']} ({file_gb_size:.2f} GB) "
            f"to {row['target_url']} copied: {total_time:.2f} minutes"
        )
        files_copied += 1
        logging.info(f"Files copied: {files_copied} / {len(tsv_contents)}")
