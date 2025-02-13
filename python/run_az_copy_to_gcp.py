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
    return parser.parse_args()


def should_reload(expiry_time_str: str, time_before_reload: int) -> bool:
    """
    Check if the given expiry time has expired or will expire in the next 10 minutes.

    :param expiry_time_str: Expiry time in ISO format (e.g., "2025-02-13T19:31:47Z").
    :return: True if expired or will expire in 10 minutes, otherwise False.
    """
    # Parse expiry time string to a datetime object
    expiry_time = datetime.strptime(expiry_time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

    # Get current UTC time
    current_time = datetime.now(timezone.utc)

    # Check if expired or will expire in 10 minutes # current_time >= expiry_time or
    return (expiry_time - current_time) <= timedelta(minutes=time_before_reload)


if __name__ == '__main__':
    args = get_args()
    tsv = args.tsv
    time_before_reload = args.time_before_reload

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
    for row in tsv_contents:
        az_path = row["az_path"]
        dataset_id = row["dataset_id"]
        target_url = row["target_url"]
        if (
            dataset_id not in dataset_tokens
            or should_reload(
                expiry_time_str=dataset_tokens[dataset_id]['expiry_time'],
                time_before_reload=time_before_reload
            )  # if no token for dataset or has already expired or will expire soon
        ):
            dataset_tokens[dataset_id] = tdr.get_sas_token(dataset_id=dataset_id)
        signed_source_url = az_path + "?" + dataset_tokens[dataset_id]['sas_token']
        azcopy_command = [
            "azcopy_linux_amd64_10.28.0/azcopy",
            "copy",
            signed_source_url,
            target_url
        ]
        result = subprocess.run(azcopy_command, env=os.environ, capture_output=True, text=True)
        logging.info(f'stdout for {signed_source_url} to {target_url}: {result.stdout}')
        logging.info(f'stderr for {signed_source_url} to {target_url}: {result.stderr}')
        if result.returncode != 0:
            logging.error(f"Error copying {signed_source_url} to {target_url}")
        else:
            if gcp_util.check_file_exists(target_url):
                logging.info(f"Successfully copied {signed_source_url} to {target_url}.")
            else:
                logging.error(f"Error copying {signed_source_url} to {target_url}")
