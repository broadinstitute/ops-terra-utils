"""
This script takes in a tsv of billing_project/workspace_name combinations and finds the
associated Terra project of the workspace. Using the Terra project, it determines the
bucket where the GCP logs are located. It then finds the last-modified log in the bucket
and finds the last logging line within the log file. The idea is to get the last activity
that occurred in the bucket. It then outputs a tsv with metadata regarding the bucket
and the associated log information.
"""

import os
import subprocess
import logging
from argparse import ArgumentParser, Namespace
from typing import Optional

from utils.csv_util import Csv
from utils.gcp_utils import GCPCloudFunctions
from utils import GCP
from utils.requests_utils.request_util import RunRequest
from utils.terra_utils.terra_util import TerraWorkspace
from utils.token_util import Token
from datetime import datetime

CLOUD_TYPE = GCP

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        "--input_tsv",
        required=True,
        help="Path to the input tsv file with 'billing_project' and 'workspace_name' as headers"
    )
    parser.add_argument(
        "--output_tsv",
        required=True,
        help="Path to the output tsv file where metadata should be written"
    )
    return parser.parse_args()


def set_gcloud_account(gcloud_account: str) -> None:
    try:
        # Run the gcloud command using subprocess
        subprocess.run(["gcloud", "config", "set", "account", gcloud_account], check=True)
        logging.info(f"Successfully set account to {gcloud_account}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error setting gcloud account: {e}")


def get_user() -> Optional[str]:
    return os.getenv("USER")


def set_firecloud_account() -> None:
    firecloud_account = f"{get_user()}@firecloud.org"
    set_gcloud_account(firecloud_account)


def set_broad_account() -> None:
    gcloud_account = f"{get_user()}@broadinstitute.org"
    set_gcloud_account(gcloud_account)


if __name__ == '__main__':
    args = parse_args()
    # set broad account
    logging.info("Setting Broad account")
    set_broad_account()

    token = Token(cloud=CLOUD_TYPE)
    request_util = RunRequest(token=token)
    workspaces = Csv(file_path=args.input_tsv).create_list_of_dicts_from_tsv()
    workspace_metadata = []

    for workspace in workspaces:
        workspace_name = workspace["workspace_name"]
        billing_project = workspace["billing_project"]

        workspace_info = TerraWorkspace(
            billing_project=billing_project,
            workspace_name=workspace_name,
            request_util=request_util
        ).get_workspace_info()
        google_project = workspace_info["workspace"]["googleProject"]
        logging.info(
            f"Found Google project {google_project} for billing project/workspace {billing_project}/{workspace_name}")

        # set firecloud account
        logging.info("Setting Firecloud account")
        set_firecloud_account()

        # instantiate the gcp tools
        most_recent_file, file_contents = GCPCloudFunctions(
            project=google_project
        ).get_file_contents_of_most_recent_blob_in_bucket(bucket_name=f"storage-logs-{google_project}")

        last_line = file_contents.strip().split("\n")[-1]
        timestamp_microseconds = int(last_line.split(",")[0].strip('"'))
        timestamp_seconds = timestamp_microseconds / 1_000_000
        human_readable_time = datetime.utcfromtimestamp(timestamp_seconds).strftime('%Y-%m-%d %H:%M:%S')
        workspace_metadata.append(
            {
                "billing_project": billing_project,
                "workspace_name": workspace_name,
                "most_recent_log_file": most_recent_file,
                "last_line_in_log": last_line,
                "last_log_timestamp": human_readable_time
            }
        )

    Csv(file_path=args.output_tsv).create_tsv_from_list_of_dicts(list_of_dicts=workspace_metadata)