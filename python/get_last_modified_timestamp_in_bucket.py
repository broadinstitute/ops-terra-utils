import argparse
import os
import subprocess
import logging

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


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--billing_project", required=True)
    parser.add_argument("--workspace_name", required=True)
    return parser.parse_args()


def set_gcloud_account(gcloud_account: str) -> None:
    try:
        # Run the gcloud command using subprocess
        subprocess.run(["gcloud", "config", "set", "account", gcloud_account], check=True)
        logging.info(f"Successfully set account to {gcloud_account}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error setting gcloud account: {e}")

def get_user() -> str:
    return os.getenv("USER")

def set_firecloud_account():
    firecloud_account = f"{get_user()}@firecloud.org"
    set_gcloud_account(firecloud_account)

def set_broad_account():
    gcloud_account = f"{get_user()}@broadinstitute.org"
    set_gcloud_account(gcloud_account)

def set_application_default_credentials():
    creds_path = "/Users/sahakian/Documents/repositories/ops-terra-utils/firecloud_credentials.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path


if __name__ == '__main__':
    args = parse_args()
    # set broad account
    logging.info("Setting Broad account")
    set_broad_account()

    token = Token(cloud=CLOUD_TYPE)
    request_util = RunRequest(token=token)
    terra_workspace = TerraWorkspace(billing_project=args.billing_project, workspace_name=args.workspace_name, request_util=request_util)
    workspace_info = terra_workspace.get_workspace_info()
    google_project = workspace_info["workspace"]["googleProject"]
    logging.info(f"Found Google project {google_project} for billing project/workspace {args.billing_project}/{args.workspace_name}")

    # set firecloud account
    logging.info("Setting Firecloud account")
    set_firecloud_account()
    set_application_default_credentials()

    # instantiate the gcp tools
    gcp = GCPCloudFunctions(project=google_project)
    file_contents = gcp.get_file_contents_of_most_recent_blob_in_bucket(bucket_name=f"storage-logs-{google_project}")
    last_line = file_contents.strip().split("\n")[-1]
    timestamp_str = last_line.split(",")[0].strip('"')
    timestamp_microseconds = int(timestamp_str)
    timestamp_seconds = timestamp_microseconds / 1_000_000
    human_readable_time = datetime.utcfromtimestamp(timestamp_seconds).strftime('%Y-%m-%d %H:%M:%S')
    print(human_readable_time)
