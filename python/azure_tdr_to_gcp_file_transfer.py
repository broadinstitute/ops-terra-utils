import logging
import json
import subprocess
import google.cloud.logging
from google.cloud import storage
from pathlib import Path
from datetime import datetime, timezone, timedelta
from argparse import ArgumentParser, Namespace
from typing import Optional, Union

from utils.tdr_util import TDR
from utils.request_util import RunRequest
from utils.token_util import Token


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

google_logging_client = google.cloud.logging.Client()
google_logging_client.setup_logging()


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="""For deletion of on prem aggregations for input samples""")
    parser.add_argument(
        "-t",
        "--export_type",
        required=True,
        help="Target to export from TDR, either entire dataset or snapshot", choices=['dataset', 'snapshot']
    )
    parser.add_argument("-id", "--target_id", required=True,
                        help="ID of dataset or snapshot to export")
    parser.add_argument("-b", "--bucket_id", required=True,
                        help="Google bucket to export data to")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-op",
        "--bucket_output_path",
        required=False,
        help="Directory to upload files to within google bucket, cannot be used alongside retain_path_structure option"
    )
    group.add_argument(
        "-rps",
        "--retain_path_structure",
        required=False,
        default=False,
        action="store_true",
        help="Option to keep path structure set in TDR from path attribute"
    )
    return parser.parse_args()


class DownloadAzBlob:

    def __init__(self, export_info: dict, tdr_client: TDR) -> None:
        self.tdr_client = tdr_client
        self.export_info = export_info
        self.sas_token: Optional[dict] = None

    def time_until_token_expiry(self) -> Union[timedelta, None]:
        if self.sas_token:
            token_expiry = datetime.fromisoformat(
                self.sas_token["expiry_time"])
            current_time = datetime.now(timezone.utc)
            time_delta = token_expiry - current_time
            return time_delta
        return None

    def get_new_sas_token(self) -> None:
        logging.info("Obtaining new sas token")
        if self.export_info["endpoint"] == "dataset":
            self.sas_token = self.tdr_client.get_sas_token(
                dataset_id=self.export_info["id"])
        elif self.export_info["endpoint"] == "snapshot":
            self.sas_token = self.tdr_client.get_sas_token(
                snapshot_id=self.export_info["id"])

    @staticmethod
    def run_az_copy(blob_path: str, output_path: str) -> subprocess.CompletedProcess:
        az_copy_command = ["azcopy", "copy", f"{blob_path}", f"{output_path}", "--output-type=json"]
        copy_cmd = subprocess.run(az_copy_command, capture_output=True)
        return copy_cmd

    def run(self, blob_path: str, output_path: str) -> Union[list, None]:
        self.get_new_sas_token()
        if self.sas_token:
            blob_path_with_token: str = f"{blob_path}?{self.sas_token['sas_token']}"
            download_output = self.run_az_copy(
                blob_path=blob_path_with_token, output_path=output_path)
            output_list = download_output.stdout.decode('utf-8').splitlines()
            json_list = [json.loads(obj) for obj in output_list]
            return json_list
        return None


if __name__ == "__main__":
    args = get_args()
    token = Token(cloud='gcp')
    request_util = RunRequest(token=token)
    tdr_client = TDR(request_util=request_util)
    gcp_storage_client = storage.Client()
    gcp_bucket = gcp_storage_client.bucket(args.bucket_id)
    export_info = {'endpoint': args.export_type, 'id': args.target_id}

    if args.export_type == 'dataset':
        file_list = tdr_client.get_data_set_files(
            dataset_id=args.target_id, batch_query=False)
    elif args.export_type == 'snapshot':
        file_list = tdr_client.get_files_from_snapshot(
            snapshot_id=args.target_id)

    download_client = DownloadAzBlob(
        export_info=export_info, tdr_client=tdr_client)
    for file in file_list:
        access_url = file["fileDetail"]["accessUrl"]
        download_path = f"/tmp/{Path(access_url).name}"
        file_download = download_client.run(
            blob_path=access_url, output_path=download_path)
        file_name = Path(access_url).name
        # construct upload path
        if args.retain_path_structure:
            gcp_upload_path = file["path"]
        elif args.bucket_output_path:
            formatted_path = Path(args.bucket_output_path) / file_name
            gcp_upload_path = str(formatted_path)
        else:
            gcp_upload_path = file_name
        breakpoint()
        logging.info(f"Uploading {file_name} to {gcp_upload_path}")
        upload_blob = gcp_bucket.blob(gcp_upload_path)
        upload_blob.upload_from_filename(download_path)
        # cleanup file before next iteration
        Path(download_path).unlink()
