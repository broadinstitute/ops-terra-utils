from azure.storage.blob import BlobClient
from azure.core.credentials import AzureSasCredential
from google.cloud import storage
import google.cloud.logging

from argparse import ArgumentParser, Namespace
from utils import TDR, RunRequest, Token
from datetime import datetime, timedelta, timezone
import logging
import json
import subprocess
from pathlib import Path


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

google_logging_client = google.cloud.logging.Client()
google_logging_client.setup_logging()


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="""For deletion of on prem aggregations for input samples""")
    parser.add_argument("-t", "--export_type", required=True,
                        help="Target to export from TDR, either entire dataset or snapshot", choices=['dataset', 'snapshot'])
    parser.add_argument("-id", "--target_id", required=True,
                        help="ID of dataset or snapshot to export")
    parser.add_argument("-b", "--bucket_id", required=True,
                        help="Google bucket to export data to")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-op", "--bucket_output_path", required=False,
                        help="Directory to upload files to within google bucket, cannot be used alongside retain_path_structure option")
    group.add_argument("-rps", "--retain_path_structure", required=False, default=False,
                        help="Option to keep path structure set in TDR from path attribute")
    return parser.parse_args()




#DATASET_ID = "34f9c0d5-3a78-4e7d-85b5-2089280ff87a"
#SNAPSHOT_ID = ""
#GOOGLE_BUCKET = "fc-912e0ea1-de65-4c99-93c0-2b914063ba22"
#MOUNT_PATH = "/mnt/mount_test"

class DownloadAzBlob:

    def __init__(self, export_info: dict, tdr_client): 
        self.tdr_client = tdr_client
        self.export_info = export_info
        self.sas_token = None

    def time_until_token_expiry(self):
        token_expiry = datetime.fromisoformat(self.sas_token['expiry_time'])
        current_time = datetime.now(timezone.utc)
        time_delta = token_expiry - current_time
        return time_delta		

    def get_new_sas_token(self):
        logging.info("Obtaining new sas token")
        if self.export_info['endpoint'] == 'dataset':
            self.sas_token = self.tdr_client.get_sas_token(dataset_id=self.export_info['id'])
        elif self.export_info['endpoint'] == 'snapshot':
            self.sas_token = self.tdr_client.get_sas_token(snapshot_id=self.export_info['id'])

    def run_az_copy(self, blob_path: str, output_path: str):
        az_copy_command = ['azcopy', 'copy', f"{blob_path}", f"{output_path}", '--output-type=json']
        copy_cmd = subprocess.run(az_copy_command, capture_output=True)
        return copy_cmd

    def run(self, blob_path: str, output_path: str):
        self.get_new_sas_token()
        blob_path_with_token = f"{blob_path}?{self.sas_token['sas_token']}"
        download_output = self.run_az_copy(blob_path=blob_path_with_token, output_path=output_path)
        output_list = download_output.stdout.decode('utf-8').splitlines()
        json_list = [json.loads(obj) for obj in output_list]
        return output_list


def format_download_output(output_list: list):
    pass

    
if __name__ == "__main__":
    args = get_args()
    token = Token(cloud='gcp')
    request_util = RunRequest(token=token)
    tdr_client = TDR(request_util=request_util)
    gcp_storage_client = storage.Client()
    gcp_bucket = gcp_storage_client.bucket(args.bucket_id)
    export_info = {'endpoint': args.export_type, 'id': args.target_id}

    if args.export_type == 'dataset':
        file_list = tdr_client.get_data_set_files(dataset_id=args.target_id)
    elif args.export_type == 'snapshot':
        file_list = tdr_client.get_snapshot_files(snapshot_id=args.target_id)


    download_client = DownloadAzBlob(export_info=export_info, tdr_client=tdr_client)
    for file in file_list:
        access_url = file['fileDetail']['accessUrl']
        file_download = download_client.run(blob_path=access_url ,output_path='/tmp/')
        file_download.stdout.split(',')
        breakpoint()
        file_name = Path(access_url).name
        if args.retain_path_structure:
            gcp_upload_path = file['path']
        else:
            output_path = f"/tmp/{file_name}"
            gcp_upload_path = args.bucket_output_path if args.bucket_output_path else file_name
        logging.info(f"Uploading {file_name} to {gcp_upload_path}")
        upload_blob = gcp_bucket.blob(gcp_upload_path)
        upload_blob.upload_from_filename(output_path)
        #cleanup file beore next iteration
        Path(output_path).unlink()

        