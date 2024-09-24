import logging
import json
import subprocess
import csv
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
        az_copy_command = ["azcopy", "copy", f"{blob_path}",
                           f"{output_path}", "--output-type=json"]
        #, "--check-md5=NoCheck"
        copy_cmd = subprocess.run(az_copy_command, capture_output=True)
        return copy_cmd

    def check_copy_completed_successfully(self, output_path: str) -> bool:
        file_exists = Path(output_path).exists()
        return file_exists


    def run(self, blob_path: str, output_path: str) -> tuple(bool, dict[Any, Any]):
        self.get_new_sas_token()
        blob_path_with_token: str = f"{blob_path}?{self.sas_token['sas_token']}"
        download_output = self.run_az_copy(
            blob_path=blob_path_with_token, output_path=output_path)
        output_list = download_output.stdout.decode('utf-8').splitlines()
        json_list = [json.loads(obj) for obj in output_list]
        job_logs = ParseAzCopyOutput().run(copy_logs=json_list)
        copy_completed = self.check_copy_completed_successfully(output_path)
        return copy_completed, job_logs

class ParseAzCopyOutput:

    def _get_copy_logs(self, job_log: dict) -> dict:
        job_dict = {}
        match job_log['MessageType']:
            case 'Init':
                job_dict['LogType'] = 'Init'
                job_dict['Message'] = job_log['MessageContent']
            case 'Progress':
                job_dict['LogType'] = 'Progress'
                job_dict['Message'] = job_log['MessageContent']
            case 'EndOfJob':               
                job_dict['LogType'] = 'EndOfJob'
                job_dict['Message'] = job_log['MessageContent']
            case _:
                pass
        return job_dict

    def run(self, copy_logs: list) -> dict:
        job_metadata = {}
        for log in copy_logs:    
            if log['MessageType'] in ['Init', 'Progress', 'EndOfJob']:
                log_info = self._get_copy_logs(log)            
                job_id = log['JobID'] if log['JobID'] else log['MessageContent']['JobID']
                if not job_metadata.get(job_id):
                    job_metadata[log['JobID']] = {}
                job_metadata[log['JobID']][log['MessageType']] = log_info
        return job_metadata


def construct_upload_path(file, args):
    if args.retain_path_structure:
        gcp_upload_path = file["path"]
    elif args.bucket_output_path:
        formatted_path = Path(args.bucket_output_path) / file_name
        gcp_upload_path = str(formatted_path)
    else:
        file_name = Path(file['fileDetail']['accessUrl']).name
        gcp_upload_path = file_name
    return gcp_upload_path


def write_to_transfer_manifest(file_dict):
    manifest_path = Path('copy_manifest.csv')
    dict_keys = file_dict.keys()
    if not manifest_path.exists():
        with open('copy_manifest.csv', 'w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=dict_keys)
            writer.writeheader()
    with open('copy_manifest.csv', 'a') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=dict_keys)
        writer.writerow(file_dict)



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
            dataset_id=args.target_id)
    elif args.export_type == 'snapshot':
        logging.warning("Snapshot export not yet implemented")
        exit
        #file_list = tdr_client.get_files_from_snapshot(
        #    snapshot_id=args.target_id)

    download_client = DownloadAzBlob(export_info=export_info, tdr_client=tdr_client)
    for file in file_list:
        access_url = file["fileDetail"]["accessUrl"]
        download_path = f"/tmp/{Path(access_url).name}"
        file_download_completed, job_logs = download_client.run(
            blob_path=access_url, output_path=download_path)
        file_name = Path(access_url).name
        md5 = next(checksum for checksum in file["checksums"] if checksum["type"] == "md5")            
        gcp_upload_path = construct_upload_path(file, args)
        copy_info = {
                "source_path": access_url,
                "destination_path": gcp_upload_path,
                "md5": md5["checksum"]                
            }
        if file_download_completed:
            copy_info["download_completed_successfully"] = 'True'            
            logging.info(f"Uploading {file_name} to {gcp_upload_path}")
            upload_blob = gcp_bucket.blob(gcp_upload_path)
            upload_blob.upload_from_filename(download_path)
            upload_completed = 'True' if upload_blob.exists() else 'False'
            copy_info["upload_completed_successfully"] = upload_completed
            write_to_transfer_manifest(copy_info)
            # cleanup file before next iteration
            Path(download_path).unlink()
        else:
            copy_info["download_completed_successfully"] = 'False'
            write_to_transfer_manifest(copy_info)
            logging.error(f"Failed to download {file_name}")