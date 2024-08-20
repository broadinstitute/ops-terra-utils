from azure.storage.blob import BlobClient
from utils import TDR, RunRequest, Token
from datetime import datetime, timedelta, timezone
import logging
import json
import subprocess
from pathlib import Path


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

azure_logger = logging.getLogger('azure.storage').setLevel(logging.WARNING)


DATASET_ID = "34f9c0d5-3a78-4e7d-85b5-2089280ff87a"
SNAPSHOT_ID = ""
GOOGLE_BUCKET = "fc-912e0ea1-de65-4c99-93c0-2b914063ba22"
MOUNT_PATH = "/mnt/mount_test"
#blob_client = BlobClient.from_blob_url("https://tdreeevicueituopmfmwfoye.blob.core.windows.net/34f9c0d5-3a78-4e7d-85b5-2089280ff87a/data/ae6b2351-a112-436c-a0c6-e1b1174b3798/Untitled.ipynb?sv=2024-08-04&spr=https&se=2024-08-15T19%3A02%3A06Z&sp=rl&sig=eaFJACGs6r866IUzc28hUtAfXQHfZCP6rjPy7ChQ2QU%3D&sr=c&rscd=jscira%40broadinstitute.org")

def create_gcsfuse_dir(bucket_name, mount_path):
	subprocess.run(['gcsfuse', bucket_name, mount_path], check=True)

def token_needs_refresh(token_expiry_time):
	token_expiry = datetime.fromisoformat(token_expiry_time)
	current_time = datetime.now(timezone.utc)
	time_delta = token_expiry - current_time
	if time_delta.total_seconds() < 1200:
		return True
	else:
		return False

def write_file_to_gcs_mount(file_path: str, output_path: str):
	blob_client = BlobClient.from_blob_url(file_path)
	with open(output_path, mode="wb") as sample_file:
		download_stream = blob_client.download_blob()
		sample_file.write(download_stream.readall())
	
if __name__ == "__main__":
	token = Token(cloud='gcp')
	request_util = RunRequest(token=token)
	tdr_client = TDR(request_util=request_util)
	create_gcsfuse_dir(GOOGLE_BUCKET, MOUNT_PATH)
	file_list = tdr_client.get_data_set_files(dataset_id=DATASET_ID)
	sas_token = tdr_client.get_sas_token(dataset_id='34f9c0d5-3a78-4e7d-85b5-2089280ff87a')
	for file in file_list:
		if token_needs_refresh(sas_token['expiry_time']):
			sas_token = tdr_client.get_sas_token(dataset_id=DATASET_ID)
		access_url = file['fileDetail']['accessUrl']
		file_path_with_sas_token = f"{access_url}?{sas_token['sas_token']}"
		file_name = Path(access_url).name
		output_path = f"{MOUNT_PATH}/{file_name}"
		write_file_to_gcs_mount(file_path=file_path_with_sas_token, output_path=output_path)

