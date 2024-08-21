from azure.storage.blob import BlobClient
from azure.core.credentials import AzureSasCredential
from utils import TDR, RunRequest, Token
from datetime import datetime, timedelta, timezone
import logging
import json
import subprocess
from pathlib import Path


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

blob_logger = logging.getLogger('azure.storage.blob').setLevel(logging.WARNING)
az_logger = logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)



DATASET_ID = "34f9c0d5-3a78-4e7d-85b5-2089280ff87a"
SNAPSHOT_ID = ""
GOOGLE_BUCKET = "fc-912e0ea1-de65-4c99-93c0-2b914063ba22"
MOUNT_PATH = "/mnt/mount_test"
#blob_client = BlobClient.from_blob_url("https://tdreeevicueituopmfmwfoye.blob.core.windows.net/34f9c0d5-3a78-4e7d-85b5-2089280ff87a/data/ae6b2351-a112-436c-a0c6-e1b1174b3798/Untitled.ipynb?sv=2024-08-04&spr=https&se=2024-08-15T19%3A02%3A06Z&sp=rl&sig=eaFJACGs6r866IUzc28hUtAfXQHfZCP6rjPy7ChQ2QU%3D&sr=c&rscd=jscira%40broadinstitute.org")

class DownloadAzBlob:

	def __init__(self, export_info: dict, tdr_client): 
		self.tdr_client = tdr_client
		self.export_info = export_info
		self.sas_token = self.get_new_sas_token()

	def time_until_token_expiry(self):
		token_expiry = datetime.fromisoformat(self.sas_token['expiry_time'])
		current_time = datetime.now(timezone.utc)
		time_delta = token_expiry - current_time
		return time_delta		

	def get_new_sas_token(self):
		logging.info("Obtaining new sas token")
		self.sas_token = self.tdr_client.get_sas_token(dataset_id=self.export_info['id'])

	def write_file_to_gcs_mount(self, blob_path: str, output_path: str):
		logging.info(f"Downloading {blob_path} to {output_path}")
		sas_creds = AzureSasCredential(self.sas_token['sas_token'])
		blob_client = BlobClient.from_blob_url(blob_url=blob_path, credential=sas_creds)
		with open(output_path, mode="wb") as sample_file:
			download_stream = blob_client.download_blob(max_concurrency=4)
			#download_stream.readinto(sample_file)
			for chunk in download_stream.chunks():
				if self.time_until_token_expiry().seconds < 300:
					self.sas_token = tdr_client.get_sas_token(dataset_id=DATASET_ID)
					blob_client.credential.update(self.sas_token['sas_token'])
				sample_file.write(chunk)



def create_gcsfuse_dir(bucket_name, mount_path):
	subprocess.run(['gcsfuse', bucket_name, mount_path], check=True)



if __name__ == "__main__":
	token = Token(cloud='gcp')
	request_util = RunRequest(token=token)
	tdr_client = TDR(request_util=request_util)
	create_gcsfuse_dir(GOOGLE_BUCKET, MOUNT_PATH)
	file_list = tdr_client.get_data_set_files(dataset_id=DATASET_ID)
	export_info = {'endpoint': 'dataset', 'id': DATASET_ID}
	download_client = DownloadAzBlob(export_info=export_info, tdr_client=tdr_client)
	for file in file_list:
		access_url = file['fileDetail']['accessUrl']
		file_name = Path(access_url).name
		output_path = f"{MOUNT_PATH}/{file_name}"
		download_client.write_file_to_gcs_mount(blob_path=access_url ,output_path=output_path)
		