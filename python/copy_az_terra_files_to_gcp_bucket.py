import logging
import csv
from argparse import ArgumentParser, Namespace
from pathlib import Path
from utils.azure_utils import AzureBlobDetails, SasTokenUtil
from utils.terra_utils.terra_util import TerraWorkspace
from utils.gcp_utils import GCPCloudFunctions
from utils.request_util import RunRequest
from utils.token_util import Token
from utils.csv_util import Csv
from azure.core.exceptions import ClientAuthenticationError

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

az_logging = logging.getLogger('azure')
az_logging.setLevel(logging.ERROR)



def get_args() -> Namespace:
    parser = ArgumentParser(
        description="""Copy files from Azure terra workspace to GCP bucket""")
    parser.add_argument("-sw", "--source_workspace", required=True, help="")
    parser.add_argument("-sb", "--source_billing_project", required=True, help="")
    parser.add_argument("-ew", "--export_workspace_name", required=True, help="")
    parser.add_argument("-asp", "--azure_source_path", required=False, help="")
    parser.add_argument("-gb", "--gcp_export_bucket", required=True, help="")
    parser.add_argument("-t", "--tmp_path", required=False,
                        help="temp path to use. Defaults to /tmp", default="/tmp_mnt")

    return parser.parse_args()


class RunExport():
    def __init__(self, gcp_client, workspace_client, temp_dir):
        self.gcp_client = gcp_client
        self.workspace_client = workspace_client
        self.sas_token = None
        self.temp_dir = temp_dir

    def _format_upload_path(self, blob_path, export_bucket):
        bucket_str = export_bucket
        if not bucket_str.startswith('gs://'):
            bucket_str = f"gs://{bucket_str}"
        if not bucket_str.endswith('/'):
            bucket_str = f"{bucket_str}/"
        return f"{bucket_str}{blob_path}"
    
    def _blob_exists(self, upload_path):
         blob = GCPCloudFunctions().load_blob_from_full_path(upload_path)
         return blob.exists()
    
    def _delete_file(self, file_path):
        try:
            file_path.unlink()
        except Exception as e:
            logging.error(f"Error deleting file {file_path}: {e}")


    def blob_download(self, blob_client, blob_name, dl_path):
        while True: 
            try:
                blob_client.chunk_blob_download(blob_name=blob_name, dl_path=dl_path)
            except ClientAuthenticationError:
                new_token = self.workspace_client.retrieve_sas_token(3000)
                blob_client.update_sas_token(new_token)

    def run(self, blob_list, export_bucket):
        self.sas_token = self.workspace_client.retrieve_sas_token(2400)
        token_util = SasTokenUtil(token=self.sas_token)
        tmp_dir = Path(self.temp_dir)
        upload_paths = []
        blob_len = len(blob_list)
        progress_count = 0
        for blob in blob_list:
            if token_util.seconds_until_token_expires() < 1200:
                self.sas_token = self.workspace_client.retrieve_sas_token(3000)
            upload_path = self._format_upload_path(blob['relative_path'], export_bucket)
            upload_paths.append(upload_path)
            if not self._blob_exists(upload_path):
                blob_name = Path(blob['file_path']).name
                dl_path = tmp_dir.joinpath(blob_name)
                blob_client = AzureBlobDetails(account_url=self.workspace_client.account_url,
                                            sas_token=self.sas_token,
                                            container_name=self.workspace_client.storage_container)
                progress_count += 1
                logging.info(f"Downloading blob {progress_count} of {blob_len}")
                logging.info(f"Downloading blob {blob_name} to {dl_path}")
                #self.blob_download(blob_client=blob_client, blob_name=blob['relative_path'], dl_path=dl_path)
                output = blob_client.dl_blob_with_az_copy(blob_path=blob['file_path'], dl_path=dl_path)
                logging.info(f"Download output: {output}") 
                logging.info(f"Uploading {dl_path} to {upload_path}")
                self.gcp_client.upload_blob(destination_path=upload_path, source_file=dl_path)
                self._delete_file(dl_path)
            else: 
                progress_count += 1
                logging.info(f"Blob {upload_path} already exists in GCP bucket")


class Manifest():
    def __init__(self, export_dict):
        self.export_dict = export_dict

    def construct_manifest(self):
        manifest_list = []
        blob_list = self.export_dict['blob_list']
        for blob in blob_list:
            manifest_list.append({
                "file_name": blob['file_name'],
                "full_path": blob['file_path'],
                "export_bucket": f"{self.export_dict['gcp_export_bucket']}"
            })
        return manifest_list

    def write_manifest(self, output_path):
        manifest_list = self.construct_manifest()
        Csv(file_path=output_path, delimiter=',').create_tsv_from_list_of_dicts(manifest_list)
    
if __name__ == "__main__":
    args = get_args()
    token = Token(cloud='gcp')
    request_util = RunRequest(token)
    gcp_client = GCPCloudFunctions()


    workspace_client = TerraWorkspace(workspace_name=args.source_workspace,
                            billing_project=args.source_billing_project,
                            request_util=request_util)
    workspace_client.set_azure_terra_variables()
    sas_token = workspace_client.retrieve_sas_token(2400)
    az_blob_client = AzureBlobDetails(account_url=workspace_client.account_url,
                sas_token=sas_token,
                container_name=workspace_client.storage_container)
    az_blobs = az_blob_client.get_blob_details(max_per_page=1000)
    export_blobs = []
    for blob in az_blobs:
        if args.azure_source_path:
            if blob['file_path'].startswith(args.azure_source_path):
                export_blobs.append(blob)  
        else:
            export_blobs.append(blob)
    
    RunExport(gcp_client=gcp_client,workspace_client=workspace_client,
            temp_dir=args.tmp_path).run(blob_list=export_blobs, export_bucket=args.gcp_export_bucket)
