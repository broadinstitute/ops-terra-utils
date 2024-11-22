import logging
from argparse import ArgumentParser, Namespace
from pathlib import Path
from utils.azure_utils import AzureBlobDetails, SasTokenUtil
from utils.terra_utils.terra_util import TerraWorkspace
from utils.gcp_utils import GCPCloudFunctions
from utils.request_util import RunRequest
from utils.token_util import Token


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="""Copy files from Azure terra workspace to GCP bucket""")
    subparsers = parser.add_subparsers()
    full_workspace_export = subparsers.add_parser("full_workspace_export")
    full_workspace_export.add_argument("-w", "--workspace_name", required=True,
                        help="terra workspace name")
    full_workspace_export.add_argument("-b", "--billing_project", required=True,
                        help="Terra billing project name")
    full_workspace_export.add_argument("-bucket", "--gcp_bucket", required=True,
                        help="GCP bucket id")
    filtered_workspace_export = subparsers.add_parser("filtered_workspace_export")
    filtered_workspace_export.add_argument("-t", "--input_tsv", required=True,
                                           help="tsv file with the following columns: workspace_name, billing_project, directory_path, export_bucket")

    parser.add_argument("-t", "--tmp_path", required=False,
                        help="temp path to use. Defaults to ./tmp", default="./tmp")

    return parser.parse_args()


class AzureToGoogleFileTransfer():
    def __init__(self, blob_list, gcp_bucket, gcp_client, az_accnt_url, az_container, workspace_client, temp_dir):
        self.blob_list = blob_list
        self.export_bucket = gcp_bucket
        self.gcp_client = gcp_client
        self.az_accnt_url = az_accnt_url
        self.az_container = az_container
        self.workspace_client = workspace_client
        self.temp_dir = temp_dir
        self.sas_token = None

    def format_upload_path(self, blob_path):
        bucket_str = self.export_bucket
        if not bucket_str.startswith('gs://'):
            bucket_str = f"gs://{bucket_str}"
        if not bucket_str.endswith('/'):
            bucket_str = f"{bucket_str}/"
        return f"{bucket_str}{blob_path}"

    def blob_exists(self, upload_path):
        return GCPCloudFunctions().get_blob_details(upload_path)

    def delete_file_after_transfer(self, file_path):
        try:
            file_path.unlink()
        except Exception as e:
            logging.error(f"Error deleting file {file_path}: {e}")

    def run(self):
        self.sas_token = self.workspace_client.retrieve_sas_token(2400)
        token_util = SasTokenUtil(token=self.sas_token)
        tmp_dir = Path(self.temp_dir)
        for blob in self.blob_list:
            if token_util.seconds_until_token_expires() < 600:
                self.sas_token = self.workspace_client.retrieve_sas_token(2400)
            upload_path = self.format_upload_path(blob['relative_path'])
            if not self.blob_exists(upload_path):
                dl_path = tmp_dir.joinpath(blob['file_name'])
                blob_client = AzureBlobDetails(account_url=self.az_accnt_url,
                                               sas_token=self.sas_token,
                                               container_name=self.az_container)
                blob_client.download_blob(blob_name=blob['relative_path'], dl_path=dl_path)
                self.gcp_client.upload_blob(destination_path=upload_path, source_file=dl_path)
                self.delete_file_after_transfer(dl_path)

        tmp_dir.rmdir()


if __name__ == "__main__":
    args = get_args()
    breakpoint()
    token = Token(cloud='gcp')
    request_util = RunRequest(token)

    workspace_client = TerraWorkspace(workspace_name=args.workspace_name,
                                      billing_project=args.billing_project,
                                      request_util=request_util)
    workspace_client.set_azure_terra_variables()
    gcp_client = GCPCloudFunctions()
    sas_token = workspace_client.retrieve_sas_token(600)
    az_storage_container_id = workspace_client.account_url.strip('.blob.core.windows.net').strip('https://')
    az_blob_client = AzureBlobDetails(account_url=workspace_client.account_url,
                                      sas_token=sas_token,
                                      container_name=workspace_client.storage_container)
    az_blobs = az_blob_client.get_blob_details()
    AzureToGoogleFileTransfer(blob_list=az_blobs,
                              gcp_bucket=args.gcp_bucket,
                              gcp_client=gcp_client,
                              az_accnt_url=workspace_client.account_url,
                              az_container=workspace_client.storage_container,
                              workspace_client=workspace_client,
                              temp_dir=args.tmp_path).run()
