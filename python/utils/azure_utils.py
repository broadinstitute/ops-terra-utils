import os
import logging
import base64


class AzureBlobDetails:
    def __init__(self, account_url: str, sas_token: str, container_name: str):
        from azure.storage.blob import BlobServiceClient
        self.account_url = account_url
        self.sas_token = sas_token
        self.container_name = container_name
        self.blob_service_client = BlobServiceClient(
            account_url=self.account_url, credential=self.sas_token)

    def get_blob_details(self, max_per_page: int = 500) -> list[dict]:
        container_client = self.blob_service_client.get_container_client(
            self.container_name)
        details = []

        blob_list = container_client.list_blobs(results_per_page=max_per_page)
        page = blob_list.by_page()

        page_count = 0
        for blob_page in page:
            page_count += 1
            logging.info(
                f"Getting page {page_count} of max {max_per_page} blobs")
            for blob in blob_page:
                blob_client = container_client.get_blob_client(blob)
                props = blob_client.get_blob_properties()
                if not blob.name.endswith('/'):
                    md5_hash = base64.b64encode(props.content_settings.content_md5).decode(
                        'utf-8') if props.content_settings.content_md5 else ""
                    full_path = blob_client.url.replace(
                        f'?{self.sas_token}', '')
                    details.append(
                        {
                            'file_name': blob.name,
                            'file_path': full_path,
                            'content_type': props.content_settings.content_type,
                            'file_extension': os.path.splitext(blob.name)[1],
                            'size_in_bytes': props.size,
                            'md5_hash': md5_hash
                        }
                    )
        return details
