import os
import logging
from mimetypes import guess_type


class GCPCloudFunctions:
    """List contents of a GCS bucket. Does NOT take in a token and auths as current user"""
    def __init__(self, bucket_name: str):
        from google.cloud import storage
        self.bucket_name = bucket_name
        self.client = storage.Client()

    @staticmethod
    def process_cloud_path(cloud_path: str) -> dict:
        platform_prefix, remaining_url = str.split(str(cloud_path), '//')
        bucket_name = str.split(remaining_url, '/')[0]
        blob_name = "/".join(str.split(remaining_url, '/')[1:])

        path_components = {'platform_prefix': platform_prefix, 'bucket': bucket_name, 'blob_url': blob_name}
        return path_components

    def list_bucket_contents(self, file_extensions_to_ignore: list[str] = [],
                             file_strings_to_ignore: list[str] = []) -> list[dict]:
        logging.info(f"Listing contents of bucket gs://{self.bucket_name}/")
        bucket = self.client.get_bucket(self.bucket_name)
        blobs = bucket.list_blobs()

        file_list = []
        for blob in blobs:
            if blob.name.endswith(tuple(file_extensions_to_ignore)):
                logging.info(f"Skipping file {blob.name}")
                continue
            if any(file_string in blob.name for file_string in file_strings_to_ignore):
                logging.info(f"Skipping file {blob.name}")
                continue
            file_info = {
                "name": os.path.basename(blob.name),
                "path": blob.name,
                "content_type": blob.content_type or guess_type(blob.name)[0] or "application/octet-stream",
                "file_extension": os.path.splitext(blob.name)[1],
                "size_in_bytes": blob.size,
                "md5_hash": blob.md5_hash
            }
            file_list.append(file_info)
        logging.info(f"Found {len(file_list)} files in bucket")
        return file_list