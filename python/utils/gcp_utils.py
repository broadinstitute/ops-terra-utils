import os
import logging
from mimetypes import guess_type
from .thread_pool_executor_util import MultiThreadedJobs

MOVE = 'move'
COPY = 'copy'

class GCPCloudFunctions:
    """List contents of a GCS bucket. Does NOT take in a token and auths as current user"""
    def __init__(self):
        from google.cloud import storage
        from google.auth import default
        credentials, project = default()
        self.client = storage.Client(credentials=credentials, project=project)

    @staticmethod
    def process_cloud_path(cloud_path: str) -> dict:
        platform_prefix, remaining_url = str.split(str(cloud_path), '//')
        bucket_name = str.split(remaining_url, '/')[0]
        blob_name = "/".join(str.split(remaining_url, '/')[1:])

        path_components = {'platform_prefix': platform_prefix, 'bucket': bucket_name, 'blob_url': blob_name}
        return path_components

    def list_bucket_contents(self, bucket_name: str, file_extensions_to_ignore: list[str] = [],
                             file_strings_to_ignore: list[str] = []) -> list[dict]:
        logging.info(f"Listing contents of bucket gs://{bucket_name}/")
        bucket = self.client.get_bucket(bucket_name)
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

    def copy_cloud_file(self, src_cloud_path: str, full_destination_path: str) -> None:
        source_file_path_components = self.process_cloud_path(src_cloud_path)
        destination_file_path_components = self.process_cloud_path(full_destination_path)

        try:
            source_bucket = self.client.get_bucket(source_file_path_components['bucket'])
            destination_bucket = self.client.get_bucket(destination_file_path_components['bucket'])

            src_blob = source_bucket.blob(source_file_path_components['blob_url'])
            dest_blob = destination_bucket.blob(destination_file_path_components['blob_url'])

            # Use rewrite so no timeouts
            rewrite_token = False

            while True:
                rewrite_token, bytes_rewritten, bytes_to_rewrite = dest_blob.rewrite(
                    src_blob, token=rewrite_token
                )
                print(f"{full_destination_path}: Progress so far: {bytes_rewritten}/{bytes_to_rewrite} bytes.")
                if not rewrite_token:
                    break

        except Exception as e:
            logging.error(f"Error copying file from {src_cloud_path} to {full_destination_path}: {e}")
            raise

    def delete_cloud_file(self, full_cloud_path: str) -> None:
        file_path_components = self.process_cloud_path(full_cloud_path)
        bucket = self.client.get_bucket(file_path_components['bucket'])
        blob = bucket.blob(file_path_components['blob_url'])
        blob.delete()

    def move_cloud_file(self, src_cloud_path: str, full_destination_path: str) -> None:
        self.copy_cloud_file(src_cloud_path, full_destination_path)
        self.delete_cloud_file(src_cloud_path)

    def get_filesize(self, target_path: str) -> int:
        source_file_path_components = self.process_cloud_path(target_path)
        source_bucket = self.client.get_bucket(source_file_path_components['bucket'])
        target = source_bucket.get_blob(source_file_path_components['blob_url'])
        size = target.size
        return size

    def validate_files_are_same(self, src_cloud_path: str, dest_cloud_path: str) -> bool:
        """Validate if two cloud files (source and destination) are identical based on their MD5 hashes."""
        src_file_path_components = self.process_cloud_path(src_cloud_path)
        dest_file_path_components = self.process_cloud_path(dest_cloud_path)

        src_bucket = self.client.get_bucket(src_file_path_components['bucket'])
        dest_bucket = self.client.get_bucket(dest_file_path_components['bucket'])

        src_blob = src_bucket.get_blob(src_file_path_components['blob_url'])
        dest_blob = dest_bucket.get_blob(dest_file_path_components['blob_url'])

        if not src_blob or not dest_blob:
            logging.error(f"One or both of the files do not exist: {src_cloud_path}, {dest_cloud_path}")
            return False

        if src_blob.md5_hash == dest_blob.md5_hash:
            logging.info(f"Files are identical: {src_cloud_path} and {dest_cloud_path}")
            return True
        else:
            logging.warning(f"Files differ: {src_cloud_path} and {dest_cloud_path}")
            return False

    def delete_multiple_files(self, files_to_delete: list[str], workers: int = 5, max_retries: int = 3) -> None:
        """Deletes multiple cloud files in parallel using multi-threading."""
        # Create the list of lists for delete job
        list_of_jobs_args_list = [[file_path] for file_path in files_to_delete]

        # Run the multi-threaded deletion
        MultiThreadedJobs().run_multi_threaded_job_with_no_output(
            workers=workers,
            function=self.delete_cloud_file,
            list_of_jobs_args_list=list_of_jobs_args_list,
            max_retries=max_retries,
            fail_on_error=True
        )

    def multithread_copy_of_files_with_validation(self, files_to_move: list[dict], workers: int, max_retries: int) -> None:
        """files_to_move_dict is list of dicts that contain {source_file: gs://bucket/file, full_destination_path: gs://new_bucket/file_path}
        action must either be move or copy"""
        updated_file_to_move = []
        logging.info(f"Checking if {len(files_to_move)} files to copy have already been copied")
        for file_dict in files_to_move:
            # Check if already copied and if so don't try to move again
            if not self.validate_files_are_same(file_dict['source_file'], file_dict['full_destination_path']):
                updated_file_to_move.append(file_dict)
        logging.info(f"Attempting to {COPY} {len(updated_file_to_move)} files")
        self.move_or_copy_multiple_files(updated_file_to_move, COPY, workers, max_retries)
        logging.info(f"Validating all {len(updated_file_to_move)} new files are identical to original")
        copy_valid = True
        for file_dict in updated_file_to_move:
            # Check if copy and original are identical. If not then not valid.
            if not self.validate_files_are_same(file_dict['source_file'], file_dict['full_destination_path']):
                logging.error(f"File {file_dict['source_file']} and {file_dict['full_destination_path']} are not identical")
                copy_valid = False
        if copy_valid:
            logging.info(f"Successfully copied {len(updated_file_to_move)} files")
        else:
            logging.error(f"Failed to copy {len(updated_file_to_move)} files")
            raise Exception("Failed to copy all files")

    def move_or_copy_multiple_files(self, files_to_move: list[dict], action: str, workers: int, max_retries: int) -> None:
        """files_to_move_dict is list of dicts that contain {source_file: gs://bucket/file, full_destination_path: gs://new_bucket/file_path}
        action must either be move or copy"""
        if action == MOVE:
            cloud_function = self.move_cloud_file
        elif action == COPY:
            cloud_function = self.copy_cloud_file
        else:
            raise Exception("Must either select move or copy")

        # Test just one copy to see if it works
        one_file = files_to_move[0]
        # TODO: REMOVE THIS AFTER COPY WORKS
        print(one_file['source_file'])
        print(one_file['full_destination_path'])
        self.copy_cloud_file(one_file['source_file'], one_file['full_destination_path'])
        sys.exit()

        list_of_jobs_args_list = [
            [
                file_dict['source_file'], file_dict['full_destination_path']
            ]
            for file_dict in files_to_move
        ]
        MultiThreadedJobs().run_multi_threaded_job_with_no_output(
            workers=workers,
            function=cloud_function,
            list_of_jobs_args_list=list_of_jobs_args_list,
            max_retries=max_retries,
            fail_on_error=True
        )
