import os
import logging
from mimetypes import guess_type

from schema import Optional

from .thread_pool_executor_util import MultiThreadedJobs

MOVE = "move"
COPY = "copy"


class GCPCloudFunctions:
    """List contents of a GCS bucket. Does NOT take in a token and auths as current user"""

    def __init__(self) -> None:
        """
        Initialize the GCPCloudFunctions class.
        Authenticates using the default credentials and sets up the storage client.
        """
        from google.cloud import storage
        from google.auth import default
        credentials, project = default()
        self.client = storage.Client(credentials=credentials, project=project)

    @staticmethod
    def process_cloud_path(cloud_path: str) -> dict:
        """
        Process a GCS cloud path into its components.

        Args:
            cloud_path (str): The GCS cloud path.

        Returns:
            dict: A dictionary containing the platform prefix, bucket name, and blob URL.
        """
        platform_prefix, remaining_url = str.split(str(cloud_path), sep="//")
        bucket_name = str.split(remaining_url, sep="/")[0]
        blob_name = "/".join(str.split(remaining_url, sep="/")[1:])
        path_components = {
            "platform_prefix": platform_prefix,
            "bucket": bucket_name,
            "blob_url": blob_name
        }
        return path_components

    def list_bucket_contents(self, bucket_name: str, file_extensions_to_ignore: list[str] = [],
                             file_strings_to_ignore: list[str] = []) -> list[dict]:
        """
        List contents of a GCS bucket and return a list of dictionaries with file information.

        Args:
            bucket_name (str): The name of the GCS bucket.
            file_extensions_to_ignore (list[str], optional): List of file extensions to ignore. Defaults to [].
            file_strings_to_ignore (list[str], optional): List of file name substrings to ignore. Defaults to [].

        Returns:
            list[dict]: A list of dictionaries containing file information.
        """
        logging.info(f"Listing contents of bucket gs://{bucket_name}/")
        blobs = self.client.list_blobs(bucket_name)

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
                "path": f"gs://{bucket_name}/{blob.name}",
                "content_type": blob.content_type or guess_type(blob.name)[0] or "application/octet-stream",
                "file_extension": os.path.splitext(blob.name)[1],
                "size_in_bytes": blob.size,
                "md5_hash": blob.md5_hash
            }
            file_list.append(file_info)
        logging.info(f"Found {len(file_list)} files in bucket")
        return file_list

    def copy_cloud_file(self, src_cloud_path: str, full_destination_path: str) -> None:
        """
        Copy a file from one GCS location to another.

        Args:
            src_cloud_path (str): The source GCS path.
            full_destination_path (str): The destination GCS path.
        """
        source_file_path_components = self.process_cloud_path(src_cloud_path)
        destination_file_path_components = self.process_cloud_path(full_destination_path)

        try:
            src_bucket = source_file_path_components["bucket"]
            src_blob_url = source_file_path_components["blob_url"]
            dest_bucket = destination_file_path_components["bucket"]
            dest_blob_url = destination_file_path_components["blob_url"]
            src_blob = self.client.bucket(src_bucket).blob(src_blob_url)
            dest_blob = self.client.bucket(dest_bucket).blob(dest_blob_url)

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
        """
        Delete a file from GCS.

        Args:
            full_cloud_path (str): The GCS path of the file to delete.
        """
        file_path_components = self.process_cloud_path(full_cloud_path)
        blob = self.client.bucket(file_path_components["bucket"]).blob(file_path_components["blob_url"])
        blob.delete()

    def move_cloud_file(self, src_cloud_path: str, full_destination_path: str) -> None:
        """
        Move a file from one GCS location to another.

        Args:
            src_cloud_path (str): The source GCS path.
            full_destination_path (str): The destination GCS path.
        """
        self.copy_cloud_file(src_cloud_path, full_destination_path)
        self.delete_cloud_file(src_cloud_path)

    def get_filesize(self, target_path: str) -> int:
        """
        Get the size of a file in GCS.

        Args:
            target_path (str): The GCS path of the file.

        Returns:
            int: The size of the file in bytes.
        """
        source_file_path_components = self.process_cloud_path(target_path)
        target = self.client.bucket(
            source_file_path_components["bucket"]
        ).get_blob(source_file_path_components["blob_url"])

        size = target.size
        return size

    def validate_files_are_same(self, src_cloud_path: str, dest_cloud_path: str) -> bool:
        """
        Validate if two cloud files (source and destination) are identical based on their MD5 hashes.

        Args:
            src_cloud_path (str): The source GCS path.
            dest_cloud_path (str): The destination GCS path.

        Returns:
            bool: True if the files are identical, False otherwise.
        """
        src_file_path_components = self.process_cloud_path(src_cloud_path)
        dest_file_path_components = self.process_cloud_path(dest_cloud_path)

        src_blob = self.client.bucket(src_file_path_components["bucket"]).get_blob(src_file_path_components["blob_url"])
        dest_blob = self.client.bucket(
            dest_file_path_components["bucket"]
        ).get_blob(dest_file_path_components["blob_url"])

        # If either blob is None, return False
        if not src_blob or not dest_blob:
            return False
        # If the MD5 hashes of the two blobs are the same, return True
        if src_blob.md5_hash == dest_blob.md5_hash:
            return True
        # Otherwise, return False
        else:
            return False

    def delete_multiple_files(
            self,
            files_to_delete: list[str],
            workers: int = 5,
            max_retries: int = 3,
            verbose: bool = False
    ) -> None:
        """
        Delete multiple cloud files in parallel using multi-threading.

        Args:
            files_to_delete (list[str]): List of GCS paths of the files to delete.
            workers (int, optional): Number of worker threads. Defaults to 5.
            max_retries (int, optional): Maximum number of retries. Defaults to 3.
            verbose (bool, optional): Whether to log each job's success. Defaults to False.
        """
        list_of_jobs_args_list = [[file_path] for file_path in files_to_delete]

        MultiThreadedJobs().run_multi_threaded_job(
            workers=workers,
            function=self.delete_cloud_file,
            list_of_jobs_args_list=list_of_jobs_args_list,
            max_retries=max_retries,
            fail_on_error=True,
            verbose=verbose,
            collect_output=False
        )

    def validate_file_pair(self, source_file: str, full_destination_path: str) -> Optional[dict]:
        """
        Helper function to validate if source and destination files are identical.

        Args:
            source_file (str): The source file path.
            full_destination_path (str): The destination file path.

        Returns:
            dict: The file dictionary if the files are not identical, otherwise None.
        """
        if not self.validate_files_are_same(source_file, full_destination_path):
            return {"source_file": source_file, "full_destination_path": full_destination_path}
        return None

    def loop_and_log_validation_files_multithreaded(
            self,
            files_to_validate: list[dict],
            log_difference: bool,
            workers: int = 5,
            max_retries: int = 3
    ) -> list[dict]:
        """
        Validate if multiple cloud files are identical based on their MD5 hashes using multithreading.

        Args:
            files_to_validate (list[Dict]): List of dictionaries containing source and destination file paths.
            log_difference (bool): Whether to log differences if files are not identical. Set false if you are running
                                   this at the start of a copy/move operation to check if files are already copied.
            workers (int, optional): Number of worker threads. Defaults to 5.
            max_retries (int, optional): Maximum number of retries for all jobs. Defaults to 3.

        Returns:
            list[Dict]: List of dictionaries containing files that are not identical.
        """
        logging.info(f"Validating if {len(files_to_validate)} files are identical")

        # Prepare jobs: pass the necessary arguments to each validation
        jobs = [(file_dict['source_file'], file_dict['full_destination_path']) for file_dict in files_to_validate]

        # Use multithreaded job runner to validate the files
        not_valid_files = MultiThreadedJobs().run_multi_threaded_job(
            workers=workers,
            function=self.validate_file_pair,
            list_of_jobs_args_list=jobs,
            collect_output=True,
            max_retries=max_retries
        )

        # If only here so linting will be happy
        if not_valid_files:
            # Filter out any None results (which represent files that are identical)
            not_valid_files = [file_dict for file_dict in not_valid_files if file_dict is not None]
            if not_valid_files:
                if log_difference:
                    for file_dict in not_valid_files:
                        logging.warning(
                            f"File {file_dict['source_file']} and {file_dict['full_destination_path']} are not identical"
                        )
                logging.info(f"Validation complete. {len(not_valid_files)} files are not identical.")
                return not_valid_files
        # If all files are identical, return an empty list
        return []

    def multithread_copy_of_files_with_validation(
            self, files_to_move: list[dict], workers: int, max_retries: int
    ) -> None:
        """
        Copy multiple files in parallel with validation.

        Args:
            files_to_move (list[dict]): List of dictionaries containing source and destination file paths.
            workers (int): Number of worker threads.
            max_retries (int): Maximum number of retries.
        """
        updated_files_to_move = self.loop_and_log_validation_files_multithreaded(
            files_to_move,
            log_difference=False,
            workers=workers,
            max_retries=max_retries
        )
        # If all files are already copied, return
        if not updated_files_to_move:
            logging.info("All files are already copied")
            return None
        logging.info(f"Attempting to {COPY} {len(updated_files_to_move)} files")
        self.move_or_copy_multiple_files(updated_files_to_move, COPY, workers, max_retries)
        logging.info(f"Validating all {len(updated_files_to_move)} new files are identical to original")
        # Validate that all files were copied successfully
        files_not_moved_successfully = self.loop_and_log_validation_files_multithreaded(
            files_to_move,
            workers=workers,
            log_difference=True,
            max_retries=max_retries
        )
        if files_not_moved_successfully:
            logging.error(f"Failed to copy {len(files_not_moved_successfully)} files")
            raise Exception("Failed to copy all files")
        logging.info(f"Successfully copied {len(updated_files_to_move)} files")
        return None

    def move_or_copy_multiple_files(
            self, files_to_move: list[dict], action: str, workers: int, max_retries: int, verbose: bool = False
    ) -> None:
        """
        Move or copy multiple files in parallel.

        Args:
            files_to_move (list[dict]): List of dictionaries containing source and destination file paths.
            action (str): The action to perform ('move' or 'copy').
            workers (int): Number of worker threads.
            max_retries (int): Maximum number of retries.
            verbose (bool, optional): Whether to log each job's success. Defaults to False.

        Raises:
            Exception: If the action is not 'move' or 'copy'.
        """
        if action == MOVE:
            cloud_function = self.move_cloud_file
        elif action == COPY:
            cloud_function = self.copy_cloud_file
        else:
            raise Exception("Must either select move or copy")

        list_of_jobs_args_list = [
            [
                file_dict['source_file'], file_dict['full_destination_path']
            ]
            for file_dict in files_to_move
        ]
        MultiThreadedJobs().run_multi_threaded_job(
            workers=workers,
            function=cloud_function,
            list_of_jobs_args_list=list_of_jobs_args_list,
            max_retries=max_retries,
            fail_on_error=True,
            verbose=verbose,
            collect_output=False
        )
