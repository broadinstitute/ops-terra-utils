import os
import logging
from mimetypes import guess_type
from typing import Optional, Any

from .thread_pool_executor_util import MultiThreadedJobs

MOVE = "move"
COPY = "copy"


class GCPCloudFunctions:
    """
    A class to interact with Google Cloud Storage (GCS) for various file operations.
    Authenticates using the default credentials and sets up the storage client.
    Does NOT use Token class for authentication.
    """

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

    @staticmethod
    def _create_bucket_contents_dict(bucket_name: str, blob: Any, file_name_only: bool) -> dict:
        """
        Create a dictionary containing file information.

        Args:
            bucket_name (str): The name of the GCS bucket.
            blob (Any): The GCS blob object.
            file_name_only (bool): Whether to return only the file list.

        Returns:
            dict: A dictionary containing file information.
        """
        if file_name_only:
            return {
                "path": f"gs://{bucket_name}/{blob.name}"
            }
        return {
            "name": os.path.basename(blob.name),
            "path": f"gs://{bucket_name}/{blob.name}",
            "content_type": blob.content_type or guess_type(blob.name)[0] or "application/octet-stream",
            "file_extension": os.path.splitext(blob.name)[1],
            "size_in_bytes": blob.size,
            "md5_hash": blob.md5_hash
        }

    @staticmethod
    def _validate_include_blob(
            blob: Any,
            bucket_name: str,
            file_extensions_to_ignore: list[str] = [],
            file_strings_to_ignore: list[str] = [],
            file_extensions_to_include: list[str] = [],
            verbose: bool = False
    ) -> bool:
        """
        Validate if a blob should be included based on its file extension.

        Args:
            file_extensions_to_include (list[str]): List of file extensions to include.
            file_extensions_to_ignore (list[str]): List of file extensions to ignore.
            file_strings_to_ignore (list[str]): List of file name substrings to ignore.
            blob (Any): The GCS blob object.
            verbose (bool): Whether to log files not being included.

        Returns:
            bool: True if the blob should be included, False otherwise.
        """
        file_path = f"gs://{bucket_name}/{blob.name}"
        if file_extensions_to_ignore and file_path.endswith(tuple(file_extensions_to_ignore)):
            if verbose:
                logging.info(f"Skipping {file_path} as it has an extension to ignore")
            return False
        if file_extensions_to_include and not file_path.endswith(tuple(file_extensions_to_include)):
            if verbose:
                logging.info(f"Skipping {file_path} as it does not have an extension to include")
            return False
        if file_strings_to_ignore and any(file_string in file_path for file_string in file_strings_to_ignore):
            if verbose:
                logging.info(f"Skipping {file_path} as it has a string to ignore")
            return False
        return True

    def list_bucket_contents(self, bucket_name: str,
                             file_extensions_to_ignore: list[str] = [],
                             file_strings_to_ignore: list[str] = [],
                             file_extensions_to_include: list[str] = [],
                             file_name_only: bool = False) -> list[dict]:
        """
        List contents of a GCS bucket and return a list of dictionaries with file information.

        Args:
            bucket_name (str): The name of the GCS bucket. If includes gs://, it will be removed.
            file_extensions_to_ignore (list[str], optional): List of file extensions to ignore. Defaults to [].
            file_strings_to_ignore (list[str], optional): List of file name substrings to ignore. Defaults to [].
            file_extensions_to_include (list[str], optional): List of file extensions to include. Defaults to [].
            file_name_only (bool, optional): Whether to return only the file list and no extra info. Defaults to False.

        Returns:
            list[dict]: A list of dictionaries containing file information.
        """
        # If the bucket name starts with gs://, remove it
        if bucket_name.startswith("gs://"):
            bucket_name = bucket_name.split("/")[2].strip()
        logging.info(f"Running list_blobs on gs://{bucket_name}/")
        blobs = self.client.list_blobs(bucket_name)
        logging.info("Finished running. Processing files now")
        # Create a list of dictionaries containing file information
        file_list = [
            self._create_bucket_contents_dict(
                blob=blob, bucket_name=bucket_name, file_name_only=file_name_only
            )
            for blob in blobs
            if self._validate_include_blob(
                blob=blob,
                file_extensions_to_ignore=file_extensions_to_ignore,
                file_strings_to_ignore=file_strings_to_ignore,
                file_extensions_to_include=file_extensions_to_include,
                bucket_name=bucket_name
            ) and not blob.name.endswith("/")
        ]
        logging.info(f"Found {len(file_list)} files in bucket")
        return file_list

    def copy_cloud_file(self, src_cloud_path: str, full_destination_path: str, verbose: bool = False) -> None:
        """
        Copy a file from one GCS location to another.

        Args:
            src_cloud_path (str): The source GCS path.
            full_destination_path (str): The destination GCS path.
            verbose (bool, optional): Whether to log progress. Defaults to False.
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
                if verbose:
                    logging.info(f"{full_destination_path}: Progress so far: {bytes_rewritten}/{bytes_to_rewrite} bytes.")
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
            verbose: bool = False,
            job_complete_for_logging: int = 500
    ) -> None:
        """
        Delete multiple cloud files in parallel using multi-threading.

        Args:
            files_to_delete (list[str]): List of GCS paths of the files to delete.
            workers (int, optional): Number of worker threads. Defaults to 5.
            max_retries (int, optional): Maximum number of retries. Defaults to 3.
            verbose (bool, optional): Whether to log each job's success. Defaults to False.
            job_complete_for_logging (int, optional): The number of jobs to complete before logging. Defaults to 500.
        """
        list_of_jobs_args_list = [[file_path] for file_path in files_to_delete]

        MultiThreadedJobs().run_multi_threaded_job(
            workers=workers,
            function=self.delete_cloud_file,
            list_of_jobs_args_list=list_of_jobs_args_list,
            max_retries=max_retries,
            fail_on_error=True,
            verbose=verbose,
            collect_output=False,
            jobs_complete_for_logging=job_complete_for_logging
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
            max_retries: int = 3,
            job_complete_for_logging: int = 500
    ) -> list[dict]:
        """
        Validate if multiple cloud files are identical based on their MD5 hashes using multithreading.

        Args:
            files_to_validate (list[Dict]): List of dictionaries containing source and destination file paths.
            log_difference (bool): Whether to log differences if files are not identical. Set false if you are running
                                   this at the start of a copy/move operation to check if files are already copied.
            workers (int, optional): Number of worker threads. Defaults to 5.
            max_retries (int, optional): Maximum number of retries for all jobs. Defaults to 3.
            job_complete_for_logging (int, optional): The number of jobs to complete before logging. Defaults to 500.

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
            max_retries=max_retries,
            jobs_complete_for_logging=job_complete_for_logging
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
            self, files_to_copy: list[dict], workers: int, max_retries: int
    ) -> None:
        """
        Copy multiple files in parallel with validation.

        Args:
            files_to_copy (list[dict]): List of dictionaries containing source and destination file paths.
            workers (int): Number of worker threads.
            max_retries (int): Maximum number of retries.
        """
        updated_files_to_move = self.loop_and_log_validation_files_multithreaded(
            files_to_copy,
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
            files_to_copy,
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
            self, files_to_move: list[dict],
            action: str,
            workers: int,
            max_retries: int,
            verbose: bool = False,
            jobs_complete_for_logging: int = 500
    ) -> None:
        """
        Move or copy multiple files in parallel.

        Args:
            files_to_move (list[dict]): List of dictionaries containing source and destination file paths.
            action (str): The action to perform ('move' or 'copy').
            workers (int): Number of worker threads.
            max_retries (int): Maximum number of retries.
            verbose (bool, optional): Whether to log each job's success. Defaults to False.
            jobs_complete_for_logging (int, optional): The number of jobs to complete before logging. Defaults to 500.

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
            collect_output=False,
            jobs_complete_for_logging=jobs_complete_for_logging
        )

    def get_blob_details(self, cloud_path: str) -> Any:
        """
        Get a GCS blob object.

        Args:
            cloud_path (str): The GCS path of the file.

        Returns:
            Any: The GCS blob object.
        """
        file_path_components = self.process_cloud_path(cloud_path=cloud_path)
        bucket_obj = self.client.bucket(bucket_name=file_path_components['bucket'])
        return bucket_obj.get_blob(file_path_components['blob_url'])

    def read_file(self, cloud_path: str, encoding: str = 'utf-8') -> str:
        """
        Read the content of a file from GCS.

        Args:
            cloud_path (str): The GCS path of the file to read.

        Returns:
            bytes: The content of the file as bytes.
        """
        file_path_components = self.process_cloud_path(cloud_path)
        blob = self.client.bucket(file_path_components['bucket']).blob(file_path_components['blob_url'])
        # Download the file content as bytes
        content_bytes = blob.download_as_bytes()
        # Convert bytes to string
        content_str = content_bytes.decode(encoding)
        return content_str
