import os
import logging
import io
import hashlib
import base64
from humanfriendly import format_size, parse_size
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

    def __init__(self, project: Optional[str] = None) -> None:
        """
        Initialize the GCPCloudFunctions class.
        Authenticates using the default credentials and sets up the storage client.
        """
        from google.cloud import storage
        from google.auth import default
        credentials, default_project = default()
        if not project:
            project = default_project
        self.client = storage.Client(credentials=credentials, project=project)

    @staticmethod
    def _process_cloud_path(cloud_path: str) -> dict:
        """
        Process a GCS cloud path into its components.

        Args:
            cloud_path (str): The GCS cloud path.

        Returns:
            dict: A dictionary containing the platform prefix, bucket name, and blob URL.
        """
        platform_prefix, remaining_url = str.split(str(cloud_path), sep="//", maxsplit=1)
        bucket_name = str.split(remaining_url, sep="/")[0]
        blob_name = "/".join(str.split(remaining_url, sep="/")[1:])
        path_components = {
            "platform_prefix": platform_prefix,
            "bucket": bucket_name,
            "blob_url": blob_name
        }
        return path_components

    def load_blob_from_full_path(self, full_path: str) -> Any:
        """
        Load a GCS blob object from a full GCS path.

        Args:
            full_path (str): The full GCS path.

        Returns:
            Any: The GCS blob object.
        """
        file_path_components = self._process_cloud_path(full_path)

        # Specify the billing project
        bucket = self.client.bucket(file_path_components["bucket"], user_project=self.client.project)
        blob = bucket.blob(file_path_components["blob_url"])

        # If blob exists in GCS reload it so metadata is there
        if blob.exists():
            blob.reload()
        return blob

    def check_file_exists(self, full_path: str) -> bool:
        """
        Check if a file exists in GCS.

        Args:
            full_path (str): The full GCS path.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        blob = self.load_blob_from_full_path(full_path)
        return blob.exists()

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

        logging.info(f"Accessing bucket: {bucket_name} with project: {self.client.project}")

        # Get the bucket object and set user_project for Requester Pays
        bucket = self.client.bucket(bucket_name, user_project=self.client.project)

        # List blobs within the bucket
        blobs = bucket.list_blobs()
        logging.info("Finished listing blobs. Processing files now.")

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
        try:
            src_blob = self.load_blob_from_full_path(src_cloud_path)
            dest_blob = self.load_blob_from_full_path(full_destination_path)

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
            logging.error(
                f"Encountered the following error while attempting to copy file from '{src_cloud_path}' to "
                f"'{full_destination_path}': {e}. If this is a retryable error, it will be re-attempted"
            )
            raise

    def delete_cloud_file(self, full_cloud_path: str) -> None:
        """
        Delete a file from GCS.

        Args:
            full_cloud_path (str): The GCS path of the file to delete.
        """
        blob = self.load_blob_from_full_path(full_cloud_path)
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
        blob = self.load_blob_from_full_path(target_path)
        return blob.size

    def validate_files_are_same(self, src_cloud_path: str, dest_cloud_path: str) -> bool:
        """
        Validate if two cloud files (source and destination) are identical based on their MD5 hashes.

        Args:
            src_cloud_path (str): The source GCS path.
            dest_cloud_path (str): The destination GCS path.

        Returns:
            bool: True if the files are identical, False otherwise.
        """

        src_blob = self.load_blob_from_full_path(src_cloud_path)
        dest_blob = self.load_blob_from_full_path(dest_cloud_path)

        # If either blob is None or does not exist
        if not src_blob or not dest_blob or not src_blob.exists() or not dest_blob.exists():
            return False
        # If the MD5 hashes exist
        if src_blob.md5_hash and dest_blob.md5_hash:
            # And are the same return True
            if src_blob.md5_hash == dest_blob.md5_hash:
                return True
        else:
            # If md5 do not exist (for larger files they may not) check size matches
            if src_blob.size == dest_blob.size:
                return True
        # Otherwise, return False
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
        list_of_jobs_args_list = [[file_path] for file_path in set(files_to_delete)]

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

    def validate_file_pair(self, source_file: str, full_destination_path: str) -> dict:
        """
        Helper function to validate if source and destination files are identical.

        Args:
            source_file (str): The source file path.
            full_destination_path (str): The destination file path.

        Returns:
            dict: The file dictionary of the files with a boolean indicating if they are identical.
        """
        if self.validate_files_are_same(source_file, full_destination_path):
            identical = True
        else:
            identical = False
        return {"source_file": source_file, "full_destination_path": full_destination_path, "identical": identical}

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
        checked_files = MultiThreadedJobs().run_multi_threaded_job(
            workers=workers,
            function=self.validate_file_pair,
            list_of_jobs_args_list=jobs,
            collect_output=True,
            max_retries=max_retries,
            jobs_complete_for_logging=job_complete_for_logging
        )
        # If any files failed to load, raise an exception
        if files_to_validate and None in checked_files:  # type: ignore[operator]
            logging.error("Failed to validate all files, could not load some blobs")
            raise Exception("Failed to validate all files")

        # Get all files that are not identical
        not_identical_files = [
            file_dict
            for file_dict in checked_files  # type: ignore[operator, union-attr]
            if not file_dict['identical']
        ]
        if not_identical_files:
            if log_difference:
                for file_dict in not_identical_files:
                    logging.warning(
                        f"File {file_dict['source_file']} and {file_dict['full_destination_path']} are not identical"
                    )
            logging.info(f"Validation complete. {len(not_identical_files)} files are not identical.")
        return not_identical_files

    def multithread_copy_of_files_with_validation(
            self, files_to_copy: list[dict], workers: int, max_retries: int
    ) -> None:
        """
        Copy multiple files in parallel with validation.

        Args:
            files_to_copy (list[dict]): List of dictionaries containing source and destination file paths.
                dict should have keys "source_file" and "full_destination_path"
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

    def read_file(self, cloud_path: str, encoding: str = 'utf-8') -> str:
        """
        Read the content of a file from GCS.

        Args:
            cloud_path (str): The GCS path of the file to read.
            encoding (str, optional): The encoding to use. Defaults to 'utf-8'.

        Returns:
            bytes: The content of the file as bytes.
        """

        blob = self.load_blob_from_full_path(cloud_path)
        # Download the file content as bytes
        content_bytes = blob.download_as_bytes()
        # Convert bytes to string
        content_str = content_bytes.decode(encoding)
        return content_str

    def upload_blob(self, destination_path: str, source_file: str) -> None:
        """
        Upload a file to GCS.

        Args:
            destination_path (str): The destination GCS path.
            source_file (str): The source file path.
        """
        blob = self.load_blob_from_full_path(destination_path)
        blob.upload_from_filename(source_file)

    def get_object_md5(
        self,
        file_path: str,
        # https://jbrojbrojbro.medium.com/finding-the-optimal-download-size-with-gcs-259dc7f26ad2
        chunk_size: int = parse_size("256 KB"),
        logging_bytes: int = parse_size("1 GB"),
        returned_md5_format: str = "hex"
    ) -> str:
        """
        Calculate the MD5 checksum of a file in GCS.

        Args:
            file_path (str): The GCS path of the file.
            chunk_size (int, optional): The size of each chunk to read. Defaults to 256 KB.
            logging_bytes (int, optional): The number of bytes to read before logging progress. Defaults to 1 GB.
            returned_md5_format (str, optional): The format of the MD5 checksum to return. Defaults to "hex".
                Options are "hex" or "base64". hex = md5sum returns and base63 = gsutil stores.

        Returns:
            str: The MD5 checksum of the file.
        """
        if returned_md5_format not in ["hex", "base64"]:
            raise ValueError("returned_md5_format must be 'hex' or 'base64'")

        blob = self.load_blob_from_full_path(file_path)

        # Create an MD5 hash object
        md5_hash = hashlib.md5()

        blob_size_str = format_size(blob.size)
        logging.info(f"Streaming {file_path} which is {blob_size_str}")
        # Use a BytesIO stream to collect data in chunks and upload it
        buffer = io.BytesIO()
        total_bytes_streamed = 0
        # Keep track of the last logged size for data logging
        last_logged = 0

        with blob.open("rb") as source_stream:
            while True:
                chunk = source_stream.read(chunk_size)
                if not chunk:
                    break
                md5_hash.update(chunk)
                buffer.write(chunk)
                total_bytes_streamed += len(chunk)
                # Log progress every 1 gb if verbose used
                if total_bytes_streamed - last_logged >= logging_bytes:
                    logging.info(f"Streamed {format_size(total_bytes_streamed)} / {blob_size_str} so far")
                    last_logged = total_bytes_streamed

        if returned_md5_format == "hex":
            md5 = md5_hash.hexdigest()
            logging.info(f"MD5 (hex) for {file_path}: {md5}")
        elif returned_md5_format == "base64":
            md5 = base64.b64encode(md5_hash.digest()).decode("utf-8")
            logging.info(f"MD5 (base64) for {file_path}: {md5}")
        return md5

    def copy_onprem_to_cloud(self, onprem_src_path: str, cloud_dest_path: str) -> None:
        """
        Copy a file from an on-premises location to GCS.

        Args:
            onprem_src_path (str): The source path of the file on-premises.
            cloud_dest_path (str): The destination GCS path.

        Raises:
            Exception: If the source file does not exist or the user does not have permission to access it.
        """
        if not os.path.isfile(onprem_src_path):
            raise Exception(f"{onprem_src_path} does not exist or user does not have permission to it")
        dest_blob = self.load_blob_from_full_path(cloud_dest_path)
        dest_blob.upload_from_filename(onprem_src_path)

    def write_to_gcs(self, cloud_path: str, content: str) -> None:
        """
        Write content to a file in GCS.

        Args:
            cloud_path (str): The GCS path of the file to write.
            content (str): The content to write.
        """
        blob = self.load_blob_from_full_path(cloud_path)
        blob.upload_from_string(content)
        logging.info(f"Successfully wrote content to {cloud_path}")

    def set_acl_public_read(self, cloud_path: str) -> None:
        """
        Set the file in the bucket to be publicly readable.

        Args:
            cloud_path (str): The GCS path of the file.
        """
        blob = self.load_blob_from_full_path(cloud_path)
        blob.acl.all().grant_read()
        blob.acl.save()

    def set_acl_group_owner(self, cloud_path: str, group_email: str) -> None:
        """
        Set the file in the bucket to grant OWNER permission to a specific group.

        Args:
            cloud_path (str): The GCS path of the file.
            group_email (str): The email of the group to grant OWNER permission
        """
        blob = self.load_blob_from_full_path(cloud_path)
        blob.acl.group(group_email).grant_owner()
        blob.acl.save()

    def set_metadata_cache_control(self, cloud_path: str, cache_control: str) -> None:
        """
        Set Cache-Control metadata for a file.

        Args:
            cloud_path (str): The GCS path of the file.
            cache_control (str): The Cache-Control metadata to set.
        """
        blob = self.load_blob_from_full_path(cloud_path)
        blob.cache_control = cache_control
        blob.patch()

    def get_file_contents_of_most_recent_blob_in_bucket(self, bucket_name: str) -> Optional[tuple[str, str]]:
        """
        Gets the most recent blob in the bucket. If the blob with the most recent timestamp doesn't have
        any logging besides the basic "storage_byte_hours" logging, will continue to look at the next most
        recent file until a log with useful information is encountered. This is useful when combing through
        GCP activity logs for Terra workspace buckets.

        Args:
            bucket_name (str): The GCS bucket name.
        Returns:
            Optional tuple of the blob found and the file contents from the blob
        """
        blobs = sorted(
            self.client.list_blobs(bucket_name), key=lambda blob: blob.updated, reverse=True
        )
        for blob in blobs:
            # Download the file contents as a string
            file_contents = blob.download_as_text()

            # Check if the content matches the undesired format
            lines = file_contents.splitlines()
            if len(lines) > 1 and lines[0] == '"bucket","storage_byte_hours"':
                logging.info(f"Skipping file {blob.name} as it matches the undesired format.")
                continue

            # If it doesn't match the undesired format, return its content
            logging.info(f"Found valid file: {blob.name}")
            return blob, file_contents

        logging.info("No valid files found.")
        return None

    def write_to_gcp_file(self, cloud_path: str, file_contents: str) -> None:
        """
        Write content to a file in GCS.
        """
        blob = self.load_blob_from_full_path(cloud_path)
        blob.upload_from_string(file_contents)
