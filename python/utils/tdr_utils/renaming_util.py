import logging
import os
from typing import Optional, Tuple
from .tdr_ingest_utils import StartAndMonitorIngest
from .tdr_api_utils import TDR
from ..gcp_utils import GCPCloudFunctions
from .. import ARG_DEFAULTS


class GetRowAndFileInfoForReingest:
    def __init__(
            self,
            table_schema_info: dict,
            files_info: dict,
            table_metrics: list[dict],
            original_column: str,
            new_column: str,
            row_identifier: str,
            temp_bucket: str,
            update_original_column: bool = False,
            column_update_only: bool = False
    ):
        """
        Initialize the GetRowAndFileInfoForReingest class.

        Args:
            table_schema_info (dict): Schema information of the table.
            files_info (dict): A dictionary where the key is the file UUID and the value is the file metadata.
            table_metrics (list[dict]): Metrics of the TDR table to update.
            original_column (str): The column name with the original value.
            new_column (str): The column name with the new value.
            row_identifier (str): The identifier for the row. Should be the primary key.
            temp_bucket (str): The temporary bucket for storing files.
            update_original_column (bool, optional): Whether to update the original column.
                If not used will just update file paths Defaults to False.
            column_update_only (bool, optional): Whether to update only the column and
                not update the file paths. Defaults to False.
        """
        self.table_schema_info = table_schema_info
        self.files_info = files_info
        self.table_metrics = table_metrics
        self.original_column = original_column
        self.new_column = new_column
        self.row_identifier = row_identifier
        self.total_files_to_reingest = 0
        self.temp_bucket = temp_bucket
        self.update_original_column = update_original_column
        self.column_update_only = column_update_only

    def _create_paths(self, file_info: dict, og_basename: str, new_basename: str) -> Tuple[str, str, str]:
        """
        Create paths for the file in TDR, updated TDR metadata, and temporary storage.

        Args:
            file_info (dict): Information about the file returned from TDR.
            og_basename (str): The original basename of the file.
            new_basename (str): The new basename of the file.

        Returns:
            Tuple[str, str, str]: Paths for temporary storage, updated TDR metadata, and access URL.
        """
        # Access url is the full path to the file in TDR
        access_url = file_info["fileDetail"]["accessUrl"]
        # Get basename of file
        file_name = os.path.basename(access_url)
        file_safe_new_basename = new_basename.replace(" ", "_")
        # Replace basename with new basename and replace spaces with underscores
        new_file_name = file_name.replace(
            f'{og_basename}.', f'{file_safe_new_basename}.')
        # get tdr path. Not real path, just the metadata
        tdr_file_path = file_info["path"]
        # Create full path to updated tdr metadata file path
        updated_tdr_metadata_path = os.path.join(
            os.path.dirname(tdr_file_path), new_file_name)
        access_url_without_bucket = access_url.split('gs://')[1]
        temp_path = os.path.join(self.temp_bucket, os.path.dirname(
            access_url_without_bucket), new_file_name)
        return temp_path, updated_tdr_metadata_path, access_url

    def _create_row_dict(
            self, row_dict: dict, file_ref_columns: list[str]
    ) -> Tuple[Optional[dict], Optional[list[dict]]]:
        """
        Go through each row and check each cell if it is a file and if it needs to be re-ingested.
        If so, create a new row dict with the new file path.

        Args:
            row_dict (dict): The original row dictionary.
            file_ref_columns (list[str]): List of columns that are file references.

        Returns:
            Tuple[Optional[dict], Optional[list[dict]]]: New row dictionary and list of files to copy,
                or None if no re-ingestion is needed.
        """
        reingest_row = False
        # Create new dictionary for ingest just the row identifier so can merge with right row later
        new_row_dict = {self.row_identifier: row_dict[self.row_identifier]}
        # Create list of all files for copy to temp location
        temp_copy_list = []
        # Get basename to replace
        og_basename = row_dict[self.original_column]
        new_basename = row_dict[self.new_column]
        # If the new basename is the same as the old one, don't do anything
        if og_basename == new_basename:
            return None, None
        for column_name in row_dict:
            # Check if column is a fileref, cell is not empty, and update is not for columns only (not files)
            if column_name in file_ref_columns and row_dict[column_name] and not self.column_update_only:
                # Get full file info for that cell
                file_info = self.files_info.get(row_dict[column_name])
                # Get potential temp path, updated tdr metadata path, and access url for file
                temp_path, updated_tdr_metadata_path, access_url = self._create_paths(
                    file_info, og_basename, new_basename  # type: ignore[arg-type]
                )
                # Check if access_url starts with og basename and then .
                if os.path.basename(access_url).startswith(f"{og_basename}."):
                    self.total_files_to_reingest += 1
                    # Add to ingest row dict to ingest from temp location with updated name
                    new_row_dict[column_name] = {
                        # temp path is the full path to the renamed file in the temp bucket
                        "sourcePath": temp_path,
                        # New target path with updated basename
                        "targetPath": updated_tdr_metadata_path,
                    }
                    # Add to copy list for copying and renaming file currently in TDR
                    temp_copy_list.append(
                        {
                            "source_file": access_url,
                            "full_destination_path": temp_path
                        }
                    )
                    # Set reingest row to True because files need to be updated
                    reingest_row = True
            # If column to update is set and column name is the column to update
            # then update the new row dict with the new file basename
            elif self.update_original_column and column_name == self.original_column:
                new_row_dict[column_name] = row_dict[self.new_column]
                reingest_row = True
        if reingest_row:
            return new_row_dict, temp_copy_list
        else:
            return None, None

    def get_new_copy_and_ingest_list(self) -> Tuple[list[dict], list[list]]:
        """
        Get the list of rows to re-ingest and files to copy to temporary storage.

        This method iterates through the table metrics, identifies the rows and files that need to be re-ingested,
        and prepares lists of these rows and files.

        Returns:
            Tuple[list[dict], list[list]]: A tuple containing a list of rows to re-ingest and a list of files to copy.
        """
        rows_to_reingest = []
        files_to_copy_to_temp = []
        # Get all columns in the table that are file references
        file_ref_columns = [
            col['name'] for col in self.table_schema_info['columns'] if col['datatype'] == 'fileref']
        for row_dict in self.table_metrics:
            new_row_dict, temp_copy_list = self._create_row_dict(row_dict, file_ref_columns)
            # If there is something to copy and update
            if new_row_dict and temp_copy_list:
                rows_to_reingest.append(new_row_dict)
                files_to_copy_to_temp.append(temp_copy_list)
        logging.info(f"Total rows to re-ingest: {len(rows_to_reingest)}")
        logging.info(f"Total files to copy and re-ingest: {self.total_files_to_reingest}")
        return rows_to_reingest, files_to_copy_to_temp


class BatchCopyAndIngest:
    def __init__(
            self,
            rows_to_ingest: list[dict],
            tdr: TDR,
            target_table_name: str,
            cloud_type: str,
            update_strategy: str,
            workers: int,
            dataset_id: str,
            copy_and_ingest_batch_size: int,
            row_files_to_copy: list[list[dict]],
            wait_time_to_poll: int = ARG_DEFAULTS['waiting_time_to_poll']  # type: ignore[assignment]
    ) -> None:
        """
        Initialize the BatchCopyAndIngest class.

        Args:
            rows_to_ingest (list[dict]): List of rows to ingest.
            tdr (TDR): TDR instance for interacting with the TDR API.
            target_table_name (str): Name of the target table.
            cloud_type (str): Type of cloud storage.
            update_strategy (str): Strategy for updating the data.
            workers (int): Number of workers for parallel processing copies of files to temp location.
            dataset_id (str): ID of the dataset.
            copy_and_ingest_batch_size (int): Size of each batch for copying and ingesting.
            row_files_to_copy (list[list[dict]]): List of files to copy for each row.
            wait_time_to_poll (int, optional): Time to wait between polling for ingest status.
                Defaults to ARG_DEFAULTS['waiting_time_to_poll'].
        """
        self.rows_to_ingest = rows_to_ingest
        self.tdr = tdr
        self.target_table_name = target_table_name
        self.dataset_id = dataset_id
        self.cloud_type = cloud_type
        self.update_strategy = update_strategy
        self.workers = workers
        self.wait_time_to_poll = wait_time_to_poll
        self.copy_and_ingest_batch_size = copy_and_ingest_batch_size
        self.row_files_to_copy = row_files_to_copy

    def run(self) -> None:
        """
        Run the batch copy and ingest process.

        This method batches the rows to copy files and ingest them into the dataset. It also
        deletes the temporary files after ingestion.
        """
        # Batch through rows to copy files down and ingest so if script fails partway through large
        # copy and ingest it will have copied over and ingested some of the files already
        logging.info(
            f"Batching {len(self.rows_to_ingest)} total rows into batches of {self.copy_and_ingest_batch_size} " +
            "for copying to temp location and ingest"
        )
        total_batches = len(self.rows_to_ingest) // self.copy_and_ingest_batch_size + 1
        gcp_functions = GCPCloudFunctions()
        for i in range(0, len(self.rows_to_ingest), self.copy_and_ingest_batch_size):
            batch_number = i // self.copy_and_ingest_batch_size + 1
            logging.info(
                f"Starting batch {batch_number} of {total_batches} for copy to temp and ingest")
            ingest_metadata_batch = self.rows_to_ingest[i:i +
                                                        self.copy_and_ingest_batch_size]
            files_to_copy_batch = self.row_files_to_copy[i:i +
                                                         self.copy_and_ingest_batch_size]
            # files_to_copy_batch will be a list of lists of dicts, so flatten it
            files_to_copy = [
                file_dict for sublist in files_to_copy_batch for file_dict in sublist]

            # Copy files to temp bucket if anything to copy
            if files_to_copy:
                gcp_functions.multithread_copy_of_files_with_validation(
                    # Create dict with new names for copy of files to temp bucket
                    files_to_copy=files_to_copy,
                    workers=self.workers,
                    max_retries=5
                )

            logging.info(
                f"Batch {batch_number} of {total_batches} batches being ingested to dataset. "
                f"{len(ingest_metadata_batch)} total rows in current ingest."
            )
            # Ingest renamed files into dataset
            StartAndMonitorIngest(
                tdr=self.tdr,
                ingest_records=ingest_metadata_batch,
                target_table_name=self.target_table_name,
                dataset_id=self.dataset_id,
                load_tag=f"{self.target_table_name}_re-ingest",
                bulk_mode=False,
                update_strategy=self.update_strategy,
                waiting_time_to_poll=self.wait_time_to_poll,
            ).run()

            # Delete files from temp bucket
            # Create list of files in temp location to delete. Full destination path is the temp location from copy
            files_to_delete = [file_dict['full_destination_path']
                               for file_dict in files_to_copy]
            gcp_functions.delete_multiple_files(
                # Create list of files in temp location to delete. Full destination path is the temp location from copy
                files_to_delete=files_to_delete,
                workers=self.workers,
            )
