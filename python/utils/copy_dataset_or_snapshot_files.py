from collections import Counter
import os
from ops_utils.vars import ARG_DEFAULTS
from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.gcp_utils import GCPCloudFunctions
import logging
from typing import Optional


class SourceDestinationMapping:
    def __init__(self, file_metadata: list[dict], output_bucket: str, download_type: str):
        self.file_metadata = file_metadata
        self.output_bucket = output_bucket
        self.download_type = download_type

    @staticmethod
    def _validate_file_destinations_unique(mapping: list[dict]) -> None:
        all_destination_paths = [a["full_destination_path"] for a in mapping]
        file_counts = Counter(all_destination_paths)
        duplicates = [file for file, count in file_counts.items() if count > 1]
        if duplicates:
            formatted_duplicates = "\n".join(duplicates)
            raise Exception(
                f"""Not all destinations were unique. If you selected 'flat' as the download type, try re-running with
                    'structured' instead. Below is a list of files that were duplicated:\n{formatted_duplicates}"""
            )

    def get_source_and_destination_paths(self) -> list[dict]:
        mapping = []
        output = self.output_bucket if self.output_bucket.startswith("gs://") else f"gs://{self.output_bucket}"

        for file in self.file_metadata:
            source = file["fileDetail"]["accessUrl"]
            if self.download_type == "flat":
                destination = os.path.join(output, os.path.basename(source).lstrip("/"))
            else:
                destination = os.path.join(output, file["path"].lstrip("/"))
            mapping.append(
                {
                    "source_file": source,
                    "full_destination_path": destination,
                }
            )

        self._validate_file_destinations_unique(mapping)
        return mapping


class CopyDatasetOrSnapshotFiles:
    def __init__(
            self,
            tdr: TDR,
            gcp_functions: GCPCloudFunctions,
            output_bucket: str,
            snapshot_id: Optional[str] = None,
            dataset_id: Optional[str] = None,
            verbose: bool = False,
            download_type: str = "structured",
    ):
        """
        Initialize the CopyDatasetOrSnapshotFiles class.

        Args:
            tdr (TDR): The TDR instance to interact with the Terra Data Repository.
            snapshot_id (str, optional): The ID of the snapshot to copy files from.
            dataset_id (str, optional): The ID of the dataset to copy files from.
            output_bucket (str): The GCS bucket where files will be copied.
            download_type (str): The type of download, either 'flat' or 'structured'.
            gcp_functions (GCPCloudFunctions): The GCP functions instance for file operations.
            verbose (bool): If True, enables verbose logging.
        """
        self.tdr = tdr
        self.snapshot_id = snapshot_id
        self.dataset_id = dataset_id
        self.output_bucket = output_bucket
        self.download_type = download_type
        self.gcp_functions = gcp_functions
        self.verbose = verbose

    def run(self) -> None:
        if self.snapshot_id:
            file_metadata = self.tdr.get_files_from_snapshot(snapshot_id=self.snapshot_id)
        else:
            file_metadata = self.tdr.get_dataset_files(dataset_id=self.dataset_id)

        # get the source and destination mapping and validate that all output paths are unique
        mapping = SourceDestinationMapping(
            file_metadata=file_metadata,
            output_bucket=self.output_bucket,
            download_type=self.download_type
        ).get_source_and_destination_paths()
        for map in mapping:
            if self.verbose:
                logging.info(f"Copying {map['source_file']} to {map['full_destination_path']}")

        self.gcp_functions.multithread_copy_of_files_with_validation(
            files_to_copy=mapping, workers=ARG_DEFAULTS['multithread_workers']
        )
