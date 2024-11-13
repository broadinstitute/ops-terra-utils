import logging
from argparse import ArgumentParser, Namespace
from utils import ARG_DEFAULTS
import os
from collections import Counter

from utils.gcp_utils import GCPCloudFunctions

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(description="copy files from one bucket to another")
    parser.add_argument(
        "--destination_path",
        "-dp",
        help="Path to where files should be copied. Should be in format " +
             "gs://bucket_name/path/to/folder/ or gs://bucket_name/",
        required=True
    )
    parser.add_argument(
        "--preserve_structure",
        "-f",
        action="store_true",
        help="Set this flag if you do not wish to flatten the structure of the files"
    )
    input_args = parser.add_mutually_exclusive_group(required=True)
    input_args.add_argument(
        "--source_bucket",
        "-sb",
        help="Use option if want to copy all files from source bucket. Should be in format gs://bucket_name/"
    )
    input_args.add_argument(
        "--source_fofn",
        "-sf",
        help="Use option if want to copy files listed in a file. Put one file per line. " +
             "File should exist in accessible GCP bucket"
    )
    return parser.parse_args()


class CreateCopyDict:
    def __init__(self, source_files: list[str], destination_path: str, preserve_structure: bool):
        self.source_files = source_files
        self.destination_path = destination_path
        self.preserve_structure = preserve_structure

    def _create_copy_dict(self, src_file_path: str) -> dict:
        if not src_file_path.startswith("gs://"):
            logging.error(f"Source file path {src_file_path} must start with gs://")
            exit(1)
        # Get the source bucket from the source file path
        source_bucket = f'gs://{src_file_path.split("/")[2].strip()}/'
        if self.preserve_structure:
            return {
                "source_file": src_file_path,
                "full_destination_path": src_file_path.replace(source_bucket, self.destination_path)
            }
        return {
            "source_file": src_file_path,
            "full_destination_path": os.path.join(self.destination_path, os.path.basename(src_file_path))
        }

    @staticmethod
    def _validate_no_duplicates(copy_dict: list[dict]) -> bool:
        valid = True
        # Extract all destination paths
        destination_paths = [item["full_destination_path"] for item in copy_dict]
        # Count occurrences of each destination path
        path_counts = Counter(destination_paths)
        # Log a warning for any path that appears more than once
        for path, count in path_counts.items():
            if count > 1:
                valid = False
                logging.warning(f"Duplicate destination path found: {path}, occurring {count} times.")
        return valid

    def run(self) -> list[dict]:
        copy_dict = [self._create_copy_dict(src_file) for src_file in self.source_files]
        if not self._validate_no_duplicates(copy_dict):
            logging.error(
                "Duplicate destination paths found. If not used try using "
                "--preserve-structure so make destination paths unique."
            )
            exit(1)
        return copy_dict


if __name__ == '__main__':
    args = get_args()
    # Ensure destination and source path ends with /
    destination_bucket = args.destination_path if args.destination_path.endswith("/") else f"{args.destination_path}/"
    if args.source_bucket:
        source_bucket = args.source_bucket if args.source_bucket.endswith("/") else f"{args.source_bucket}/"
    else:
        source_bucket = None
    source_fofn = args.source_fofn
    preserve_structure = args.preserve_structure

    if not destination_bucket.startswith("gs://"):
        logging.error("Destination path must start with gs://")
        exit(1)
    if source_bucket and not source_bucket.startswith("gs://"):
        logging.error("Source bucket must start with gs://")
        exit(1)

    gcp = GCPCloudFunctions()
    if source_bucket:
        files_to_copy = [
            file_dict['path']
            for file_dict in gcp.list_bucket_contents(source_bucket, file_name_only=True)
        ]
    else:
        files_to_copy = [
            file_path.strip()
            for file_path in gcp.read_file(source_fofn).split("\n")
            if file_path.strip()
        ]

    copy_dict = CreateCopyDict(
        source_files=files_to_copy,
        destination_path=destination_bucket,
        preserve_structure=preserve_structure
    ).run()

    gcp.multithread_copy_of_files_with_validation(
        files_to_copy=copy_dict,
        max_retries=ARG_DEFAULTS["max_retries"],
        workers=ARG_DEFAULTS["multithread_workers"]
    )
