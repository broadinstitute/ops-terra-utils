import logging
from argparse import ArgumentParser, Namespace
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils import comma_separated_list
from ops_utils.gcp_utils import GCPCloudFunctions
from ops_utils.terra_util import TerraWorkspace
from ops_utils.csv_util import Csv
from ops_utils.vars import ARG_DEFAULTS, GCP
import os

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("--workspace_name", "-w", type=str, required=True, help="Terra workspace name")
    parser.add_argument("--billing_project", "-b", type=str, required=True, help="Billing project name")
    parser.add_argument("--metrics_tsv", "-m", type=str, required=True, help="Path to the metrics TSV file")
    parser.add_argument("--skip_upload_column", "-s", type=comma_separated_list,
                        help="Column name to skip upload. Use comma separated values for multiple columns. Optional")
    parser.add_argument("--flatten_path", "-f", action="store_true",
                        help="If you want to flatten all file paths and put all files in one dir. Optional")
    parser.add_argument("--subdir", "-d", type=str, help="Subdirectory to upload files to. Optional")
    parser.add_argument("--id_column", "-i", type=str, help="Column name for the id column", required=True)
    return parser.parse_args()


class ConvertContents:
    def __init__(
            self,
            contents: list[dict],
            id_column: str,
            bucket_name: str,
            flatten_path: bool,
            subdir: str,
            skip_upload_column: list[str],
    ):
        self.contents = contents
        self.id_column = id_column
        self.flatten_path = flatten_path
        self.skip_upload_column = skip_upload_column
        self.new_bucket_path = f"gs://{bucket_name}" if not subdir else f"gs://{bucket_name}/{subdir}"
        self.files_to_copy: list[dict] = []
        self.headers: set = set()
        self.new_tsv_contents: list[dict] = []

    def _get_file_copy_dict(self, file_path: str) -> dict:
        if self.flatten_path:
            file_name = os.path.basename(file_path)
            new_path = f"{self.new_bucket_path}/{file_name}"
        else:
            path_without_bucket = '/'.join(file_path.split("/")[3:])
            new_path = f"{self.new_bucket_path}/{path_without_bucket}"
        return {"source_file": file_path, "full_destination_path": new_path}

    @staticmethod
    def _check_list_unique(list_of_strs: list[str], list_type: str) -> bool:
        seen = set()
        duplicates = set()
        for entry in list_of_strs:
            if entry in seen:
                duplicates.add(entry)
            seen.add(entry)
        if duplicates:
            logging.error(f"Duplicate {list_type} found. Will overwrite each other: {duplicates}")
            return False
        return True

    def _update_file_paths(self, cell_value: str) -> str:
        if cell_value.startswith("gs://"):
            copy_dict = self._get_file_copy_dict(cell_value)
            self.files_to_copy.append(copy_dict)
            return copy_dict["full_destination_path"]
        return cell_value

    def _validate_results(self) -> None:
        valid = True
        dest_file_paths = [copy_dict["full_destination_path"] for copy_dict in self.files_to_copy]
        tsv_identifiers = [row[f"entity:{self.id_column}"] for row in self.new_tsv_contents]
        # Check for duplicates in file paths and tsv identifiers
        if not self._check_list_unique(dest_file_paths, "destination file paths"):
            valid = False
        if not self._check_list_unique(tsv_identifiers, self.id_column):
            valid = False
        # Check for id column in TSV
        if f"entity:{self.id_column}" not in self.headers:
            logging.error(f"ID column {self.id_column} not found in TSV file.")
            valid = False
        # Check for null or empty values in id column
        if None or "" in tsv_identifiers:
            logging.error(f"ID column {self.id_column} contains null or empty values.")
            valid = False
        # Check ids only contain alphanumeric characters, underscores, dashes, and periods
        for identifier in tsv_identifiers:
            if not all(char.isalnum() or char in ['_', '-', '.'] for char in identifier):
                logging.error(
                    f"Invalid character in ID column {self.id_column}: {identifier}."
                    f" Only alphanumeric characters, underscores, dashes, and periods are allowed.")
                valid = False
        if not valid:
            raise ValueError("Invalid input. Check logs for details.")

    def run(self) -> tuple[list[dict], list[dict], set]:
        for row in self.contents:
            new_row = {}
            for header, value in row.items():
                # Add entity: to the id column
                if header == self.id_column:
                    header = f"entity:{header}"
                # Add header to set of headers
                self.headers.add(header)
                if self.skip_upload_column and header in self.skip_upload_column:
                    # if skip upload column then leave as is
                    new_row[header] = value
                # If value is a list then check each entry in the list
                elif isinstance(value, list):
                    new_list = []
                    for entry in value:
                        new_list.append(self._update_file_paths(entry))
                    new_row[header] = new_list
                else:
                    new_row[header] = self._update_file_paths(value)
            self.new_tsv_contents.append(new_row)
        self._validate_results()
        return self.new_tsv_contents, self.files_to_copy, self.headers


class UploadContentsToTerra:
    NEW_TSV = "updated_metrics.tsv"

    def __init__(self, terra_workspace: TerraWorkspace, contents: list[dict], id_column: str, headers: set):
        self.terra_workspace = terra_workspace
        self.contents = contents
        self.id_column = f"entity:{id_column}"
        self.headers = headers

    def run(self) -> None:
        header_list = [self.id_column] + [header for header in self.headers if header != self.id_column]
        Csv(file_path=self.NEW_TSV).create_tsv_from_list_of_dicts(
            list_of_dicts=self.contents,
            header_list=header_list
        )
        logging.info(f"Uploading {self.NEW_TSV} to Terra")
        self.terra_workspace.upload_metadata_to_workspace_table(self.NEW_TSV)


if __name__ == '__main__':
    args = get_args()
    billing_project, workspace_name = args.billing_project, args.workspace_name
    metrics_tsv, skip_upload_column = args.metrics_tsv, args.skip_upload_column
    flatten_path, subdir = args.flatten_path, args.subdir

    token = Token()
    request_util = RunRequest(token=token)
    # Create Terra object to interact with the Terra with the request_util object
    terra_workspace = TerraWorkspace(
        billing_project=billing_project,
        workspace_name=workspace_name,
        request_util=request_util
    )

    workspace_bucket = terra_workspace.get_workspace_bucket()
    # Read in TSV file
    metrics_tsv_contents = Csv(file_path=metrics_tsv).create_list_of_dicts_from_tsv()

    converted_contents, files_to_copy, headers = ConvertContents(
        contents=metrics_tsv_contents,
        id_column=args.id_column,
        bucket_name=workspace_bucket,
        flatten_path=flatten_path,
        subdir=subdir,
        skip_upload_column=skip_upload_column
    ).run()

    logging.info(f"Copying {len(files_to_copy)} files to {workspace_bucket}")
    # Copy files to new location
    GCPCloudFunctions().multithread_copy_of_files_with_validation(
        files_to_copy=files_to_copy,
        workers=ARG_DEFAULTS['multithread_workers'],
        max_retries=5
    )

    UploadContentsToTerra(
        terra_workspace=terra_workspace,
        contents=converted_contents,
        id_column=args.id_column,
        headers=headers
    ).run()
