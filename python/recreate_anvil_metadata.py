from ops_utils.terra_util import TerraWorkspace
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.gcp_utils import GCPCloudFunctions
from ops_utils.csv_util import Csv
from ops_utils import comma_separated_list
from typing import Optional, Set

import argparse

import logging

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Get information for files in workspace bucket and upload metadata to file_metadata table"
    )
    parser.add_argument("--workspace_name", "-w", required=True, type=str)
    parser.add_argument("--billing_project", "-b", required=True, type=str)
    parser.add_argument("--dataset_id", "-d", required=True, type=str,
                        help="The ID of the dataset to recreate metadata for")
    parser.add_argument("--force", "-f", action="store_true",
                        help="If tables already exists in terra workspace, force upload")
    parser.add_argument("--tables_to_ignore", "-i", type=comma_separated_list,
                        help="Comma-separated list of table names to ignore")
    parser.add_argument("--table_prefix_to_ignore", "-p", type=str)
    return parser.parse_args()


class WorkspaceFileFetcher:
    """Fetches file IDs and paths from a Terra workspace bucket."""

    def __init__(self, terra_workspace: TerraWorkspace):
        self.terra_workspace = terra_workspace

    def get_file_id_to_path_dict(self) -> dict:
        """Return a dict mapping file_id to file_path from the workspace bucket."""
        workspace_bucket = self.terra_workspace.get_workspace_bucket()
        workspace_files = GCPCloudFunctions().list_bucket_contents(
            bucket_name=workspace_bucket,
            file_name_only=True
        )
        # Assumes file_id is the 4th element in the path split by '/'
        return {
            file['path'].split('/')[3]: file['path']
            for file in workspace_files
        }


class TDRTableDataCollector:
    """
    Collects table data from TDR and returns it with file reference columns replaced by file paths or 'DOES_NOT_EXIST'.
    Also fetches table schemas internally.
    """

    def __init__(self, tdr: TDR, dataset_id: str, file_id_to_path_dict: dict, tables_to_ignore: Optional[Set[str]] = None, prefix_to_ignore: Optional[str] = None):
        self.tdr = tdr
        self.dataset_id = dataset_id
        self.file_id_to_path_dict = file_id_to_path_dict
        self.tables_to_ignore = tables_to_ignore or set()
        self.prefix_to_ignore = prefix_to_ignore

    def get_table_dicts(self) -> list:
        """Return a list of table schema dicts for the dataset."""
        dataset_info = self.tdr.get_dataset_info(dataset_id=self.dataset_id, info_to_include=["SCHEMA"]).json()
        return [table for table in dataset_info["schema"]["tables"]]

    def collect_tables(self) -> dict:
        """
        Return a dict of collected table data keyed by table name, with file reference columns replaced.
        """
        table_dicts = self.get_table_dicts()
        collected_tables = {}
        for table_dict in table_dicts:
            table_name = table_dict["name"]
            if (table_name in self.tables_to_ignore or
                    (self.prefix_to_ignore and table_name.startswith(self.prefix_to_ignore))
                ):
                logging.info(f"Skipping table {table_name} as it is in the ignore list or has the prefix to ignore.")
                continue
            # Get columns where datatype is fileref
            file_columns = [column["name"] for column in table_dict["columns"] if column["datatype"] == "fileref"]
            # Get table data (metrics)
            table_metrics = self.tdr.get_dataset_table_metrics(
                dataset_id=self.dataset_id,
                target_table_name=table_name
            )
            # Replace file_id with file_path or 'DOES_NOT_EXIST' in file_columns
            for row in table_metrics:
                for col in file_columns:
                    if col in row:
                        file_id = row[col]
                        row[col] = self.file_id_to_path_dict.get(file_id, "DOES_NOT_EXIST")
            # If the table name is 'workspace_attributes', rename it to 'orig_workspace_attributes'
            if table_name == "workspace_attributes":
                table_name = "orig_workspace_attributes"

            # Only add the table if it has data
            if table_metrics:
                collected_tables[table_name] = table_metrics
            else:
                logging.info(f"Skipping table {table_name} as it has no metrics")
        return collected_tables


class TableColumnNormalizer:
    """
    Utility class to normalize column names for specific tables before TSV creation.
    """
    @staticmethod
    def normalize_columns(table_name: str, rows: list[dict]) -> list[dict]:
        if table_name == "file_inventory":
            for row in rows:
                # Rename file_id to file_inventory_id
                if "file_id" in row:
                    row["file_inventory_id"] = row.pop("file_id")
                # Rename name to file_name
                if "name" in row:
                    row["file_name"] = row.pop("name")
                # Remove uri column if present
                if "uri" in row:
                    del row["uri"]
        elif table_name == "orig_workspace_attributes":
            for row in rows:
                if "attribute" in row:
                    attribute = row.pop("attribute")
                    clean_attribute = attribute.replace(":", "_")
                    row["orig_workspace_attributes_id"] = clean_attribute
        return rows


class TerraTableUploader:
    """
    Handles creation of TSV files from collected table data and uploads them to a Terra workspace.
    """

    def __init__(self, collected_tables: dict, terra_workspace: TerraWorkspace):
        self.collected_tables = collected_tables
        self.terra_workspace = terra_workspace

    def _dicts_to_tsv(self, table_name: str, rows: list[dict]) -> str:
        """
        Converts a list of dicts to a TSV file for Terra import.
        The first column is entity:{table_name}_id, and the columns {table_name}_id and datarepo_row_id are removed from the rest.
        Returns the path to the TSV file.
        """
        # Normalize columns for special tables
        rows = TableColumnNormalizer.normalize_columns(table_name, rows)

        file_path = f"{table_name}.tsv"
        if not rows:
            raise ValueError(f"No data to write for table {table_name}")
        id_col = f"{table_name}_id"
        entity_id_col = f"entity:{table_name}_id"
        exclude_columns = [id_col, "datarepo_row_id"]
        # Prepare header: entity:{table_name}_id first, then all other columns except those in exclude_columns
        fieldnames = [entity_id_col] + [k for k in rows[0].keys() if k not in exclude_columns]
        # Prepare the list of dicts for TSV: replace id_col with entity_id_col and remove exclude_columns from the rest
        tsv_rows = []
        for row in rows:
            tsv_row = {entity_id_col: row[id_col]}
            tsv_row.update({k: v for k, v in row.items() if k not in exclude_columns})
            tsv_rows.append(tsv_row)
        # Use Csv utility to create the TSV file
        Csv(file_path=file_path).create_tsv_from_list_of_dicts(
            list_of_dicts=tsv_rows,
            header_list=fieldnames
        )
        return file_path

    def upload_all_tables(self) -> None:
        """
        For each table, create a TSV and upload it to Terra. Returns a dict of table_name: upload response.
        """
        for table_name, rows in self.collected_tables.items():
            tsv_path = self._dicts_to_tsv(table_name, rows)
            logging.info(f"Uploading {tsv_path}")
            self.terra_workspace.upload_metadata_to_workspace_table(tsv_path)


if __name__ == '__main__':
    # Parse arguments
    args = get_args()
    billing_project = args.billing_project
    workspace_name = args.workspace_name
    dataset_id = args.dataset_id
    force_upload = args.force
    tables_to_ignore = args.tables_to_ignore
    table_prefix_to_ignore = args.table_prefix_to_ignore

    # Set up authentication and request utilities
    token = Token()
    request_util = RunRequest(token=token)
    tdr = TDR(request_util=request_util)

    # Initialize TerraWorkspace once and pass to fetcher
    terra_workspace = TerraWorkspace(
        billing_project=billing_project,
        workspace_name=workspace_name,
        request_util=request_util
    )
    # Get existing tables in the Terra workspace
    existing_table_info = terra_workspace.get_workspace_entity_info().json()
    if existing_table_info is not None and len(existing_table_info) > 0:
        logging.info(f"Found {len(existing_table_info)} tables in the workspace {billing_project}/{workspace_name}.")
        if not force_upload:
            raise Exception("Tables already exist in the workspace. Use --force to overwrite them.")
        else:
            logging.info("Force upload is set. Proceeding to recreate tables.")

    # Fetch file IDs and paths from the workspace bucket
    file_id_to_path_dict = WorkspaceFileFetcher(terra_workspace).get_file_id_to_path_dict()

    # Collect table data with file paths
    collected_tables = TDRTableDataCollector(
        tdr=tdr,
        dataset_id=dataset_id,
        file_id_to_path_dict=file_id_to_path_dict,
        tables_to_ignore=tables_to_ignore,
        prefix_to_ignore=table_prefix_to_ignore
    ).collect_tables()

    # Upload collected tables to Terra
    TerraTableUploader(
        collected_tables=collected_tables,
        terra_workspace=terra_workspace
    ).upload_all_tables()
