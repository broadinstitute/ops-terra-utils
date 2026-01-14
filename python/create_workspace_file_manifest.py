from ops_utils.terra_util import TerraWorkspace
from ops_utils.gcp_utils import GCPCloudFunctions
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils import comma_separated_list
import csv

import logging
from argparse import ArgumentParser, Namespace

from typing import Any

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

ENTITY_FILE_PATH = "entities.tsv"
MIN_ROWS_TO_CHECK_FOR_GS_PATH = 200


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="Get information for files in workspace bucket and upload metata to file_metadata table")
    parser.add_argument("--workspace_name", "-w", required=True, type=str)
    parser.add_argument("--billing_project", "-b", required=True, type=str)
    parser.add_argument("--strings_to_exclude", type=comma_separated_list,
                        help="comma seperated list of strings to exclude from file paths to report")
    parser.add_argument("--include_external_files", "-ief", action="store_true",
                        help="Include files that are referenced in the workspace but not physically present in the bucket")
    search_options = parser.add_argument_group('Search Options', 'Modifiers to file search functionality')
    file_list_options = search_options.add_mutually_exclusive_group(required=False)
    file_list_options.add_argument("--extension_exclude_list", "-el", type=comma_separated_list,
                                   help="list of file extensions to be excluded from \
                                    data loaded into the file metadata table")
    file_list_options.add_argument("--extension_include_list", "-il", type=comma_separated_list,
                                   help="list of file extensions to include in \
                                   data loaded into the file metadata table,\
                                   all other file extensions wil be ignored")
    parser.add_argument("--service_account_json", "-saj", type=str,
                        help="Path to the service account JSON file. If not provided, will use the default credentials.")
    return parser.parse_args()


def write_entities_tsv(file_dicts: list[dict]) -> None:
    headers = ['entity:file_metadata_id', 'file_path', 'file_name',
               'content_type', 'file_extension', 'size_in_bytes', 'md5_hash', 'external_file']
    logging.info("writing file metadata to entities.tsv")
    with open(ENTITY_FILE_PATH, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for file_dict in file_dicts:
            file_id = file_dict['path'].removeprefix("gs://").replace('/', '_').replace(' ', '_')
            formatted_dict = {
                'entity:file_metadata_id': file_id,
                'file_path': f"{file_dict['path']}",
                'file_name': f"{file_dict['name']}",
                'content_type': f"{file_dict['content_type']}",
                'file_extension': f"{file_dict['file_extension']}",
                'size_in_bytes': f"{file_dict['size_in_bytes']}",
                'md5_hash': f"{file_dict['md5_hash']}",
                'external_file': file_dict['external_file']
            }
            writer.writerow(formatted_dict)


class GetExternalFiles:
    def __init__(
            self,
            terra_workspace: TerraWorkspace,
            gcp_bucket: str,
            gcp_util: GCPCloudFunctions,
            extension_exclude_list: list[str] = [],
            extension_include_list: list[str] = []
    ):
        self.terra_workspace = terra_workspace
        self.gcp_bucket = gcp_bucket
        self.gcp_util = gcp_util
        self.extension_exclude_list = extension_exclude_list
        self.extension_include_list = extension_include_list

    def _extract_gs_uris(self, value: Any) -> list[str]:
        """Best-effort extraction of gs:// URIs from a cell value."""
        if value is None:
            return []

        if isinstance(value, str):
            s = value.strip()
            if s.startswith("gs://"):
                # Apply extension filters
                if self.extension_exclude_list and any(s.endswith(ext) for ext in self.extension_exclude_list):
                    return []
                if self.extension_include_list and not any(s.endswith(ext) for ext in self.extension_include_list):
                    return []
                return [value]

        if isinstance(value, (list, tuple, set)):
            uris = []
            for item in value:
                uris.extend(self._extract_gs_uris(item))
            return uris

        if isinstance(value, dict):
            uris = []
            for v in value.values():
                uris.extend(self._extract_gs_uris(v))
            return uris

        return []

    def _get_external_files_from_table_metrics(self, table: str, min_rows_to_check: int) -> list[str]:
        """Scan a Terra entity table for external gs:// paths.

        Rules per column:
          - Always scan at least `min_rows_to_check` rows.
          - If no gs:// paths are found within those rows, stop scanning that column.
          - If any gs:// paths are found within those rows, scan the entire column (all rows).
          - Collect any gs:// path that does NOT start with gs://{workspace_bucket}/.

        Returns a de-duplicated list of external gs:// URIs.
        """
        sample_metrics: list[dict] = self.terra_workspace.get_gcp_workspace_metrics(entity_type=table, remove_dicts=True)
        if not sample_metrics:
            return []

        internal_prefix = f"gs://{self.gcp_bucket}/"
        columns = list(sample_metrics[0].keys())

        external_paths: set[str] = set()
        total_rows = len(sample_metrics)
        initial_scan_count = min(min_rows_to_check, total_rows)

        for col in columns:
            # Phase 1: scan first N rows
            saw_any_gs = False
            for row in sample_metrics[:initial_scan_count]:
                for uri in self._extract_gs_uris(row.get(col)):
                    saw_any_gs = True
                    if not uri.startswith(internal_prefix):
                        external_paths.add(uri)

            # If no gs:// values at all in first N rows, stop checking this column.
            if not saw_any_gs:
                continue

            # Phase 2: gs:// was present, so scan the rest of the column.
            for row in sample_metrics[initial_scan_count:]:
                for uri in self._extract_gs_uris(row.get(col)):
                    if not uri.startswith(internal_prefix):
                        external_paths.add(uri)
        return sorted(external_paths)

    def run(self, min_rows_to_check: int = MIN_ROWS_TO_CHECK_FOR_GS_PATH) -> list[str]:
        """Collect external files referenced in any workspace entity table."""
        workspace_table_names = [
            table
            for table in self.terra_workspace.get_workspace_entity_info().json()
            if table != 'file_metadata'
        ]

        all_external = set()
        logging.info(f"Collecting external gs:// paths for {len(workspace_table_names)} tables")
        for table_name in workspace_table_names:
            all_external.update(self._get_external_files_from_table_metrics(
                table=table_name,
                min_rows_to_check=min_rows_to_check
            ))
        logging.info(f"Found {len(all_external)} unique external gs:// paths in workspace tables. Checking gcp metadata.")
        external_metadata = self.gcp_util.load_blobs_from_full_paths_multithreaded(
            full_paths=list(all_external),
            job_complete_for_logging=2000
        )
        # Mark all as external files
        for meta in external_metadata:
            meta['external_file'] = 'true'
        logging.info(f"Retrieved metadata for {len(external_metadata)} external files.")
        return external_metadata


if __name__ == '__main__':
    args = get_args()
    billing_project = args.billing_project
    workspace_name = args.workspace_name
    strings_to_exclude = args.strings_to_exclude
    extension_exclude_list = args.extension_exclude_list
    extension_include_list = args.extension_include_list
    service_account_json = args.service_account_json
    include_external_files = args.include_external_files

    auth_token = Token(service_account_json=service_account_json)
    request_util = RunRequest(token=auth_token)
    workspace = TerraWorkspace(
        billing_project=billing_project,
        workspace_name=workspace_name,
        request_util=request_util
    )
    gcp_util = GCPCloudFunctions()

    gcp_bucket = workspace.get_workspace_bucket()
    logging.info(
        f"getting all files in bucket for workspace {billing_project}/{workspace_name}: {gcp_bucket}")
    workspace_files = gcp_util.list_bucket_contents(
        bucket_name=gcp_bucket,
        file_strings_to_ignore=strings_to_exclude,
        file_extensions_to_ignore=extension_exclude_list,
        file_extensions_to_include=extension_include_list
    )
    # Mark all as internal files
    for file in workspace_files:
        file['external_file'] = 'false'
    logging.info(f"Found {len(workspace_files)} files in workspace bucket after applying filters.")
    if include_external_files:
        external_file_metadata = GetExternalFiles(
            terra_workspace=workspace,
            gcp_bucket=gcp_bucket,
            extension_exclude_list=extension_exclude_list,
            extension_include_list=extension_include_list,
            gcp_util=gcp_util
        ).run(min_rows_to_check_rows=200)
        workspace_files.extend(external_file_metadata)

    write_entities_tsv(workspace_files)
    logging.info("Uploading metadata to workspace file_metadata table")
    metadata_upload = workspace.upload_metadata_to_workspace_table(entities_tsv=ENTITY_FILE_PATH)
