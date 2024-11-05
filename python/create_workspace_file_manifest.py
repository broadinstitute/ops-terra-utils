from utils.terra_utils.terra_util import TerraWorkspace
from utils.gcp_utils import GCPCloudFunctions
from utils.request_util import RunRequest
from utils.token_util import Token
from utils import GCP, comma_separated_list
import csv

import logging
from argparse import ArgumentParser, Namespace

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

ENTITY_FILE_PATH = "entities.tsv"


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="Get information for files in workspace bucket and upload metata to file_metadata table")
    parser.add_argument("--workspace_name", "-w", required=True, type=str)
    parser.add_argument("--billing_project", "-b", required=True, type=str)
    parser.add_argument("--strings_to_exclude", type=comma_separated_list,
                        help="comma seperated list of strings to exclude from file paths to report")
    search_options = parser.add_argument_group('Search Options', 'Modifiers to file search functionality')
    file_list_options = search_options.add_mutually_exclusive_group(required=False)
    file_list_options.add_argument("--extension_exclude_list", "-El", type=comma_separated_list,
                                   help="list of file extensions to be excluded from \
                                    data loaded into the file metadata table")
    file_list_options.add_argument("--extension_include_list", "-Il", type=comma_separated_list,
                                   help="list of file extensions to include in \
                                   data loaded into the file metadata table,\
                                   all other file extensions wil be ignored")
    return parser.parse_args()


def write_entities_tsv(file_dicts: list[dict]) -> None:
    headers = ['entity:file_metadata_id', 'file_path', 'file_name',
               'content_type', 'file_extension', 'size_in_bytes', 'md5_hash']
    logging.info("writing file metadata to entities.tsv")
    with open(ENTITY_FILE_PATH, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for file_dict in file_dicts:
            file_id = file_dict['path'].removeprefix("gs://").replace('/', '_')
            formatted_dict = {'entity:file_metadata_id': file_id,
                              "file_path": f"{file_dict['path']}",
                              'file_name': f"{file_dict['name']}",
                              'content_type': f"{file_dict['content_type']}",
                              'file_extension': f"{file_dict['file_extension']}",
                              'size_in_bytes': f"{file_dict['size_in_bytes']}",
                              'md5_hash': f"{file_dict['md5_hash']}"}
            writer.writerow(formatted_dict)


if __name__ == '__main__':
    args = get_args()
    billing_project = args.billing_project
    workspace_name = args.workspace_name
    strings_to_exclude = args.strings_to_exclude
    extension_exclude_list = args.extension_exclude_list
    extension_include_list = args.extension_include_list

    auth_token = Token(cloud=GCP)
    request_util = RunRequest(token=auth_token)

    workspace = TerraWorkspace(billing_project=billing_project,
                               workspace_name=workspace_name, request_util=request_util)
    gcp_bucket = workspace.get_workspace_bucket()
    logging.info(
        f"getting all files in bucket for workspace {billing_project}/{workspace_name}: {gcp_bucket}")
    workspace_files = GCPCloudFunctions().list_bucket_contents(
        bucket_name=gcp_bucket,
        file_strings_to_ignore=strings_to_exclude,
        file_extensions_to_ignore=extension_exclude_list,
        file_extensions_to_include=extension_include_list
    )
    logging.info(f"Found {len(workspace_files)} files in bucket")
    write_entities_tsv(workspace_files)
    metadata_upload = workspace.upload_metadata_to_workspace_table(entities_tsv=ENTITY_FILE_PATH)
