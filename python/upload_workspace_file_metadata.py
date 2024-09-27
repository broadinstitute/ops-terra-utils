from utils.terra_util import TerraWorkspace
from utils.gcp_utils import GCPCloudFunctions
from utils.request_util import RunRequest
from utils.token_util import Token
import csv

import logging
from argparse import ArgumentParser, Namespace

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="Get information for files in workspace bucket and upload metata to file_metadata table")
    parser.add_argument("--workspace_name", "-w", required=True, type=str)
    parser.add_argument("--billing_project", "-b", required=True, type=str)
    search_options = parser.add_argument_group('Search Options', 'Modifiers to file search functionality')
    file_list_options = search_options.add_mutually_exclusive_group(required=False)
    file_list_options.add_argument("--extension_exclude_list", "-El", type=str,
                                   help="list of file extensions to be excluded from data loaded into the file metadata table")
    file_list_options.add_argument("--extension_include_list", "-Il", type=str,
                                   help="list of file extensions to include in data loaded into the file metadata table, all other file extensions wil be ignored")
    return parser.parse_args()


def write_entities_tsv(file_dicts: list[dict]) -> None:
    headers = ['entity:file_metadata_id', 'file_path', 'file_name',
               'content_type', 'file_extension', 'size_in_bytes', 'md5_hash']
    logging.info(f"writing file metadata to entities.tsv")
    with open('entities.tsv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for file_dict in file_dicts:
            file_id = file_dict['path'].removeprefix("gs://").replace('/', '_')
            formatted_dict = {'entity:file_metadata_id': file_id, "file_path": f"{file_dict['path']}", 'file_name': f"{file_dict['name']}", 'content_type': f"{file_dict['content_type']}",
                              'file_extension': f"{file_dict['file_extension']}", 'size_in_bytes': f"{file_dict['size_in_bytes']}", 'md5_hash': f"{file_dict['md5_hash']}"}
            writer.writerow(formatted_dict)


if __name__ == '__main__':
    args = get_args()
    if args.extension_exclude_list:
        args.extension_exclude_list = [extension.strip() for extension in args.extension_exclude_list.split(',')]
    if args.extension_include_list:
        args.extension_include_list = [extension.strip() for extension in args.extension_include_list.split(',')]

    auth_token = Token(cloud='gcp')
    request_util = RunRequest(token=auth_token)

    workspace = TerraWorkspace(billing_project=args.billing_project,
                               workspace_name=args.workspace_name, request_util=request_util)
    gcp_bucket = workspace.get_workspace_bucket()
    logging.info(
        f"getting all files in bucket for workspace {args.billing_project}/{args.workspace_name}: {gcp_bucket}")
    workspace_files = GCPCloudFunctions().list_bucket_contents(bucket_name=gcp_bucket)
    logging.info(f"Found {len(workspace_files)} files in bucket")
    write_entities_tsv(workspace_files)
    metadata_upload = workspace.upload_data_to_workspace_table(entities_tsv='entities.tsv')
