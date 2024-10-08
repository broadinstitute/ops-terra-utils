from typing import List, Optional
import logging
from typing import Any
from argparse import Namespace, ArgumentParser

from utils import GCP, comma_separated_list
from utils.terra_utils.terra_util import TerraWorkspace
from utils.request_util import RunRequest
from utils.token_util import Token
from utils.csv_util import Csv
from utils.gcp_utils import GCPCloudFunctions

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

DEFAULT_WORKERS = 10


def get_args() -> Namespace:
    parser = ArgumentParser(description='Clone a Terra workspace')
    parser.add_argument('--source_billing_project', "-sb", type=str, required=True)
    parser.add_argument('--source_workspace_name', "-sn", type=str, required=True)
    parser.add_argument('--dest_billing_project', "-db", type=str, required=True)
    parser.add_argument('--dest_workspace_name', "-dn", type=str, required=True)
    parser.add_argument('--allow_already_created', "-a", action="store_true",
                        help="Allow the destination workspace to already exist")
    parser.add_argument('--workers', "-w", type=int, default=DEFAULT_WORKERS,
                        help="Number of workers to use when copying files")
    parser.add_argument('--extensions_to_ignore', "-i", type=comma_separated_list,
                        help="comma separated list of file extensions to ignore when copying files")
    parser.add_argument('--batch_size', "-b", type=int,
                        help="Number of files validate and copy at a time. If not specified, all files will be copied at once")
    return parser.parse_args()


class CreateEntityTsv:
    def __init__(self, src_bucket: str, dest_bucket: str, source_workspace: TerraWorkspace):
        self.src_bucket = src_bucket
        self.source_workspace = source_workspace
        self.dest_bucket = dest_bucket

    def _update_cell_value(self, cell_value: Any) -> Any:
        if isinstance(cell_value, str):
            return cell_value.replace(self.src_bucket, self.dest_bucket)
        # If the cell value is a list, recursively call this function on each element of the list
        if isinstance(cell_value, list):
            return [
                self._update_cell_value(value)
                for value in cell_value
            ]
        if isinstance(cell_value, dict):
            # If cell is dict where it links to participant just upload the participant name
            # and not the whole dict
            entity_type = cell_value.get("entityType")
            if entity_type == 'participant':
                return cell_value['entityName']
        return cell_value

    def _update_row_info(self, row_dict: dict, row_id_header: str) -> dict:
        new_row_dict = {
            # If path points to the source bucket, replace it with the destination bucket
            attribute_key: self._update_cell_value(attribute_value)
            for attribute_key, attribute_value in row_dict["attributes"].items()
        }
        # Add the row id to the new row dict
        new_row_dict[row_id_header] = row_dict["name"]
        return new_row_dict

    def run(self) -> list[str]:
        tsv_to_upload = []
        entity_info = self.source_workspace.get_workspace_entity_info()
        for table_name in entity_info:
            headers = entity_info[table_name]["attributeNames"]
            row_id_header = f'entity:{entity_info[table_name]["idName"]}'
            table_metadata = self.source_workspace.get_gcp_workspace_metrics(entity_type=table_name)
            updated_table_metadata = [
                self._update_row_info(row_dict=row, row_id_header=row_id_header)
                for row in table_metadata
            ]
            tsv = f"{table_name}.tsv"
            # Make sure row id header is at the beginning of the headers list
            headers.insert(0, row_id_header)
            Csv(file_path=tsv, delimiter="\t").create_tsv_from_list_of_dicts(
                header_list=headers,
                list_of_dicts=updated_table_metadata
            )
            tsv_to_upload.append(tsv)
        return tsv_to_upload


class CopyFilesToDestWorkspace:
    def __init__(self, src_bucket: str, dest_bucket: str, workers: int, extensions_to_ignore: list[str] = [],
                 batch_size: Optional[int] = None):
        self.src_bucket = src_bucket
        self.dest_bucket = dest_bucket
        self.extensions_to_ignore = extensions_to_ignore
        self.gcp_cloud_functions = GCPCloudFunctions()
        self.workers = workers
        self.batch_size = batch_size

    def run(self) -> None:
        logging.info(f"Getting all files from source bucket {self.src_bucket}")
        list_bucket_contents = self.gcp_cloud_functions.list_bucket_contents(
            bucket_name=self.src_bucket,
            file_extensions_to_ignore=self.extensions_to_ignore
        )

        files_to_copy = [
            {
                "source_file": file_info['path'],
                "full_destination_path": file_info['path'].replace(self.src_bucket, self.dest_bucket)
            }
            for file_info in list_bucket_contents
        ]

        if not files_to_copy:
            logging.info("No files to copy")
            return

        # If a batch size is specified, break files into chunks
        if self.batch_size:
            file_batches = self._batch_files(files_to_copy, self.batch_size)
        else:
            file_batches = [files_to_copy]  # Process everything at once if no batch size is given

        # Process each batch separately
        for i, batch in enumerate(file_batches):
            logging.info(
                f"Copying batch {i + 1}/{len(file_batches)} with {len(batch)} files to destination bucket {self.dest_bucket}")
            self.gcp_cloud_functions.multithread_copy_of_files_with_validation(
                files_to_move=batch,
                workers=self.workers,
                max_retries=5
            )

    @staticmethod
    def _batch_files(files: List[dict], batch_size: int) -> List[List[dict]]:
        """Helper function to split a list of files into batches."""
        return [files[i:i + batch_size] for i in range(0, len(files), batch_size)]


class UpdateWorkspaceAcls:
    def __init__(self, src_workspace: TerraWorkspace, dest_workspace: TerraWorkspace):
        self.src_workspace = src_workspace
        self.dest_workspace = dest_workspace

    def run(self) -> None:
        # Get the source workspace ACLs and entities
        src_workspace_acls = self.src_workspace.get_workspace_acl()
        # Convert the source workspace ACLs to a list of dictionaries
        src_workspace_acls_list = [
            {
                "email": user,
                "accessLevel": user_acl["accessLevel"],
                "canShare": user_acl["canShare"],
                "canCompute": user_acl["canCompute"]
            }
            for user, user_acl in src_workspace_acls["acl"].items()
        ]
        self.dest_workspace.update_multiple_users_acl(acl_list=src_workspace_acls_list)


if __name__ == '__main__':
    args = get_args()
    source_billing_project = args.source_billing_project
    source_workspace_name = args.source_workspace_name
    dest_billing_project = args.dest_billing_project
    dest_workspace_name = args.dest_workspace_name
    allow_already_created = args.allow_already_created
    workers = args.workers
    extensions_to_ignore = args.extensions_to_ignore
    batch_size = args.batch_size

    token = Token(cloud=GCP)
    request_util = RunRequest(token=token)
    # Initialize the source Terra workspace classes
    src_workspace = TerraWorkspace(
        billing_project=source_billing_project,
        workspace_name=source_workspace_name,
        request_util=request_util
    )
    # Initialize the destination Terra workspace classes
    dest_workspace = TerraWorkspace(
        billing_project=dest_billing_project,
        workspace_name=dest_workspace_name,
        request_util=request_util
    )

    # Get the source workspace info
    src_workspace_info = src_workspace.get_workspace_info()
    src_auth_domain = src_workspace_info["workspace"]["authorizationDomain"]
    src_bucket = src_workspace_info["workspace"]["bucketName"]

    # Separate the source workspace attributes into src and library attributes
    src_attributes = {}
    library_attributes = {}
    for k, v in src_workspace_info['workspace']['attributes'].items():
        if k.startswith('library:'):
            library_attributes[k] = v
        else:
            src_attributes[k] = v

    # Create the destination workspace
    dest_workspace.create_workspace(
        attributes=src_attributes,
        auth_domain=src_auth_domain,
        continue_if_exists=allow_already_created
    )

    # Add the library attributes to the destination workspace if they exist
    if library_attributes:
        dest_workspace.put_metadata_for_library_dataset(library_metadata=library_attributes)

    dest_workspace_info = dest_workspace.get_workspace_info()
    dest_bucket = dest_workspace_info["workspace"]["bucketName"]

    # Get source workspace metadata
    tsvs_to_upload = CreateEntityTsv(
        src_bucket=src_bucket,
        dest_bucket=dest_bucket,
        source_workspace=src_workspace
    ).run()

    for tsv in tsvs_to_upload:
        logging.info(f"Uploading {tsv} to destination workspace")
        dest_workspace.upload_metadata_to_workspace_table(entities_tsv=tsv)

    CopyFilesToDestWorkspace(
        src_bucket=src_bucket,
        dest_bucket=dest_bucket,
        extensions_to_ignore=extensions_to_ignore,
        workers=workers,
        batch_size=batch_size
    ).run()

    # Set the destination workspace ACLs
    UpdateWorkspaceAcls(src_workspace=src_workspace, dest_workspace=dest_workspace).run()
