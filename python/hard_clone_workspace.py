from typing import Optional
import logging
from typing import Any
from argparse import Namespace, ArgumentParser

from ops_utils.vars import ARG_DEFAULTS
from ops_utils import comma_separated_list
from ops_utils.terra_util import TerraWorkspace
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils.csv_util import Csv
from ops_utils.gcp_utils import GCPCloudFunctions

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

DEST_BUCKET_FILE = "dest_workspace_bucket.txt"
SOURCE_BUCKET_FILE = "source_workspace_bucket.txt"
MAX_TIME_TO_CHECK_FOR_PERMISSIONS = 5  # in hours


def get_args() -> Namespace:
    parser = ArgumentParser(description='Clone a Terra workspace')
    parser.add_argument('--source_billing_project', "-sb", type=str, required=True)
    parser.add_argument('--source_workspace_name', "-sn", type=str, required=True)
    parser.add_argument('--dest_billing_project', "-db", type=str, required=True)
    parser.add_argument('--dest_workspace_name', "-dn", type=str, required=True)
    parser.add_argument('--google_project', "-p", type=str)
    parser.add_argument(
        '--terra_google_project_id',
        type=str,
        required=False,
        help="If the bucket linked to your source workspace has the requester pays setting turned on,"
             " the Terra Google project ID to use is required."
    )
    parser.add_argument('--external_bucket', "-eb", type=str,
                        help="gcp bucket if you want to store files in bucket outside of workspace gs://bucket/")
    parser.add_argument('--allow_already_created', "-a", action="store_true",
                        help="Allow the destination workspace to already exist")
    parser.add_argument('--workers', "-w", type=int, default=ARG_DEFAULTS['multithread_workers'],
                        help="Number of workers to use when copying files. " +
                             f"Defaults to {ARG_DEFAULTS['multithread_workers']}")
    parser.add_argument('--extensions_to_ignore', "-i", type=comma_separated_list,
                        help="comma separated list of file extensions to ignore when copying files")
    parser.add_argument('--batch_size', "-b", type=int,
                        help="Number of files validate and copy at a time. If not specified, "
                             "all files will be copied at once")
    parser.add_argument('--do_not_update_acls', action="store_true",
                        help="Do not update the destination workspace ACLs with the source workspace ACLs. " +
                             "If you do not have owner access of the source workspace, you should use this flag.")
    parser.add_argument(
        "--check_and_wait_for_permissions",
        action="store_true",
        help="When used, workflow will check write permissions on destination bucket every 30 minutes for 5 hours"
             " before exiting. Useful for when permissions were newly added and could take some time to propagate"
    )
    parser.add_argument(
        "--max_permissions_wait_time",
        type=int,
        required=False,
        default=MAX_TIME_TO_CHECK_FOR_PERMISSIONS,
        help=f"Max time to wait for permissions before exiting. Defaults to {MAX_TIME_TO_CHECK_FOR_PERMISSIONS} hours "
             f"if not provided. Cannot be more than {MAX_TIME_TO_CHECK_FOR_PERMISSIONS} hours."
    )
    parser.add_argument(
        "--skip_check_if_already_copied",
        action="store_true",
        help="Skip checking if files have already been copied to the destination bucket and start copying immediately."
    )

    return parser.parse_args()


class CreateEntityTsv:
    def __init__(self, src_bucket: str, dest_bucket: str, source_workspace: TerraWorkspace):
        self.src_bucket = src_bucket
        self.source_workspace = source_workspace
        self.dest_bucket = dest_bucket

    def _update_cell_value(self, cell_value: Any) -> Any:
        if isinstance(cell_value, list):
            if all(isinstance(item, str) for item in cell_value):
                return '["' + '","'.join(
                    [
                        self._update_cell_value(entity)
                        for entity in cell_value
                    ]
                ) + '"]'
            else:
                return cell_value
        elif isinstance(cell_value, str):
            return cell_value.replace(self.src_bucket, self.dest_bucket)

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
        entity_info = self.source_workspace.get_workspace_entity_info().json()
        for table_name in entity_info:
            headers = entity_info[table_name]["attributeNames"]
            row_id_header = f'entity:{entity_info[table_name]["idName"]}'
            table_metadata = self.source_workspace.get_gcp_workspace_metrics(entity_type=table_name, remove_dicts=True)
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
    def __init__(
            self,
            src_bucket: str,
            dest_bucket: str,
            workers: int,
            skip_check_if_already_copied: bool,
            gcp_cloud_functions: GCPCloudFunctions,
            extensions_to_ignore: list[str] = [],
            batch_size: Optional[int] = None
    ):
        self.src_bucket = src_bucket
        self.dest_bucket = dest_bucket
        self.extensions_to_ignore = extensions_to_ignore
        self.gcp_cloud_functions = gcp_cloud_functions
        self.workers = workers
        self.batch_size = batch_size
        self.skip_check_if_already_copied = skip_check_if_already_copied

    def run(self) -> None:
        logging.info(f"Getting all files from source bucket {self.src_bucket}")
        list_bucket_contents = self.gcp_cloud_functions.list_bucket_contents(
            bucket_name=self.src_bucket,
            file_extensions_to_ignore=self.extensions_to_ignore,
            file_name_only=True,
            # Ignore log files for this workflow since could be updating as running
            file_strings_to_ignore=["/HardCloneTerraWorkspace/", "/HardCloneWithExternalBucket/"]
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
            file_batches = self._batch_files(files_to_copy)
        else:
            file_batches = [files_to_copy]  # Process everything at once if no batch size is given

        # Process each batch separately
        for i, batch in enumerate(file_batches):
            logging.info(
                f"Starting validation on batch {i + 1}/{len(file_batches)} with {len(batch)} files to "
                f"be copied to destination bucket: '{self.dest_bucket}'")
            self.gcp_cloud_functions.multithread_copy_of_files_with_validation(
                files_to_copy=batch,
                workers=self.workers,
                max_retries=5,
                skip_check_if_already_copied=self.skip_check_if_already_copied
            )

    def _batch_files(self, files: list[dict]) -> list[list[dict]]:
        """Helper function to split a list of files into batches."""
        return [
            files[i:i + self.batch_size]  # type: ignore[operator]
            for i in range(0, len(files), self.batch_size)  # type: ignore[arg-type]
        ]


class UpdateWorkspaceAcls:
    def __init__(self, src_workspace: TerraWorkspace, dest_workspace: TerraWorkspace):
        self.src_workspace = src_workspace
        self.dest_workspace = dest_workspace

    def run(self) -> None:
        # Get the source workspace ACLs and entities
        src_workspace_acls = self.src_workspace.get_workspace_acl().json()
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
    do_not_update_acls = args.do_not_update_acls
    external_bucket = args.external_bucket
    skip_check_if_already_copied = args.skip_check_if_already_copied
    google_project = args.google_project
    terra_google_project_id = args.terra_google_project_id

    if external_bucket:
        if not external_bucket.startswith("gs://") or not external_bucket.endswith("/"):
            raise ValueError("gcp_bucket must start with gs:// and end with /")
        # Remove the gs:// prefix and trailing slash to match what is returned by the Terra API
        external_bucket = external_bucket.replace("gs://", "").rstrip("/")

    token = Token()
    request_util = RunRequest(token=token)
    # Initialize the source Terra workspace classes
    src_workspace = TerraWorkspace(
        billing_project=source_billing_project, workspace_name=source_workspace_name, request_util=request_util
    )
    # Initialize the destination Terra workspace classes
    dest_workspace = TerraWorkspace(
        billing_project=dest_billing_project, workspace_name=dest_workspace_name, request_util=request_util
    )

    # Get the source workspace info
    if terra_google_project_id:
        src_workspace_info = src_workspace.get_workspace_details(terra_google_project_id=terra_google_project_id).json()
    else:
        src_workspace_info = src_workspace.get_workspace_info().json()

    src_auth_domain = src_workspace_info["workspace"]["authorizationDomain"]
    src_bucket = src_workspace_info["workspace"]["bucketName"]

    # Gather the workspace attributes. Functionality for library attributes has been
    # deprecated, and we do not need to collect existing library attributes
    src_attributes = {}
    for k, v in src_workspace_info['workspace']['attributes'].items():
        if not k.startswith('library:'):
            src_attributes[k] = v

    # Create the destination workspace
    dest_workspace.create_workspace(
        attributes=src_attributes, auth_domain=src_auth_domain, continue_if_exists=allow_already_created
    )

    dest_workspace_info = dest_workspace.get_workspace_info().json()
    dest_bucket = dest_workspace_info["workspace"]["bucketName"]

    # Use the external bucket if it is provided, otherwise use the destination workspace bucket
    dest_bucket = external_bucket if external_bucket else dest_bucket

    gcp_util = GCPCloudFunctions(project=google_project)
    if args.check_and_wait_for_permissions:
        total_hours = (
            args.max_permissions_wait_time
            if args.max_permissions_wait_time <= MAX_TIME_TO_CHECK_FOR_PERMISSIONS
            else MAX_TIME_TO_CHECK_FOR_PERMISSIONS
        )
        gcp_util.wait_for_write_permission(
            cloud_path=f"gs://{dest_bucket}/",
            interval_wait_time_minutes=30,
            max_wait_time_minutes=total_hours * 60
        )

    # Get source workspace metadata
    tsvs_to_upload = CreateEntityTsv(
        src_bucket=src_bucket,
        dest_bucket=dest_bucket,
        source_workspace=src_workspace
    ).run()

    for tsv in tsvs_to_upload:
        logging.info(f"Uploading {tsv} to destination workspace")
        dest_workspace.upload_metadata_to_workspace_table(entities_tsv=tsv)

    # Copy files from source workspace to destination workspace
    CopyFilesToDestWorkspace(
        src_bucket=src_bucket,
        dest_bucket=dest_bucket,
        extensions_to_ignore=extensions_to_ignore,
        workers=workers,
        batch_size=batch_size,
        skip_check_if_already_copied=skip_check_if_already_copied,
        gcp_cloud_functions=gcp_util
    ).run()

    # Set the destination workspace ACLs
    if not do_not_update_acls:
        logging.info(
            "Updating destination workspace ACLs. If fails with 403 you probably do not " +
            "have access to list source workspace ACLs. try running with --do_not_update_acls")
        UpdateWorkspaceAcls(src_workspace=src_workspace, dest_workspace=dest_workspace).run()
