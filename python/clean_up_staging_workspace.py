import logging
import sys
from argparse import ArgumentParser, Namespace
from typing import Optional

from utils.tdr_utils.tdr_api_utils import TDR
from utils.requests_utils.request_util import RunRequest
from utils.token_util import Token
from utils.terra_utils.terra_util import TerraWorkspace
from utils import GCP, comma_separated_list
from utils.gcp_utils import GCPCloudFunctions

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

WDL_NAME_TO_IGNORE = "/call-CleanUpStagingWorkspace/"


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="""For cleanup of staging workspace""")
    parser.add_argument("-d", "--dataset_id", required=True, help="ID of dataset linked with workspace"
                        )
    parser.add_argument("-b", "--billing_project", required=True, help="billing project of workspace")
    parser.add_argument("-n", "--workspace_name", required=True, help="workspace name")
    parser.add_argument("-i", "--file_paths_to_ignore", type=comma_separated_list,
                        help="comma seperated list of gcp paths to ignore (recursively) in workspace. Not required")
    parser.add_argument("-o", "--output_file", required=True,
                        help="output file to write the paths to delete.")
    parser.add_argument("-r", "--run_delete", action="store_true",
                        help="If not provided, will only print paths to delete")
    parser.add_argument("-g", "--gcp_project",
                        help="Optional GCP project to use. If requester pays is turned on will be needed")
    return parser.parse_args()


class GetFilesToDelete:
    def __init__(
            self,
            terra_workspace: TerraWorkspace,
            dataset_id: str,
            tdr_util: TDR,
            gcp_util: GCPCloudFunctions,
            file_paths_to_ignore: Optional[list[str]] = None
    ):
        self.terra_workspace = terra_workspace
        self.tdr_util = tdr_util
        self.gcp_util = gcp_util
        self.file_paths_to_ignore = file_paths_to_ignore
        self.dataset_id = dataset_id

    def _is_self_hosted(self) -> bool:
        dataset_info = self.tdr_util.get_dataset_info(dataset_id=self.dataset_id)
        return dataset_info['selfHosted']

    def _get_workspace_files_to_compare(self) -> list[str]:
        return [
            file_dict['path']
            for file_dict in self.gcp_util.list_bucket_contents(
                bucket_name=self.terra_workspace.get_workspace_bucket()
            )
            # Filter out paths to ignore if provided
            if (not self.file_paths_to_ignore or
                not any(file_dict['path'].startswith(ignore) for ignore in self.file_paths_to_ignore)
                )
            # Filter out paths that have the WDL_NAME_TO_IGNORE in them
            or WDL_NAME_TO_IGNORE in file_dict['path']
        ]

    def _get_dataset_files(self) -> list[str]:
        return [
            file_dict['fileDetail']['accessUrl']
            for file_dict in self.tdr_util.get_dataset_files(dataset_id=dataset_id)
        ]

    def get_files_to_delete(self) -> list[str]:
        if not self._is_self_hosted():
            logging.error("Dataset is not self hosted, this is only for self hosted datasets")
            sys.exit(1)

        dataset_files = self._get_dataset_files()
        workspace_files_to_compare = self._get_workspace_files_to_compare()
        logging.info(f"Found {len(workspace_files_to_compare)} files in workspace after filtering out paths to ignore")
        return list(set(workspace_files_to_compare) - set(dataset_files))


if __name__ == '__main__':
    args = get_args()
    dataset_id = args.dataset_id
    billing_project = args.billing_project
    workspace_name = args.workspace_name
    file_paths_to_ignore = args.file_paths_to_ignore
    output_file = args.output_file
    run_deletes = args.run_delete
    gcp_project = args.gcp_project

    token = Token(cloud=GCP)
    request_util = RunRequest(token=token)
    tdr_util = TDR(request_util=request_util)
    terra_workspace = TerraWorkspace(
        billing_project=billing_project,
        workspace_name=workspace_name,
        request_util=request_util
    )
    gcp_utils = GCPCloudFunctions(project=gcp_project)

    files_to_delete = GetFilesToDelete(
        terra_workspace=terra_workspace,
        dataset_id=dataset_id,
        tdr_util=tdr_util,
        gcp_util=gcp_utils,
        file_paths_to_ignore=file_paths_to_ignore
    ).get_files_to_delete()

    logging.info(
        f"Found {len(files_to_delete)} files to delete in {billing_project}/{workspace_name} that are "
        f"not in dataset {dataset_id}"
    )

    if files_to_delete:
        with open(output_file, "w") as f:
            f.write("\n".join(files_to_delete))
        logging.info(f"Paths to delete written to {output_file}")
        if run_deletes:
            logging.info(f"Deleting {len(files_to_delete)} files")
            gcp_utils.delete_multiple_files(files_to_delete=files_to_delete)
        else:
            logging.info("Run with -r flag to delete files")
    else:
        logging.info("No files to delete")
