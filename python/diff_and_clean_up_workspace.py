import logging
import os
from argparse import ArgumentParser, Namespace
from typing import Optional

from utils.tdr_utils.tdr_api_utils import TDR
from utils.requests_utils.request_util import RunRequest
from utils.token_util import Token
from utils.terra_utils.terra_util import TerraWorkspace
from utils.bq_utils import BigQueryUtil
from utils import GCP, comma_separated_list
from utils.gcp_utils import GCPCloudFunctions

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)
WORKSPACE_ONLY = "workspace_only_file"
FILES_IN_BOTH = "files_in_both"

WDL_NAME_TO_IGNORE = "/call-DiffAndCleanUpWorkspace/"


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="""For cleanup of staging workspace""")
    parser.add_argument("-d", "--dataset_id", required=True, help="ID of dataset linked with workspace"
                        )
    parser.add_argument("-b", "--billing_project", required=True, help="billing project of workspace")
    parser.add_argument("-n", "--workspace_name", required=True, help="workspace name")
    parser.add_argument("-i", "--file_paths_to_ignore", type=comma_separated_list,
                        help="comma seperated list of gcp paths to ignore (recursively) in workspace. Not required")
    parser.add_argument("-cd", "--cloud_directory", required=True,
                        help="Cloud directory to write output files")
    parser.add_argument("-r", "--delete_from_workspace", choices=[WORKSPACE_ONLY, FILES_IN_BOTH],
                        help="If not provided, will only make list of files to delete. "
                             "If provided, will delete files from workspace")
    parser.add_argument("-g", "--gcp_project",
                        help="Optional GCP project to use. If requester pays is turned on will be needed")
    return parser.parse_args()


class GetFileLists:
    def __init__(
            self,
            terra_workspace: TerraWorkspace,
            dataset_id: str,
            tdr_util: TDR,
            gcp_util: GCPCloudFunctions,
            self_hosted: bool,
            file_paths_to_ignore: Optional[list[str]] = None
    ):
        self.terra_workspace = terra_workspace
        self.tdr_util = tdr_util
        self.gcp_util = gcp_util
        self.file_paths_to_ignore = file_paths_to_ignore
        self.dataset_id = dataset_id
        self.self_hosted = self_hosted

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

    def get_files_to_delete(self) -> tuple[list[str], list[str]]:
        self_hosted = self._is_self_hosted()
        dataset_source_files = GetDatasetSourceFiles(
            dataset_id=self.dataset_id,
            tdr_util=self.tdr_util,
            self_hosted=self_hosted
        ).run()
        workspace_files_to_compare = self._get_workspace_files_to_compare()
        logging.info(f"Found {len(workspace_files_to_compare)} files in workspace after filtering out paths to ignore")
        files_in_workspace_only = list(set(workspace_files_to_compare) - set(dataset_source_files))
        logging.info(f"Found {len(files_in_workspace_only)} files in workspace that are not in dataset")
        files_in_both = list(set(workspace_files_to_compare) & set(dataset_source_files))
        logging.info(f"Found {len(files_in_both)} files in both workspace and dataset")
        return files_in_workspace_only, files_in_both


class GetDatasetSourceFiles:
    def __init__(self, dataset_id: str, tdr_util: TDR, self_hosted: bool):
        self.dataset_id = dataset_id
        self.tdr_util = tdr_util
        self.self_hosted = self_hosted

    def _get_load_history_table(self) -> dict:
        dataset_info = self.tdr_util.get_dataset_info(dataset_id=self.dataset_id)
        dataset_google_project = dataset_info["dataProject"]
        dataset_name = dataset_info["name"]
        load_history_table = f"{dataset_google_project}.datarepo_{dataset_name}.datarepo_load_history"
        bq_util = BigQueryUtil()
        query = f"""SELECT source_name, target_path, file_id
            FROM `{load_history_table}`
            where state = 'succeeded'"""
        logging.info(f"Getting load history table: {load_history_table}")
        results = bq_util.query_table(query=query)
        return {
            row['file_id']: row
            for row in results
        }

    def _get_non_self_hosted_source_files(self) -> list[str]:
        load_history_table_dict = self._get_load_history_table()
        logging.info(f"Found {len(load_history_table_dict)} files in load history table")
        dataset_files = self.tdr_util.get_dataset_files(self.dataset_id)
        logging.info(f"Found {len(dataset_files)} files in dataset files using API")
        return [
            load_history_table_dict[file_dict['fileId']]['source_name']
            for file_dict in dataset_files
            if file_dict['fileId'] in load_history_table_dict
        ]

    def _get_self_hosted_source_files(self) -> list[str]:
        return [
            file_dict['fileDetail']['accessUrl']
            for file_dict in self.tdr_util.get_dataset_files(dataset_id=self.dataset_id)
        ]

    def run(self) -> list[str]:
        if self.self_hosted:
            return self._get_self_hosted_source_files()
        return self._get_non_self_hosted_source_files()


if __name__ == '__main__':
    args = get_args()
    dataset_id = args.dataset_id
    billing_project = args.billing_project
    workspace_name = args.workspace_name
    file_paths_to_ignore = args.file_paths_to_ignore
    cloud_directory = args.cloud_directory
    delete_from_workspace = args.delete_from_workspace
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

    self_hosted = tdr_util.get_dataset_info(dataset_id=dataset_id)['selfHosted']

    files_in_workspace_only, files_in_both = GetFileLists(
        terra_workspace=terra_workspace,
        dataset_id=dataset_id,
        tdr_util=tdr_util,
        gcp_util=gcp_utils,
        file_paths_to_ignore=file_paths_to_ignore,
        self_hosted=self_hosted
    ).get_files_to_delete()

    to_delete = []

    if files_in_workspace_only:
        output_file = os.path.join(cloud_directory, 'file_in_workspace_only.txt')
        logging.info(f"Writing {len(files_in_workspace_only)} files in workspace only to {output_file}")
        with open(output_file, 'w') as f:
            f.write("\n".join(files_in_workspace_only))
        if delete_from_workspace and delete_from_workspace == WORKSPACE_ONLY:
            to_delete.extend(files_in_workspace_only)

    if files_in_both:
        output_file = os.path.join(cloud_directory, 'file_in_both.txt')
        logging.info(f"Writing {len(files_in_both)} files in both to {output_file}")
        with open(output_file, 'w') as f:
            f.write("\n".join(files_in_both))
        if delete_from_workspace and delete_from_workspace == FILES_IN_BOTH:
            if self_hosted:
                raise Exception(
                    "Cannot delete self hosted source files in both because files in workspace are only copy, "
                    "files in dataset are just references"
                )
            to_delete.extend(files_in_both)

    # Will only be list if delete_from_workspace is provided and files exist to clean up
    if to_delete:
        logging.info(f"Deleting {len(to_delete)} files from workspace which are {delete_from_workspace}")
        GCPCloudFunctions(project=gcp_project).delete_multiple_files(files_to_delete=to_delete)
