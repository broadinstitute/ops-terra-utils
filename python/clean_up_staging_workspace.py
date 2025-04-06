import logging
import sys
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
        self_hosted = self._is_self_hosted()
        dataset_source_files = GetDatasetSourceFiles(
            dataset_id=self.dataset_id,
            tdr_util=self.tdr_util,
            self_hosted=self_hosted
        ).run()
        workspace_files_to_compare = self._get_workspace_files_to_compare()
        logging.info(f"Found {len(workspace_files_to_compare)} files in workspace after filtering out paths to ignore")
        if self_hosted:
            # Return files that are in the workspace but not in the dataset since those files
            # since self hosted so those files are NOT uploaded
            return list(set(workspace_files_to_compare) - set(dataset_source_files))
        else:
            # Return files that are in the dataset and in the workspace.
            # Since the dataset is not self hosted, we want to delete files that are in the dataset since
            # we are paying twice.
            return list(set(workspace_files_to_compare) & set(dataset_source_files))


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
