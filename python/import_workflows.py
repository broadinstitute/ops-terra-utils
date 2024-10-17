import logging
import argparse
from typing import Any
from utils.terra_utils.terra_util import TerraWorkspace
from utils.request_util import RunRequest
from utils.token_util import Token
from utils.terra_utils.terra_workflow_configs import WorkflowConfigs

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Get files that are not in the dataset metadata")
    parser.add_argument("--billing_project", "-b", required=True)
    parser.add_argument("--workspace_name", "-w", required=True)
    parser.add_argument(
        "--workflow_list",
        required=True,
        choices=WorkflowConfigs().list_workflows(),
        help="Workflows to import into specified workspace. --workflow_list Workflow1 Workflow2 ...",
        nargs='+'
    )
    return parser.parse_args()


def workflow_already_in_workspace(workflow_name: str, workspace_workflows: list[dict]) -> Any:
    return any(workflow_name == workflow['name'] for workflow in workspace_workflows if workflow['methodRepoMethod']['methodPath'] == f"github.com/broadinstitute/ops-terra-utils/{workflow_name}")  # noqa: E501


if __name__ == '__main__':
    args = get_args()
    auth_token = Token(cloud='gcp')
    request_util = RunRequest(token=auth_token)
    workflows_to_import = args.workflow_list
    workspace = TerraWorkspace(billing_project=args.billing_project,
                               workspace_name=args.workspace_name, request_util=request_util)
    imported_workflows = workspace.get_workspace_workflows()
    for workflow in workflows_to_import:
        if not workflow_already_in_workspace(workflow_name=workflow, workspace_workflows=imported_workflows):
            logging.info(f"Importing {workflow} into {args.billing_project}/{args.workspace_name}")
            workflow_config = getattr(WorkflowConfigs(), workflow)(billing_project=args.billing_project)
            status_code = workspace.import_workflow(workflow_dict=workflow_config)
            if status_code == 201:
                logging.info(
                    f"Successfully started import for workflow '{workflow}' into workspace '{args.workspace_name}'"
                )
            else:
                logging.info(
                    f"Got the following status code when attempting to import workflow '{workflow}': '{status_code}'"
                )
        else:
            logging.info(f"{workflow} already in workspace, skipping import")
