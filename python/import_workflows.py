import logging
import argparse
from typing import Any
from ops_utils.terra_util import TerraWorkspace
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
from utils.terra_workflow_configs import WorkflowConfigs, GetWorkflowNames

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
        choices=GetWorkflowNames().get_workflow_names(),
        help="Workflows to import into specified workspace. --workflow_list Workflow1 Workflow2 ...",
        nargs='+'
    )
    return parser.parse_args()


def workflow_already_in_workspace(workflow_name: str, workspace_workflows: list[dict]) -> Any:
    # "methodPath" key is only available when workflows are imported via Dockstore
    return any(workflow_name == workflow['name'] for workflow in workspace_workflows if workflow['methodRepoMethod'].get("methodPath") == f"github.com/broadinstitute/ops-terra-utils/{workflow_name}")  # noqa: E501


if __name__ == '__main__':
    args = get_args()
    auth_token = Token()
    request_util = RunRequest(token=auth_token)
    workflows_to_import = args.workflow_list
    workspace = TerraWorkspace(billing_project=args.billing_project,
                               workspace_name=args.workspace_name, request_util=request_util)
    imported_workflows = workspace.get_workspace_workflows().json()
    for workflow in workflows_to_import:
        if not workflow_already_in_workspace(workflow_name=workflow, workspace_workflows=imported_workflows):
            logging.info(f"Importing {workflow} into {args.billing_project}/{args.workspace_name}")
            WorkflowConfigs(
                workflow_name=workflow,
                billing_project=args.billing_project,
                terra_workspace_util=workspace
            ).import_workflow()
        else:
            logging.info(f"{workflow} already in workspace, skipping import")
