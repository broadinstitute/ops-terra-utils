import logging
import os.path
import sys
from argparse import ArgumentParser, Namespace
from urllib.request import urlopen

from utils.token_util import Token
from utils.requests_utils.request_util import RunRequest
from utils.terra_utils.terra_util import TerraWorkspace, Terra
from utils.gcp_utils import GCPCloudFunctions
from utils import GCP
import json

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

CLOUD_TYPE = GCP
FEATURED_JSON_DIR = "gs://firecloud-alerts/"
FEATURED_WORKSPACE_JSON_PATH = os.path.join(FEATURED_JSON_DIR, "featured-workspaces.json")
SHOWCASE_JSON_PATH = os.path.join(FEATURED_JSON_DIR, "showcase.json")
FEATURED_WORKSPACE_LINK = "https://storage.googleapis.com/firecloud-alerts/featured-workspaces.json"


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="Make a GCP terra workspace featured")
    parser.add_argument("--billing_project", "-b", required=True)
    parser.add_argument("--workspace_name", "-w", required=True)
    return parser.parse_args()


class UpdateFeaturedWorkspaceJson:
    def __init__(self, billing_project: str, workspace_name: str, gcp_utils: GCPCloudFunctions):
        self.billing_project = billing_project
        self.workspace_name = workspace_name
        self.gcp_utils = gcp_utils

    def run(self) -> None:
        # Download current featured-workspaces.json
        featured_workspace_json = json.loads(self.gcp_utils.read_file(FEATURED_WORKSPACE_JSON_PATH))
        # Add the new workspace to the list
        featured_workspace_json.append({"namespace": self.billing_project, "name": self.workspace_name})
        # Write the new json back to the bucket
        self.gcp_utils.write_to_gcs(
            cloud_path=FEATURED_WORKSPACE_JSON_PATH,
            content=json.dumps(featured_workspace_json, indent=4)
        )


class ShowcaseContent:
    def __init__(self, terra: Terra, gcp_utils: GCPCloudFunctions):
        self.terra = terra
        self.gcp_utils = gcp_utils

    @staticmethod
    def _get_featured_workspaces() -> dict:
        logging.info("Fetching featured workspaces")
        return json.load(urlopen(FEATURED_WORKSPACE_LINK))

    def _get_public_workspace(self) -> list[dict]:
        logging.info("Fetching all accessible workspaces")
        workspaces = self.terra.fetch_accessible_workspaces(
            fields=[
                "public",
                "workspace",
                "workspace.attributes.description",
                "workspace.attributes.tag:tags"
            ]
        )
        return [workspace for workspace in workspaces if workspace["public"] == True]

    @staticmethod
    def _get_showcase_data(workspace: dict) -> dict:
        return {
            "namespace": workspace["workspace"]["namespace"],
            "name": workspace["workspace"]["name"],
            "cloudPlatform": workspace["workspace"]["cloudPlatform"],
            "created": workspace["workspace"]["createdDate"],
            "tags": workspace["workspace"]["attributes"].get("tag:tags", {}),
            "description": workspace["workspace"]["attributes"]["description"],
        }

    def get_showcase_content(self) -> list[dict]:
        showcase = []
        # Get the featured workspaces
        featured_workspaces = self._get_featured_workspaces()
        # Get the public workspaces
        public_workspaces = self._get_public_workspace()
        for featured_workspace in featured_workspaces:
            billing_project = featured_workspace["namespace"]
            workspace_name = featured_workspace["name"]
            try:
                workspace = next(
                    w for w in public_workspaces
                    if w["workspace"]["namespace"] == billing_project and w["workspace"]["name"] == workspace_name
                )
            # If the workspace is not found in the public workspaces, log a warning and continue
            except StopIteration:
                logging.warning(f"featured workspace {billing_project}/{workspace_name} not found in public workspaces")
                continue
            else:
                # Add the workspace data to the showcase list
                showcase.append(self._get_showcase_data(workspace))
        return showcase

    def write_showcase(self) -> None:
        showcase_content = self.get_showcase_content()
        # Write the showcase content file at SHOWCASE_JSON_PATH
        self.gcp_utils.write_to_gcs(
            cloud_path=SHOWCASE_JSON_PATH,
            content=json.dumps(showcase_content, indent=4)
        )


if __name__ == '__main__':
    args = get_args()
    billing_project = args.billing_project
    workspace_name = args.workspace_name

    # Initialize the Terra and TDR classes
    token = Token(cloud=CLOUD_TYPE)
    request_util = RunRequest(token=token)
    terra_workspace = TerraWorkspace(
        billing_project=billing_project,
        workspace_name=workspace_name,
        request_util=request_util
    )
    terra = Terra(request_util=request_util)
    gcp_utils = GCPCloudFunctions()

    # Make the workspace public
    terra_workspace.make_workspace_public()

    # Update featured workspace json and showcase content
    UpdateFeaturedWorkspaceJson(billing_project=billing_project, workspace_name=workspace_name, gcp_utils=gcp_utils).run()
    ShowcaseContent(terra=terra, gcp_utils=gcp_utils).write_showcase()
