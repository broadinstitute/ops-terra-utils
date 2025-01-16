import logging
import os.path
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
FEATURED_WORKSPACE_JSON = "featured-workspaces.json"
SHOWCASE_JSON = "showcase.json"
FEATURED_WORKSPACE_LINK = "https://storage.googleapis.com/firecloud-alerts/featured-workspaces.json"


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="Make a GCP terra workspace featured")
    parser.add_argument("--billing_project", "-b", required=True)
    parser.add_argument("--workspace_name", "-w", required=True)
    parser.add_argument("--env", "-e", options=["dev", "prod"], required=True)
    return parser.parse_args()


class UploadJsonAndSetPermissions:
    def __init__(self, gcp_utils: GCPCloudFunctions, comms_group: str, bucket_name: str):
        self.gcp_utils = gcp_utils
        self.comms_group = comms_group
        self.bucket_name = bucket_name

    def run(self, file_contents_json: dict, file_name: str) -> None:
        full_cloud_path = os.path.join(self.bucket_name, file_name)
        logging.info(f"Uploading {file_name} to {full_cloud_path} and updating permissions")
        self.gcp_utils.write_to_gcs(
            cloud_path=full_cloud_path,
            content=json.dumps(file_contents_json, indent=4)
        )
        self.gcp_utils.set_acl_public_read(cloud_path=full_cloud_path)
        self.gcp_utils.set_acl_group_owner(cloud_path=full_cloud_path, group_email=self.comms_group)
        self.gcp_utils.set_metadata_cache_control(
            cloud_path=full_cloud_path,
            cache_control="private, max-age=0, no-store"
        )


class UpdateFeaturedWorkspaceJson:
    def __init__(
            self,
            billing_project: str,
            workspace_name: str,
            gcp_utils: GCPCloudFunctions,
            bucket_name: str,
            upload_util: UploadJsonAndSetPermissions
    ):
        self.billing_project = billing_project
        self.workspace_name = workspace_name
        self.gcp_utils = gcp_utils
        self.upload_util = upload_util
        self.featured_workspace_json = os.path.join(bucket_name, FEATURED_WORKSPACE_JSON)

    def run(self) -> None:
        # Download current featured-workspaces.json
        featured_workspace_json = json.loads(self.gcp_utils.read_file(self.featured_workspace_json))
        # Add the new workspace to the list
        featured_workspace_json.append({"namespace": self.billing_project, "name": self.workspace_name})
        # Write the new json back to the bucket
        self.upload_util.run(file_contents_json=featured_workspace_json, file_name=FEATURED_WORKSPACE_JSON)


class ShowcaseContent:
    def __init__(self, terra: Terra, gcp_utils: GCPCloudFunctions, upload_util: UploadJsonAndSetPermissions):
        self.terra = terra
        self.gcp_utils = gcp_utils
        self.upload_util = upload_util

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
        return [workspace for workspace in workspaces if workspace["public"]]

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

    def _get_showcase_content(self) -> list[dict]:
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
        showcase_content = self._get_showcase_content()
        # Write the showcase content file at SHOWCASE_JSON_PATH
        self.upload_util.run(file_contents_json=showcase_content, file_name=SHOWCASE_JSON)  # type: ignore[arg-type]


if __name__ == '__main__':
    args = get_args()
    billing_project = args.billing_project
    workspace_name = args.workspace_name
    env = args.env

    bucket_name = "gs://firecloud-alerts/" if env == "prod" else f"gs://firecloud-alerts-{env}/"
    comms_group = "fc-comms@firecloud.org" if env == "prod" else f"fc-comms@{env}.test.firecloud.org"

    # Initialize the necessary classes
    token = Token(cloud=CLOUD_TYPE)
    request_util = RunRequest(token=token)
    terra_workspace = TerraWorkspace(
        billing_project=billing_project,
        workspace_name=workspace_name,
        request_util=request_util
    )
    terra = Terra(request_util=request_util)
    gcp_utils = GCPCloudFunctions()
    upload_util = UploadJsonAndSetPermissions(
        gcp_utils=gcp_utils,
        comms_group=comms_group,
        bucket_name=bucket_name
    )

    # Make the workspace public
    terra_workspace.make_workspace_public()

    # Update featured workspace json and showcase content
    UpdateFeaturedWorkspaceJson(
        billing_project=billing_project,
        workspace_name=workspace_name,
        gcp_utils=gcp_utils,
        upload_util=upload_util,
        bucket_name=bucket_name
    ).run()
    ShowcaseContent(
        terra=terra,
        gcp_utils=gcp_utils,
        upload_util=upload_util
    ).write_showcase()
