import logging
import os.path
from argparse import ArgumentParser, Namespace
from urllib.request import urlopen

from ops_utils.token_util import Token
from ops_utils.request_util import RunRequest
from ops_utils.terra_util import TerraWorkspace, Terra
from ops_utils.gcp_utils import GCPCloudFunctions
from ops_utils.vars import GCP
import json

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

FEATURED_WORKSPACE_JSON = "featured-workspaces.json"
SHOWCASE_JSON = "showcase.json"
DEFAULT_SERVICE_ACCOUNT = "feature-workspace-sa@operations-portal-427515.iam.gserviceaccount.com"


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="Make a GCP terra workspace featured")
    parser.add_argument("--billing_project", "-b", required=True)
    parser.add_argument("--workspace_name", "-w", required=True)
    parser.add_argument("--env", "-e", choices=["dev", "prod"], required=True)
    return parser.parse_args()


class UploadJsonAndSetPermissions:
    def __init__(self, gcp_utils: GCPCloudFunctions, comms_group: str, bucket_name: str):
        self.gcp_utils = gcp_utils
        self.comms_group = comms_group
        self.bucket_name = bucket_name

    def run(self, file_contents_json: dict, file_name: str) -> None:
        full_cloud_path = os.path.join(f'gs://{self.bucket_name}/', file_name)
        logging.info(f"Uploading {file_name} to {full_cloud_path} and updating permissions")
        self.gcp_utils.write_to_gcp_file(
            cloud_path=full_cloud_path,
            file_contents=json.dumps(file_contents_json, indent=4)
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
            upload_json_util: UploadJsonAndSetPermissions
    ):
        self.billing_project = billing_project
        self.workspace_name = workspace_name
        self.gcp_utils = gcp_utils
        self.upload_json_util = upload_json_util
        self.featured_workspace_json = os.path.join(f'gs://{bucket_name}/', FEATURED_WORKSPACE_JSON)

    def _check_workspace_in_featured_json(self, featured_workspace_json: list[dict]) -> bool:
        if any(
                workspace["namespace"] == self.billing_project and workspace["name"] == self.workspace_name
                for workspace in featured_workspace_json
        ):
            return True
        return False

    def run(self) -> None:
        # Download current featured-workspaces.json
        featured_workspace_json = json.loads(self.gcp_utils.read_file(self.featured_workspace_json))
        # Check if workspace already in json and do not update if it is
        if self._check_workspace_in_featured_json(featured_workspace_json):
            logging.info(f"Workspace {self.billing_project}/{self.workspace_name} already in featured workspaces")
            return
        else:
            # Add the new workspace to the list
            featured_workspace_json.append({"namespace": self.billing_project, "name": self.workspace_name})
            # Write the new json back to the bucket
            self.upload_json_util.run(file_contents_json=featured_workspace_json, file_name=FEATURED_WORKSPACE_JSON)


class ShowcaseContent:
    def __init__(
            self,
            terra: Terra,
            gcp_utils: GCPCloudFunctions,
            upload_json_util: UploadJsonAndSetPermissions,
            bucket_name: str,
            billing_project: str,
            workspace_name: str,
            request_util: RunRequest
    ):
        self.terra = terra
        self.gcp_utils = gcp_utils
        self.upload_json_util = upload_json_util
        self.bucket_name = bucket_name
        self.billing_project = billing_project
        self.workspace_name = workspace_name
        self.request_util = request_util

    def _get_featured_workspaces(self) -> dict:
        logging.info("Fetching featured workspaces")
        return json.load(urlopen(f"https://storage.googleapis.com/{self.bucket_name}/{FEATURED_WORKSPACE_JSON}"))

    def _get_accessible_workspaces(self) -> dict:
        logging.info("Fetching all accessible workspaces")
        workspaces = self.terra.fetch_accessible_workspaces(
            fields=[
                "public",
                "workspace",
                "workspace.attributes.description",
                "workspace.attributes.tag:tags"
            ]
        )
        return {
            f'{w["workspace"]["namespace"]}.{w["workspace"]["name"]}': w
            for w in workspaces
        }

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
        # Get the accessible workspaces
        accessible_workspaces = self._get_accessible_workspaces()
        # Get the featured workspaces
        featured_workspaces = self._get_featured_workspaces()
        for featured_workspace in featured_workspaces:
            namespace = featured_workspace["namespace"]
            name = featured_workspace["name"]
            workspace_dict = accessible_workspaces.get(f"{namespace}.{name}")
            # If the workspace is not found accessible workspaces
            if not workspace_dict:
                logging.warning(f"featured workspace {namespace}/{name} not found in accessible workspaces")
                continue
            if not workspace_dict['public']:
                # Check directly if the workspace is public because can be hour delay in SAM being updated
                if not TerraWorkspace(
                        billing_project=namespace,
                        workspace_name=name,
                        request_util=self.request_util
                ).check_workspace_public():
                    logging.warning(
                        f"featured workspace {namespace}/{name} is not a public workspace but is in featured"
                    )
                    continue
                else:
                    logging.info(
                        f"Workspace {namespace}/{name} is public but metadata not updated in SAM. Setting public"
                    )
                    # Update the workspace to be public in the accessible workspaces
                    workspace_dict['public'] = True
            # Add the workspace data to the showcase list
            showcase.append(self._get_showcase_data(workspace_dict))
        return showcase

    def write_showcase(self) -> None:
        showcase_content = self._get_showcase_content()
        # Write the showcase content file at SHOWCASE_JSON_PATH
        self.upload_json_util.run(file_contents_json=showcase_content, file_name=SHOWCASE_JSON)  # type: ignore[arg-type]


if __name__ == '__main__':
    args = get_args()
    billing_project = args.billing_project
    workspace_name = args.workspace_name
    env = args.env

    bucket_name = "firecloud-alerts" if env == "prod" else f"firecloud-alerts-{env}"
    comms_group = "fc-comms@firecloud.org" if env == "prod" else f"fc-comms@{env}.test.firecloud.org"

    # Initialize the necessary classes
    token = Token()
    request_util = RunRequest(token=token)
    terra_workspace = TerraWorkspace(
        billing_project=billing_project,
        workspace_name=workspace_name,
        request_util=request_util
    )
    terra = Terra(request_util=request_util)
    gcp_utils = GCPCloudFunctions()
    upload_json_util = UploadJsonAndSetPermissions(
        gcp_utils=gcp_utils,
        comms_group=comms_group,
        bucket_name=bucket_name
    )

    # Make the workspace public
    logging.info("Checking if workspace is already public")
    if terra_workspace.check_workspace_public():
        logging.info("Workspace is already public")
    else:
        logging.info("Making workspace public")
        terra_workspace.change_workspace_public_setting(public=True)

    # Update featured workspace json and showcase content
    UpdateFeaturedWorkspaceJson(
        billing_project=billing_project,
        workspace_name=workspace_name,
        gcp_utils=gcp_utils,
        upload_json_util=upload_json_util,
        bucket_name=bucket_name
    ).run()

    # Write showcase content
    ShowcaseContent(
        terra=terra,
        gcp_utils=gcp_utils,
        upload_json_util=upload_json_util,
        bucket_name=bucket_name,
        billing_project=billing_project,
        workspace_name=workspace_name,
        request_util=request_util
    ).write_showcase()

    logging.info(
        f"Attempting to remove SA '{DEFAULT_SERVICE_ACCOUNT}' from workspace: '{billing_project}/{workspace_name}'")
    # Removes the SA as a direct owner on the workspace
    terra_workspace.leave_workspace()
