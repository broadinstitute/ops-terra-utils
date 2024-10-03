import json
import logging
from typing import Any, Optional
from urllib.parse import urlparse

from . import GCP

from .request_util import GET, POST, PATCH, PUT


class Terra:
    TERRA_LINK = "https://api.firecloud.org/api"

    def __init__(self, request_util: Any):
        self.request_util = request_util

    def add_user_to_group(self, group: str, email: str, role: str = "member") -> None:
        url = f"{self.TERRA_LINK}/groups/{group}/{role}/{email}"
        self.request_util.run_request(
            uri=url,
            method=PUT
        )
        logging.info(f"Added {email} to group {group} as {role}")


class TerraWorkspace:
    TERRA_LINK = "https://api.firecloud.org/api"
    LEONARDO_LINK = "https://leonardo.dsde-prod.broadinstitute.org/api"
    WORKSPACE_LINK = "https://workspace.dsde-prod.broadinstitute.org/api/workspaces/v1"

    def __init__(self, billing_project: str, workspace_name: str, request_util: Any):
        self.billing_project = billing_project
        self.workspace_name = workspace_name
        self.workspace_id = None
        self.resource_id = None
        self.storage_container = None
        self.bucket = None
        self.wds_url = None
        self.account_url: Optional[str] = None
        self.request_util = request_util

    def __repr__(self) -> str:
        return f"{self.billing_project}/{self.workspace_name}"

    def _yield_all_entity_metrics(self, entity: str, total_entities_per_page: int = 40000) -> Any:
        """Yield all entity metrics from workspace."""
        url = f"{self.TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/entityQuery/{entity}?pageSize={total_entities_per_page}"  # noqa: E501
        response = self.request_util.run_request(
            uri=url,
            method=GET,
            content_type="application/json"
        )
        first_page_json = response.json()
        yield first_page_json
        total_pages = first_page_json["resultMetadata"]["filteredPageCount"]
        logging.info(
            f"Looping through {total_pages} pages of data")

        for page in range(2, total_pages + 1):
            logging.info(f"Getting page {page} of {total_pages}")
            next_page = self.request_util.run_request(
                uri=url,
                method=GET,
                content_type="application/json",
                params={"page": page}
            )
            yield next_page.json()

    def get_workspace_info(self) -> dict:
        """Get workspace info."""
        url = f"{self.TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}"
        logging.info(
            f"Getting workspace info for {self.billing_project}/{self.workspace_name}")
        response = self.request_util.run_request(uri=url, method=GET)
        return json.loads(response.text)

    def _set_resource_id_and_storage_container(self) -> None:
        """Get resource ID and storage container."""
        url = f"{self.WORKSPACE_LINK}/{self.workspace_id}/resources?offset=0&limit=10&resource=AZURE_STORAGE_CONTAINER"
        logging.info(
            f"Getting resource ID for {self.billing_project}/{self.workspace_name}")
        response = self.request_util.run_request(uri=url, method=GET)
        for resource_entry in response.json()["resources"]:
            storage_container_name = resource_entry["resourceAttributes"][
                "azureStorageContainer"]["storageContainerName"]
            # Check if storage container name is sc- and set resource ID and storage_container_name as bucket
            if storage_container_name.startswith("sc-"):
                self.resource_id = resource_entry["metadata"]["resourceId"]
                self.storage_container = storage_container_name
                return None

        raise ValueError(
            f"No resource ID found for {self.billing_project}/{self.workspace_name} - "
            f"{self.workspace_id}: {json.dumps(response.json(), indent=4)}"
        )

    def set_azure_terra_variables(self) -> None:
        """Get all needed variables and set it for the class"""
        workspace_info = self.get_workspace_info()
        self.workspace_id = workspace_info["workspace"]["workspaceId"]
        self._set_resource_id_and_storage_container()
        self._set_account_url()
        self._set_wds_url()

    def _set_wds_url(self) -> None:
        """"Get url for wds."""
        uri = f"{self.LEONARDO_LINK}/apps/v2/{self.workspace_id}?includeDeleted=false"
        logging.info(
            f"Getting WDS URL for {self.billing_project}/{self.workspace_name}")
        response = self.request_util.run_request(uri=uri, method=GET)
        for entries in json.loads(response.text):
            if entries['appType'] == 'WDS' and entries['proxyUrls']['wds'] is not None:
                self.wds_url = entries['proxyUrls']['wds']
                return None
        raise ValueError(
            f"No WDS URL found for {self.billing_project}/{self.workspace_name} - {self.workspace_id}")

    def get_gcp_workspace_metrics(self, entity_type: str) -> list[dict]:
        """Get metrics for entity type in workspace."""
        results = []
        logging.info(
            f"Getting {entity_type} metadata for {self.billing_project}/{self.workspace_name}")
        full_entity_generator = self._yield_all_entity_metrics(
            entity=entity_type
        )
        for page in full_entity_generator:
            results.extend(page["results"])
        return results

    def _get_sas_token_json(self, sas_expiration_in_secs: int) -> dict:
        """Get SAS token JSON."""
        url = f"{self.WORKSPACE_LINK}/{self.workspace_id}/resources/controlled/azure/storageContainer/{self.resource_id}/getSasToken?sasExpirationDuration={str(sas_expiration_in_secs)}"  # noqa: E501
        response = self.request_util.run_request(uri=url, method=POST)
        return json.loads(response.text)

    def _set_account_url(self) -> None:
        """Set account URL for Azure workspace."""
        # Can only get account URL after setting resource ID and from getting a sas token
        sas_token_json = self._get_sas_token_json(sas_expiration_in_secs=1)
        parsed_url = urlparse(sas_token_json["url"])
        # Set url to be https://account_name.blob.core.windows.net
        self.account_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

    def retrieve_sas_token(self, sas_expiration_in_secs: int) -> str:
        """Retrieve SAS token for workspace."""
        sas_response_json = self._get_sas_token_json(
            sas_expiration_in_secs=sas_expiration_in_secs)
        return sas_response_json["token"]

    def set_workspace_id(self, workspace_info: dict) -> None:
        """Set workspace ID."""
        self.workspace_id = workspace_info["workspace"]["workspaceId"]

    def get_workspace_bucket(self) -> str:
        return self.get_workspace_info()["workspace"]["bucketName"]

    def get_workspace_entity_info(self, use_cache: bool = True) -> dict:
        """Get workspace entity info."""
        use_cache = "true" if use_cache else "false"  # type: ignore[assignment]
        url = f"{self.TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/entities?useCache={use_cache}"
        response = self.request_util.run_request(uri=url, method=GET)
        return json.loads(response.text)

    def get_workspace_acl(self) -> dict:
        url = f"{self.TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/acl"
        response = self.request_util.run_request(
            uri=url,
            method=GET
        )
        return response.json()

    def update_user_acl(
            self, email: str, access_level: str, can_share: bool = False, can_compute: bool = False
    ) -> dict:
        url = f"{self.TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/acl"
        payload = {
            "email": email,
            "accessLevel": access_level,
            "canShare": can_share,
            "canCompute": can_compute,
        }
        logging.info(
            f"Updating user {email} to {access_level} in workspace {self.billing_project}/{self.workspace_name}")
        response = self.request_util.run_request(
            uri=url,
            method=PATCH,
            content_type="application/json",
            data="[" + json.dumps(payload) + "]"
        )
        request_json = response.json()
        if request_json["usersNotFound"]:
            # Will be a list of one user
            user_not_found = request_json["usersNotFound"][0]
            raise Exception(
                f'The user {user_not_found["email"]} was not found and access was not updated'
            )
        return request_json

    def put_metadata_for_library_dataset(self, library_metadata: dict, validate: bool = False) -> None:
        acl = f"{self.TERRA_LINK}/library/{self.billing_project}/{self.workspace_name}" + \
              f"/metadata?validate={str(validate).lower()}"
        self.request_util.run_request(
            uri=acl,
            method=PUT,
            data=json.dumps(library_metadata)
        )

    def update_multiple_users_acl(self, acl_list: list[dict]) -> dict:
        """acl list should be a list of dictionaries with the following format:
        [{"email": "email", "accessLevel": "access_level", "canShare": "can_share", "canCompute": "can_compute"}]"""
        url = f"{self.TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/acl"
        logging.info(
            f"Updating users in workspace {self.billing_project}/{self.workspace_name}")
        response = self.request_util.run_request(
            uri=url,
            method=PATCH,
            content_type="application/json",
            data=json.dumps(acl_list)
        )
        return response.json()

    def create_workspace(
        self, auth_domain: list[dict] = [], attributes: dict = {},
        continue_if_exists: bool = False, cloud_platform: str = GCP
    ) -> Optional[dict]:
        """
        Create a new workspace in Terra.

        Args:
            auth_domain (list[dict], optional): A list of authorization domains. Should look
                like [{"membersGroupName": "some_auth_domain"}]. Defaults to an empty list.
            attributes (dict, optional): A dictionary of attributes for the workspace. Defaults to an empty dictionary.
            continue_if_exists (bool, optional): Whether to continue if the workspace already exists. Defaults to False.
            cloud_platform (str, optional): The cloud platform for the workspace. Defaults to GCP.

        Returns:
            dict: The response from the Terra API containing the workspace details.
        """
        payload = {
            "namespace": self.billing_project,
            "name": self.workspace_name,
            "authorizationDomain": auth_domain,
            "attributes": attributes,
            "cloudPlatform": cloud_platform
        }
        # If workspace already exists then continue if exists
        accept_return_codes = [409] if continue_if_exists else []
        logging.info(f"Creating workspace {self.billing_project}/{self.workspace_name}")
        response = self.request_util.run_request(
            uri=f"{self.TERRA_LINK}/workspaces",
            method=POST,
            content_type="application/json",
            data=json.dumps(payload),
            accept_return_codes=accept_return_codes
        )
        if continue_if_exists and response.status_code == 409:
            logging.info(f"Workspace {self.billing_project}/{self.workspace_name} already exists")
        return response.json()

    def create_workspace_attributes_ingest_dict(self, workspace_attributes: Optional[dict] = None) -> list[dict]:
        """Create ingest dictionary for workspace attributes. If attributes passed in should JUST be attributes
        and not whole workspace info."""
        # If not provided then call API to get it
        workspace_attributes = workspace_attributes if workspace_attributes else self.get_workspace_info()[
            "workspace"]["attributes"]

        ingest_dict = []
        for key, value in workspace_attributes.items():
            # If value is dict just use 'items' as value
            if isinstance(value, dict):
                value = value.get("items")
            # If value is list convert to comma separated string
            if isinstance(value, list):
                value = ", ".join(value)
            ingest_dict.append(
                {
                    "attribute": key,
                    "value": str(value) if value else None
                }
            )
        return ingest_dict

    def upload_metadata_to_workspace_table(self, entities_tsv: str) -> str:
        endpoint = f"{self.TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/flexibleImportEntities"
        data = {"entities": open(entities_tsv, "rb")}
        response = self.request_util.upload_file(
            uri=endpoint,
            data=data
        )
        return response

    def get_workspace_workflows(self) -> dict:
        uri = f"{self.TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/methodconfigs?allRepos=true"
        response = self.request_util.run_request(
            uri=uri,
            method=GET
        )
        return response.json()

    def import_workflow(self, workflow_dict: dict) -> dict:
        uri = f"{self.TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/methodconfigs"
        workflow_json = json.dumps(workflow_dict)
        response = self.request_util.run_request(
            uri=uri,
            method=POST,
            data=workflow_json,
            content_type="application/json"
        )
        return response
