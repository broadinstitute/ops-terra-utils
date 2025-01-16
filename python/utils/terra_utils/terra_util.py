import json
import logging
import re
from typing import Any, Optional
from urllib.parse import urlparse

from .. import GCP

from ..requests_utils.request_util import GET, POST, PATCH, PUT, DELETE

TERRA_LINK = "https://api.firecloud.org/api"
LEONARDO_LINK = "https://leonardo.dsde-prod.broadinstitute.org/api"
WORKSPACE_LINK = "https://workspace.dsde-prod.broadinstitute.org/api/workspaces/v1"
SAM_LINK = "https://sam.dsde-prod.broadinstitute.org/api"
RAWLS_LINK = "https://rawls.dsde-prod.broadinstitute.org/api"

MEMBER = "member"
ADMIN = "admin"


class Terra:
    def __init__(self, request_util: Any):
        """
        Initialize the Terra class.

        Args:
            request_util (Any): An instance of a request utility class to handle HTTP requests.
        """
        self.request_util = request_util

    def fetch_accessible_workspaces(self, fields: Optional[list[str]]) -> list[dict]:
        fields_str = "fields=" + ",".join(fields) if fields else ""
        url = f'{RAWLS_LINK}/workspaces?{fields_str}'
        response = self.request_util.run_request(
            uri=url,
            method=GET
        )
        return response.json()


class TerraGroups:
    """
    A class to manage Terra groups and their memberships.
    """

    GROUP_MEMBERSHIP_OPTIONS = [MEMBER, ADMIN]

    def __init__(self, request_util: Any):
        """
        Initialize the TerraGroups class.

        Args:
            request_util (Any): An instance of a request utility class to handle HTTP requests.
        """
        self.request_util = request_util

    def _check_role(self, role: str) -> None:
        """
        Check if the role is valid.

        Args:
            role (str): The role to check.

        Raises:
            ValueError: If the role is not one of the allowed options.
        """
        if role not in self.GROUP_MEMBERSHIP_OPTIONS:
            raise ValueError(f"Role must be one of {self.GROUP_MEMBERSHIP_OPTIONS}")

    def remove_user_from_group(self, group: str, email: str, role: str) -> int:
        """
        Remove a user from a group.

        Args:
            group (str): The name of the group.
            email (str): The email of the user to remove.
            role (str): The role of the user in the group.
        Returns:
            int: The response code
        """
        url = f"{SAM_LINK}/groups/v1/{group}/{role}/{email}"
        self._check_role(role)
        res = self.request_util.run_request(
            uri=url,
            method=DELETE
        )
        logging.info(f"Removed {email} from group {group}")
        return res.status_code

    def create_group(self, group_name: str, continue_if_exists: bool = False) -> int:
        """
        Create a new group.

        Args:
            group_name (str): The name of the group to create.
            continue_if_exists (bool, optional): Whether to continue if the group already exists. Defaults to False.
        Returns:
            int: The response code
        """
        url = f"{SAM_LINK}/groups/v1/{group_name}"
        accept_return_codes = [409] if continue_if_exists else []
        response = self.request_util.run_request(
            uri=url,
            method=POST,
            accept_return_codes=accept_return_codes
        )
        if continue_if_exists and response.status_code == 409:
            logging.info(f"Group {group_name} already exists. Continuing.")
            return response.status_code
        else:
            logging.info(f"Created group {group_name}")
            return response.status_code

    def delete_group(self, group_name: str) -> int:
        """
        Delete a group.

        Args:
            group_name (str): The name of the group to delete.
        Returns:
            int: The status code
        """
        url = f"{SAM_LINK}/groups/v1/{group_name}"
        res = self.request_util.run_request(
            uri=url,
            method=DELETE
        )
        logging.info(f"Deleted group {group_name}")
        return res.status_code

    def add_user_to_group(self, group: str, email: str, role: str, continue_if_exists: bool = False) -> int:
        """
        Add a user to a group.

        Args:
            group (str): The name of the group.
            email (str): The email of the user to add.
            role (str): The role of the user in the group.
            continue_if_exists (bool, optional): Whether to continue if the user is already in the group.
                Defaults to False.
        Returns:
            int: The response code
        """
        url = f"{SAM_LINK}/groups/v1/{group}/{role}/{email}"
        self._check_role(role)
        accept_return_codes = [409] if continue_if_exists else []
        res = self.request_util.run_request(
            uri=url,
            method=PUT,
            accept_return_codes=accept_return_codes
        )
        logging.info(f"Added {email} to group {group} as {role}")
        return res.status_code


class TerraWorkspace:
    def __init__(self, billing_project: str, workspace_name: str, request_util: Any):
        """
        Initialize the TerraWorkspace class.

        Args:
            billing_project (str): The billing project associated with the workspace.
            workspace_name (str): The name of the workspace.
            request_util (Any): An instance of a request utility class to handle HTTP requests.
        """
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
        """
        Return a string representation of the TerraWorkspace instance.

        Returns:
            str: The string representation of the TerraWorkspace instance.
        """
        return f"{self.billing_project}/{self.workspace_name}"

    def _yield_all_entity_metrics(self, entity: str, total_entities_per_page: int = 40000) -> Any:
        """
        Yield all entity metrics from the workspace.

        Args:
            entity (str): The type of entity to query.
            total_entities_per_page (int, optional): The number of entities per page. Defaults to 40000.

        Yields:
            Any: The JSON response containing entity metrics.
        """
        url = f"{TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/entityQuery/{entity}?pageSize={total_entities_per_page}"  # noqa: E501
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

    @staticmethod
    def validate_terra_headers_for_tdr_conversion(table_name: str, headers: list[str]) -> None:
        tdr_header_allowed_pattern = "^[a-zA-Z][_a-zA-Z0-9]*$"
        tdr_max_header_length = 63

        headers_containing_too_many_characters = []
        headers_contain_invalid_characters = []

        for header in headers:
            if len(header) > tdr_max_header_length:
                headers_containing_too_many_characters.append(header)
            if not re.match(tdr_header_allowed_pattern, header):
                headers_contain_invalid_characters.append(header)

        base_error_message = """In order to proceed, please update the problematic header(s) in you Terra table,
        and then re-attempt the import once all problematic header(s) have been updated to follow TDR rules for
        header naming."""
        too_many_characters_error_message = f"""The following header(s) in table "{table_name}" contain too many
        characters: "{', '.join(headers_containing_too_many_characters)}". The max number of characters for a header
        allowed in TDR is {tdr_max_header_length}.\n"""
        invalid_characters_error_message = f"""The following header(s) in table "{table_name}" contain invalid
        characters: "{', '.join(headers_contain_invalid_characters)}". TDR headers must start with a letter, and must
        only contain numbers, letters, and underscore characters.\n"""

        error_to_report = ""
        if headers_containing_too_many_characters:
            error_to_report += too_many_characters_error_message
        if headers_contain_invalid_characters:
            error_to_report += invalid_characters_error_message
        if error_to_report:
            error_to_report += base_error_message
            raise ValueError(error_to_report)

    def get_workspace_info(self) -> dict:
        """
        Get workspace information.

        Returns:
            dict: The JSON response containing workspace information.
        """
        url = f"{TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}"
        logging.info(
            f"Getting workspace info for {self.billing_project}/{self.workspace_name}")
        response = self.request_util.run_request(uri=url, method=GET)
        return json.loads(response.text)

    def _set_resource_id_and_storage_container(self) -> None:
        """
        Get and set the resource ID and storage container for the workspace.

        Raises:
            ValueError: If no resource ID is found.
        """
        url = f"{WORKSPACE_LINK}/{self.workspace_id}/resources?offset=0&limit=10&resource=AZURE_STORAGE_CONTAINER"
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
        """
        Get all needed variables and set them for the class.
        """
        workspace_info = self.get_workspace_info()
        self.workspace_id = workspace_info["workspace"]["workspaceId"]
        self._set_resource_id_and_storage_container()
        self._set_account_url()
        self._set_wds_url()

    def _set_wds_url(self) -> None:
        """
        Get and set the WDS URL for the workspace.

        Raises:
            ValueError: If no WDS URL is found.
        """
        uri = f"{LEONARDO_LINK}/apps/v2/{self.workspace_id}?includeDeleted=false"
        logging.info(
            f"Getting WDS URL for {self.billing_project}/{self.workspace_name}")
        response = self.request_util.run_request(uri=uri, method=GET)
        for entries in json.loads(response.text):
            if entries['appType'] == 'WDS' and entries['proxyUrls']['wds'] is not None:
                self.wds_url = entries['proxyUrls']['wds']
                return None
        raise ValueError(
            f"No WDS URL found for {self.billing_project}/{self.workspace_name} - {self.workspace_id}")

    def get_gcp_workspace_metrics(self, entity_type: str, remove_dicts: bool = False) -> list[dict]:
        """
        Get metrics for a specific entity type in the workspace.

        Args:
            entity_type (str): The type of entity to get metrics for.
            remove_dicts (bool, optional): Whether to remove dictionaries from the workspace metrics. Defaults to False.

        Returns:
            list[dict]: A list of dictionaries containing entity metrics.
        """
        results = []
        logging.info(f"Getting {entity_type} metadata for {self.billing_project}/{self.workspace_name}")

        for page in self._yield_all_entity_metrics(entity=entity_type):
            results.extend(page["results"])

        # If remove_dicts is True, remove dictionaries from the workspace metrics
        if remove_dicts:
            for row in results:
                row['attributes'] = self._remove_dict_from_attributes(row['attributes'])
        return results

    def _remove_dict_from_attributes(self, attributes: dict) -> dict:
        """
        Remove dictionaries from the attributes.

        Args:
            attributes (dict): The attributes to remove dictionaries from.

        Returns:
            dict: The updated attributes with no dictionaries.
        """
        for key, value in attributes.items():
            attributes[key] = self._remove_dict_from_cell(value)
        return attributes

    def _remove_dict_from_cell(self, cell_value: Any) -> Any:
        """
        Remove a dictionary from a cell.

        Args:
            cell_value (Any): The dictionary to remove.

        Returns:
            Any: The updated cell with no dictionaries.
        """
        if isinstance(cell_value, dict):
            entity_name = cell_value.get("entityName")
            # If the cell value is a dictionary, check if it has an entityName key
            if entity_name:
                # If the cell value is a dictionary with an entityName key, return the entityName
                return entity_name
            entity_list = cell_value.get("items")
            if entity_list or entity_list == []:
                # If the cell value is a list of dictionaries, recursively call this function on each dictionary
                return [
                    self._remove_dict_from_cell(entity) for entity in entity_list
                ]
            logging.warning(f"Cell is a dict but no entityName or items found: {cell_value}")
            return cell_value
        return cell_value

    def _get_sas_token_json(self, sas_expiration_in_secs: int) -> dict:
        """
        Get the SAS token JSON.

        Args:
            sas_expiration_in_secs (int): The expiration time for the SAS token in seconds.

        Returns:
            dict: The JSON response containing the SAS token.
        """
        url = f"{WORKSPACE_LINK}/{self.workspace_id}/resources/controlled/azure/storageContainer/{self.resource_id}/getSasToken?sasExpirationDuration={str(sas_expiration_in_secs)}"  # noqa: E501
        response = self.request_util.run_request(uri=url, method=POST)
        return json.loads(response.text)

    def _set_account_url(self) -> None:
        """
        Set the account URL for the Azure workspace.
        """
        # Can only get account URL after setting resource ID and from getting a sas token
        sas_token_json = self._get_sas_token_json(sas_expiration_in_secs=1)
        parsed_url = urlparse(sas_token_json["url"])
        # Set url to be https://account_name.blob.core.windows.net
        self.account_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

    def retrieve_sas_token(self, sas_expiration_in_secs: int) -> str:
        """
        Retrieve the SAS token for the workspace.

        Args:
            sas_expiration_in_secs (int): The expiration time for the SAS token in seconds.

        Returns:
            str: The SAS token.
        """
        sas_response_json = self._get_sas_token_json(
            sas_expiration_in_secs=sas_expiration_in_secs)
        return sas_response_json["token"]

    def set_workspace_id(self, workspace_info: dict) -> None:
        """
        Set the workspace ID.

        Args:
            workspace_info (dict): The dictionary containing workspace information.
        """
        self.workspace_id = workspace_info["workspace"]["workspaceId"]

    def get_workspace_bucket(self) -> str:
        """
        Get the workspace bucket name. Does not include the gs:// prefix.

        Returns:
            str: The bucket name.
        """
        return self.get_workspace_info()["workspace"]["bucketName"]

    def get_workspace_entity_info(self, use_cache: bool = True) -> dict:
        """
        Get workspace entity information.

        Args:
            use_cache (bool, optional): Whether to use cache. Defaults to True.

        Returns:
            dict: The JSON response containing workspace entity information.
        """
        use_cache = "true" if use_cache else "false"  # type: ignore[assignment]
        url = f"{TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/entities?useCache={use_cache}"
        response = self.request_util.run_request(uri=url, method=GET)
        return json.loads(response.text)

    def get_workspace_acl(self) -> dict:
        """
        Get the workspace access control list (ACL).

        Returns:
            dict: The JSON response containing the workspace ACL.
        """
        url = f"{TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/acl"
        response = self.request_util.run_request(
            uri=url,
            method=GET
        )
        return response.json()

    def update_user_acl(
            self,
            email: str,
            access_level: str,
            can_share: bool = False,
            can_compute: bool = False,
            invite_users_not_found: bool = False,
    ) -> dict:
        """
        Update the access control list (ACL) for a user in the workspace.

        Args:
            email (str): The email of the user.
            access_level (str): The access level to grant to the user.
            can_share (bool, optional): Whether the user can share the workspace. Defaults to False.
            can_compute (bool, optional): Whether the user can compute in the workspace. Defaults to False.
            invite_users_not_found (bool, optional): Whether a user that's not found should still be invited to access
                the workspace. Defaults to False

        Returns:
            dict: The JSON response containing the updated ACL.
        """
        url = f"{TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/acl?" + \
              f"inviteUsersNotFound={str(invite_users_not_found).lower()}"
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
        if request_json["usersNotFound"] and not invite_users_not_found:
            # Will be a list of one user
            user_not_found = request_json["usersNotFound"][0]
            raise Exception(
                f'The user {user_not_found["email"]} was not found and access was not updated'
            )
        return request_json

    def put_metadata_for_library_dataset(self, library_metadata: dict, validate: bool = False) -> dict:
        """
        Update the metadata for a library dataset.

        Args:
            library_metadata (dict): The metadata to update.
            validate (bool, optional): Whether to validate the metadata. Defaults to False.
        Returns:
            dict: The JSON response containing the updated library attributes.
        """
        acl = f"{TERRA_LINK}/library/{self.billing_project}/{self.workspace_name}" + \
              f"/metadata?validate={str(validate).lower()}"
        res = self.request_util.run_request(
            uri=acl,
            method=PUT,
            data=json.dumps(library_metadata)
        )
        return res.json()

    def update_multiple_users_acl(self, acl_list: list[dict], invite_users_not_found: bool = False) -> dict:
        """
        Update the access control list (ACL) for multiple users in the workspace.

        Args:
            acl_list (list[dict]): A list of dictionaries containing the ACL information for each user.
            invite_users_not_found (bool, optional): Whether a user that's not found should still be invited to access
                the workspace. Defaults to False

        Returns:
            dict: The JSON response containing the updated ACL.
        """
        url = f"{TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/acl?" + \
            f"inviteUsersNotFound={str(invite_users_not_found).lower()}"
        logging.info(
            f"Updating users in workspace {self.billing_project}/{self.workspace_name}")
        response = self.request_util.run_request(
            uri=url,
            method=PATCH,
            content_type="application/json",
            data=json.dumps(acl_list)
        )
        request_json = response.json()
        if request_json["usersNotFound"] and not invite_users_not_found:
            # Will be a list of one user
            users_not_found = [u["email"] for u in request_json["usersNotFound"]]
            raise Exception(
                f"The following users were not found and access was not updated: {users_not_found}"
            )
        return request_json

    def create_workspace(
            self,
            auth_domain: list[dict] = [],
            attributes: dict = {},
            continue_if_exists: bool = False,
            cloud_platform: str = GCP
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
            uri=f"{TERRA_LINK}/workspaces",
            method=POST,
            content_type="application/json",
            data=json.dumps(payload),
            accept_return_codes=accept_return_codes
        )
        if continue_if_exists and response.status_code == 409:
            logging.info(f"Workspace {self.billing_project}/{self.workspace_name} already exists")
        return response.json()

    def create_workspace_attributes_ingest_dict(self, workspace_attributes: Optional[dict] = None) -> list[dict]:
        """
        Create an ingest dictionary for workspace attributes.

        Args:
            workspace_attributes (Optional[dict], optional): A dictionary of workspace attributes. Defaults to None.

        Returns:
            list[dict]: A list of dictionaries containing the workspace attributes.
        """
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
        """
        Upload metadata to the workspace table.

        Args:
            entities_tsv (str): The path to the TSV file containing the metadata.

        Returns:
            str: The response from the upload request.
        """
        endpoint = f"{TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/flexibleImportEntities"
        data = {"entities": open(entities_tsv, "rb")}
        response = self.request_util.upload_file(
            uri=endpoint,
            data=data
        )
        return response

    def get_workspace_workflows(self) -> dict:
        """
        Get the workflows for the workspace.

        Returns:
            dict: The JSON response containing the workspace workflows.
        """
        uri = f"{TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/methodconfigs?allRepos=true"
        response = self.request_util.run_request(
            uri=uri,
            method=GET
        )
        return response.json()

    def import_workflow(self, workflow_dict: dict, continue_if_exists: bool = False) -> int:
        """
        Import a workflow into the workspace.

        Args:
            workflow_dict (dict): The dictionary containing the workflow information.
            continue_if_exists (bool, optional): Whether to continue if the workflow
                already exists. Defaults to False.

        Returns:
            int: The response status code
        """
        uri = f"{TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/methodconfigs"
        workflow_json = json.dumps(workflow_dict)
        accept_return_codes = [409] if continue_if_exists else []
        response = self.request_util.run_request(
            uri=uri,
            method=POST,
            data=workflow_json,
            content_type="application/json",
            accept_return_codes=accept_return_codes
        )
        return response.status_code

    def delete_workspace(self) -> int:
        """
        Delete a Terra workspace.

        Returns:
            int: The response status code
        """
        response = self.request_util.run_request(
            uri=f"{TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}",
            method=DELETE
        )
        return response

    def update_workspace_attributes(self, attributes: list[dict]) -> None:
        """
        Update the attributes for the workspace.

        Args:
            attributes (dict): The attributes to update.

        Returns:
            int: The response status code
        """
        self.request_util.run_request(
            uri=f"{TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/updateAttributes",
            method=PATCH,
            data=json.dumps(attributes),
            content_type="application/json"
        )

    def leave_workspace(self, workspace_id: Optional[str] = None, ignore_direct_access_error: bool = False) -> None:
        """
        Leave a workspace. If workspace ID not supplied will look it up

        Args:
            workspace_id (Optional[str], optional): The workspace ID. Defaults to None.
            ignore_direct_access_error (Optional[bool], optional): Whether to ignore direct access errors.
             Defaults to False.
        """
        if not workspace_id:
            workspace_info = self.get_workspace_info()
            workspace_id = workspace_info['workspace']['workspaceId']
        accepted_return_code = [403] if ignore_direct_access_error else []

        res = self.request_util.run_request(
            uri=f"{SAM_LINK}/resources/v2/workspace/{workspace_id}/leave",
            method=DELETE,
            accept_return_codes=accepted_return_code
        )
        if (res.status_code == 403
                and res.json()["message"] == "You can only leave a resource that you have direct access to."):
            logging.info(
                f"Did not remove user from workspace with id '{workspace_id}' as current user does not have direct"
                f"access to the workspace (they could be an owner on the billing project)"
            )

    def change_workspace_public_setting(self, public: bool) -> None:
        """
        Make a workspace public.
        """
        workspace_bucket = self.get_workspace_bucket()
        bucket_prefix_stripped = workspace_bucket.removeprefix("fc-secure-").removeprefix("fc-")
        self.request_util.run_request(
            uri=f"{SAM_LINK}/resources/v2/workspace/{bucket_prefix_stripped}/policies/reader/public",
            method=PUT,
            content_type="application/json",
            data="true" if public else "false"
        )

    def check_workspace_public(self, bucket: Optional[str] = None) -> bool:
        """
        Check if a workspace is public.
        """
        workspace_bucket = bucket if bucket else self.get_workspace_bucket()
        bucket_prefix_stripped = workspace_bucket.removeprefix("fc-secure-").removeprefix("fc-")
        response = self.request_util.run_request(
            uri=f"{SAM_LINK}/resources/v2/workspace/{bucket_prefix_stripped}/policies/reader/public",
            method=GET
        )
        return response.json()
