import logging
from argparse import ArgumentParser, Namespace

from typing import Optional

from utils.tdr_utils.tdr_api_utils import TDR
from utils.tdr_utils.tdr_ingest_utils import StartAndMonitorIngest
from utils.terra_utils.terra_util import TerraWorkspace, TerraGroups
from utils.request_util import RunRequest
from utils.token_util import Token
from utils import GCP, comma_separated_list

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

ANVIL_TDR_BILLING_PROFILE = "e0e03e48-5b96-45ec-baa4-8cc1ebf74c61"
ANVIL_TERRA_BILLING_PROJECT = "anvil-datastorage"
ANVIL_ADMINS = "anvil-admins@firecloud.org"
ANVIL_DEVS = "anvil_devs@firecloud.org"


def get_args() -> Namespace:
    parser = ArgumentParser(description="description of script")
    parser.add_argument("-d", "--dataset_name", required=True)
    parser.add_argument("-bp", "--tdr_billing_profile", required=True)
    parser.add_argument("-b", "--terra_billing_project", required=True)
    parser.add_argument("--controlled_access", action="store_true")
    parser.add_argument("-p", "--phs_id", required=False)
    parser.add_argument("-ro", "--resource_owners", type=comma_separated_list,
                        help="comma seperated list of resource owners", required=False)
    parser.add_argument("-rm", "--resource_members", type=comma_separated_list,
                        help="comma seperated list of resource members", required=False)
    parser.add_argument("-c", "--continue_if_exists", action="store_true",
                        help="Continue if workspace and/or dataset already exists")
    parser.add_argument("-cu", "--current_user_email", required=True,
                        help="Used for removing current user from workspace")
    parser.add_argument("--dbgap_consent_code",
                        help="dbGaP consent code for controlled access datasets. Optional")
    parser.add_argument("--duos_identifier",
                        help="DUOS identifier. Optional")
    return parser.parse_args()


class SetUpTerraWorkspace:
    def __init__(
            self,
            terra_workspace: TerraWorkspace,
            terra_groups: TerraGroups,
            auth_group: str,
            continue_if_exists: bool,
            controlled_access: bool,
            resource_owners: list[str],
            resource_members: list[str]
    ):
        self.terra_workspace = terra_workspace
        self.terra_groups = terra_groups
        self.auth_group = auth_group
        self.continue_if_exists = continue_if_exists
        self.controlled_access = controlled_access
        self.resource_owners = resource_owners
        self.resource_members = resource_members

    def _set_up_access_group(self) -> None:
        logging.info(f"Creating group {self.auth_group}")
        self.terra_groups.create_group(group_name=self.auth_group, continue_if_exists=self.continue_if_exists)
        for user in resource_owners:
            self.terra_groups.add_user_to_group(email=user, group=self.auth_group, role="admin")
        for user in resource_members:
            self.terra_groups.add_user_to_group(email=user, group=self.auth_group, role="member")

    def _add_permissions_to_workspace(self) -> None:
        logging.info(f"Adding permissions to workspace {self.terra_workspace}")
        for user in self.resource_owners:
            self.terra_workspace.update_user_acl(email=user, access_level="OWNER")
        self.terra_workspace.update_user_acl(email=f'{self.auth_group}@firecloud.org', access_level="WRITER")

    def _set_up_workspace(self) -> None:
        # Only add auth domain if workspace is controlled
        if self.controlled_access:
            auth_domain = [{"membersGroupName": self.auth_group}]
        else:
            auth_domain = []
        self.terra_workspace.create_workspace(
            auth_domain=auth_domain,
            cloud_platform=GCP,
            attributes={},
            continue_if_exists=self.continue_if_exists,
        )

    def _validate_auth_domain(self) -> None:
        workspace_info = self.terra_workspace.get_workspace_info()
        auth_domain_list = workspace_info['workspace']['authorizationDomain']
        if (self.controlled_access and
                (not auth_domain_list or auth_domain_list[0]['membersGroupName'] != self.auth_group)):
            logging.error(
                f"Controlled access set for {self.terra_workspace}, but either auth domain is not used or "
                f"does not match expected group name. Auth domain list: {auth_domain_list}")
            raise Exception("Auth domain not set correctly")
        elif not self.controlled_access and auth_domain_list:
            logging.error(
                f"Controlled access not set for {self.terra_workspace}, "
                f"but auth domain is set. Auth domain list: {auth_domain_list}")
            raise Exception("Auth domain not set correctly")

    def run(self) -> None:
        self._set_up_access_group()
        self._set_up_workspace()
        self._add_permissions_to_workspace()
        self._validate_auth_domain()


class SetUpDataset:
    REFERENCE_TABLE = "ingestion_reference"
    SCHEMA = {
        "tables": [
            {
                "name": REFERENCE_TABLE,
                "columns": [
                    {
                        "name": "key",
                        "datatype": "string",
                        "array_of": False,
                        "required": True
                    },
                    {
                        "name": "value",
                        "datatype": "string",
                        "array_of": False,
                        "required": True
                    }
                ]
            }
        ]
    }

    def __init__(
            self,
            tdr: TDR,
            dataset_name: str,
            continue_if_exists: bool,
            workspace_name: str,
            tdr_billing_profile: str,
            resource_owners: list[str],
            auth_group: str,
            controlled_access: bool,
            terra_billing_project: str,
            phs_id: Optional[str] = None
    ):
        self.tdr = tdr
        self.dataset_name = dataset_name
        self.phs_id = phs_id
        self.continue_if_exists = continue_if_exists
        self.terra_billing_project = terra_billing_project
        self.workspace_name = workspace_name
        self.tdr_billing_profile = tdr_billing_profile
        self.resource_owners = resource_owners
        self.auth_group = auth_group
        self.controlled_access = controlled_access

    def _create_dataset_properties(self) -> dict:
        additional_properties = {
            "experimentalSelfHosted": True,
            "dedicatedIngestServiceAccount": True,
            "experimentalPredictableFileIds": True,
            "enableSecureMonitoring": True if self.controlled_access else False,
        }
        if self.phs_id:
            additional_properties["phsId"] = self.phs_id  # type: ignore[assignment]
        return additional_properties

    def _add_row_to_table(self, dataset_id: str) -> None:
        StartAndMonitorIngest(
            ingest_records=[
                {"key": "Staging Workspace", "value": f'{self.terra_billing_project}/{self.workspace_name}'},
                {"key": "Authorization Group", "value": self.auth_group}
            ],
            target_table_name=self.REFERENCE_TABLE,
            dataset_id=dataset_id,
            load_tag=f"{dataset_name}_initial_load",
            bulk_mode=False,
            update_strategy="replace",
            waiting_time_to_poll=30,
            tdr=self.tdr,
        ).run()

    def _set_up_permissions(self, dataset_id: str) -> None:
        for user in self.resource_owners:
            self.tdr.add_user_to_dataset(
                dataset_id=dataset_id,
                user=user,
                policy="steward"
            )
        self.tdr.add_user_to_dataset(
            dataset_id=dataset_id,
            user=f'{self.auth_group}@firecloud.org',
            policy="custodian"
        )

    def run(self) -> dict:
        existing_datasets = self.tdr.check_if_dataset_exists(
            dataset_name=self.dataset_name,
            billing_profile=self.tdr_billing_profile
        )
        if existing_datasets:
            logging.info(f"Dataset {self.dataset_name} already exists")
            if not self.continue_if_exists:
                logging.error("Exiting because dataset already exists. Use --continue_if_exists to continue")
                exit(1)
        dataset_id = self.tdr.get_or_create_dataset(
            dataset_name=dataset_name,
            billing_profile=self.tdr_billing_profile,
            schema=self.SCHEMA,
            description="",
            cloud_platform=GCP,
            additional_properties_dict=self._create_dataset_properties()
        )
        if not existing_datasets:
            self._add_row_to_table(dataset_id)
        else:
            logging.info(f"Because dataset already exists skipping adding row to {self.REFERENCE_TABLE}")
        self._set_up_permissions(dataset_id)
        return self.tdr.get_dataset_info(dataset_id)


class RemoveAllIndividualAccess:
    def __init__(
            self,
            terra_workspace: TerraWorkspace,
            tdr: TDR,
            auth_group: str,
            current_user_email: str,
            dataset_id: str,
            terra_groups: TerraGroups
    ):
        self.terra_workspace = terra_workspace
        self.tdr = tdr
        self.auth_group = auth_group
        self.current_user_email = current_user_email
        self.dataset_id = dataset_id
        self.terra_groups = terra_groups

    def run(self) -> None:
        logging.info(
            f"Removing {self.current_user_email} from workspace {self.terra_workspace}, dataset {self.dataset_id}, "
            f"and group {self.auth_group}")
        self.terra_workspace.leave_workspace()
        self.tdr.remove_user_from_dataset(
            dataset_id=self.dataset_id,
            user=self.current_user_email,
            policy="steward"
        )
        self.terra_groups.remove_user_from_group(
            group=self.auth_group,
            email=self.current_user_email,
            role="admin"
        )


class UpdateWorkspaceAttributes:
    def __init__(
            self,
            terra_workspace: TerraWorkspace,
            auth_group: str,
            dataset_id: str,
            dataset_name: str,
            data_ingest_sa: str,
            dbgap_consent_code: Optional[str] = None,
            duos_identifier: Optional[str] = None,
            phs_id: Optional[str] = None
    ):
        self.terra_workspace = terra_workspace
        self.auth_group = auth_group
        self.dataset_id = dataset_id
        self.dataset_name = dataset_name
        self.data_ingest_sa = data_ingest_sa
        self.dbgap_consent_code = dbgap_consent_code
        self.duos_identifier = duos_identifier
        self.phs_id = phs_id

    def _create_attribute_dict_for_pair(self, attribute_key: str, attribute_value: str) -> dict:
        return {
            "op": "AddUpdateAttribute",
            "attributeName": attribute_key,
            "addUpdateAttribute": attribute_value
        }

    def run(self) -> None:
        attributes = [
            self._create_attribute_dict_for_pair("dataset_id", self.dataset_id),
            self._create_attribute_dict_for_pair("dataset_name", self.dataset_name),
            self._create_attribute_dict_for_pair("auth_group", self.auth_group),
            self._create_attribute_dict_for_pair("data_ingest_sa", self.data_ingest_sa)
        ]
        if self.dbgap_consent_code:
            attributes.append(
                self._create_attribute_dict_for_pair("consent_code", self.dbgap_consent_code)
            )
        if self.duos_identifier:
            attributes.append(
                self._create_attribute_dict_for_pair("duos_id", self.duos_identifier)
            )
        if self.phs_id:
            attributes.append(
                self._create_attribute_dict_for_pair("phs_id", self.phs_id)
            )
        logging.info(f"Updating workspace attributes for {self.terra_workspace}")
        self.terra_workspace.update_workspace_attributes(attributes)


if __name__ == '__main__':
    args = get_args()

    # Get arguments
    dataset_name = args.dataset_name
    controlled_access = args.controlled_access
    phs_id = args.phs_id
    continue_if_exists = args.continue_if_exists
    current_user_email = args.current_user_email
    resource_owners = args.resource_owners
    resource_members = args.resource_members
    terra_billing_project = args.terra_billing_project
    tdr_billing_profile = args.tdr_billing_profile
    dbgap_consent_code = args.dbgap_consent_code
    duos_identifier = args.duos_identifier

    workspace_name = f'{dataset_name}_Staging'
    auth_group = f"AUTH_{dataset_name}"

    # Set up Terra, TerraGroups, and TDR classes
    token = Token(cloud=GCP)
    request_util = RunRequest(token=token, max_retries=5, max_backoff_time=60)
    tdr = TDR(request_util=request_util)
    terra_groups = TerraGroups(request_util=request_util)
    terra_workspace = TerraWorkspace(
        request_util=request_util,
        billing_project=terra_billing_project,
        workspace_name=workspace_name
    )

    # Set up workspace and groups
    SetUpTerraWorkspace(
        terra_workspace=terra_workspace,
        terra_groups=terra_groups,
        auth_group=auth_group,
        continue_if_exists=continue_if_exists,
        controlled_access=controlled_access,
        resource_owners=resource_owners,
        resource_members=resource_members
    ).run()
    logging.info("Finished setting up Terra workspace")

    # Set up dataset
    dataset_info = SetUpDataset(
        tdr=tdr,
        dataset_name=dataset_name,
        tdr_billing_profile=tdr_billing_profile,
        phs_id=phs_id,
        continue_if_exists=continue_if_exists,
        terra_billing_project=terra_billing_project,
        workspace_name=workspace_name,
        resource_owners=resource_owners,
        auth_group=auth_group,
        controlled_access=controlled_access
    ).run()

    data_ingest_sa = dataset_info["ingestServiceAccount"]
    dataset_id = dataset_info["id"]
    # Add data ingest service account to workspace and auth group
    terra_workspace.update_user_acl(
        email=data_ingest_sa,
        access_level="READER"
    )
    terra_groups.add_user_to_group(
        email=data_ingest_sa,
        group=auth_group,
        role="member"
    )

    # Update workspace attributes
    UpdateWorkspaceAttributes(
        terra_workspace=terra_workspace,
        auth_group=auth_group,
        dataset_id=dataset_id,
        dbgap_consent_code=dbgap_consent_code,
        duos_identifier=duos_identifier,
        phs_id=phs_id,
        dataset_name=dataset_name,
        data_ingest_sa=data_ingest_sa
    ).run()

    # Remove current user from workspace and dataset if not a resource owner
    if current_user_email not in resource_owners:
        logging.info(f"Removing {current_user_email} owner access from workspace and dataset")
        RemoveAllIndividualAccess(
            terra_workspace=terra_workspace,
            tdr=tdr,
            auth_group=auth_group,
            current_user_email=current_user_email,
            dataset_id=dataset_id,
            terra_groups=terra_groups
        ).run()
