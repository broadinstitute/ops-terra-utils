import logging
import sys
import re
import argparse
from typing import Optional
from datetime import datetime

from utils import GCP
from utils.tdr_utils.tdr_api_utils import TDR, FILE_INVENTORY_DEFAULT_SCHEMA
from utils.tdr_utils.tdr_ingest_utils import (
    ConvertTerraTableInfoForIngest,
    FilterAndBatchIngest,
    GetPermissionsForWorkspaceIngest
)
from utils.tdr_utils.tdr_table_utils import SetUpTDRTables
from utils.request_util import RunRequest
from utils.token_util import Token
from utils.terra_util import TerraWorkspace, Terra
from utils.gcp_utils import GCPCloudFunctions

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

TOKEN_TYPE = GCP  # The cloud type for the token
CLOUD_TYPE = GCP  # The cloud type for the TDR dataset and workspace
MAX_RETRIES = 5  # The maximum number of retries for a failed request
# The maximum backoff time for a failed request (in seconds)
MAX_BACKOFF_TIME = 5 * 60
# Anvil prod billing profile id
ANVIL_TDR_BILLING_PROFILE = "e0e03e48-5b96-45ec-baa4-8cc1ebf74c61"
DATASET_MONITORING = True  # Enable monitoring for dataset
# The number of rows to ingest at a time when ingesting files
FILE_INGEST_BATCH_SIZE = 500
# How long to wait between polling for ingest status when ingesting files
FILE_INGEST_WAITING_TIME_TO_POLL = 30
# The number of rows to ingest at a time when ingesting metadata
METADATA_INGEST_BATCH_SIZE = 1000
# How long to wait between polling for ingest status when ingesting metadata
METADATA_INGEST_WAITING_TIME_TO_POLL = 45
TEST_INGEST = False  # Whether to test the ingest by just doing first batch
FILE_INVENTORY_TABLE_NAME = "file_inventory"


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create and ingest data into a new GCP dataset from a workspace")
    parser.add_argument("--billing_project", required=True)
    parser.add_argument("--workspace_name", required=True)
    parser.add_argument(
        "--dataset_name",
        required=False,
        help="Will default to generating one based on the workspace name if not explicitly provided"
    )
    parser.add_argument("--phs_id", required=True)
    parser.add_argument(
        "--update_strategy",
        required=False,
        choices=["REPLACE", "APPEND", "UPDATE"],
        default="REPLACE",
        help="Defaults to REPLACE if not provided"
    )
    parser.add_argument(
        "--bulk_mode",
        action="store_true",
        help="""If used, will use bulk mode for ingest. Using bulk mode for TDR Ingest loads data faster when ingesting
         a large number of files (e.g. more than 10,000 files) at once. The performance does come at the cost of
         some safeguards (such as guaranteed rollbacks and potential recopying of files) and it also forces exclusive
         locking of the dataset (i.e. you can’t run multiple ingests at once)"""
    )
    parser.add_argument(
        "--tdr_billing_profile",
        required=False,
        default=ANVIL_TDR_BILLING_PROFILE,
        help="Defaults to the AnVIL-specific TDR billing profile if not provided"
    )
    parser.add_argument(
        "--file_ingest_batch_size",
        required=False,
        default=FILE_INGEST_BATCH_SIZE,
        help=f"The number of rows to ingest at a time. Defaults to {FILE_INGEST_BATCH_SIZE} if not provided"
    )
    parser.add_argument(
        "--max_backoff_time",
        required=False,
        default=MAX_BACKOFF_TIME,
        help=f"The maximum backoff time for a failed request (in seconds). Defaults to {MAX_BACKOFF_TIME} seconds if "
             f"not provided"
    )
    parser.add_argument(
        "--max_retries",
        required=False,
        default=MAX_RETRIES,
        help=f"The maximum number of retries for a failed request. Defaults to {MAX_RETRIES} if not provided."
    )
    parser.add_argument(
        "--dataset_self_hosted",
        action="store_true",
        help="If used then experimentalSelfHosted in new dataset will be set to True. This means " +
             "does not copy files into dataset, just symlinks out to current location."
    )
    parser.add_argument(
        "--file_path_flat",
        action="store_true",
        help="If used then 'path' in fileref info in dataset will replace '/' with '_'."
    )
    parser.add_argument(
        "--filter_existing_ids",
        action="store_true",
        help="If used then will filter out rows where id already exist in the dataset from new ingest."
    )
    parser.add_argument(
        "--already_added_to_auth_domain",
        action="store_true",
        help="If used will not stop after creating dataset and will assume tdr account already added to auth domians."
    )

    return parser.parse_args()


class CreateIngestTableInfo:
    """Create a list of dictionaries for each table to ingest"""

    def __init__(
            self,
            file_paths_dict: list[dict],
            metadata_table_names: list[str],
            workspace_metadata: list[dict],
            terra_workspace: TerraWorkspace
    ) -> None:
        self.file_paths_dict = file_paths_dict
        self.metadata_table_names = metadata_table_names
        self.workspace_metadata = workspace_metadata
        self.terra_workspace = terra_workspace

    @staticmethod
    def _create_data_table_dict(table_name: str) -> dict:
        table_unique_id = f"{table_name}_id"
        # Download table metadata from Terra
        table_metadata = terra_workspace.get_gcp_workspace_metrics(
            entity_type=table_name)
        # Convert table metadata to ingest metadata
        ingest_metadata = ConvertTerraTableInfoForIngest(
            table_metadata=table_metadata,
            tdr_row_id=table_unique_id
        ).run()
        return {
            'table_name': table_name,
            'primary_key': table_unique_id,
            'table_unique_id': table_unique_id,
            'ingest_metadata': ingest_metadata,
            'file_list': False
        }

    def _create_workspace_table_dict(self) -> dict:
        return {
            'table_name': 'workspace_metadata',
            'primary_key': None,
            'table_unique_id': 'attribute',
            'ingest_metadata': self.workspace_metadata,
            'file_list': False
        }

    def _create_file_metadata_table_dict(self) -> dict:
        return {
            'table_name': FILE_INVENTORY_TABLE_NAME,
            'primary_key': None,
            'table_unique_id': 'path',
            'ingest_metadata': self.file_paths_dict,
            'file_list': True
        }

    def run(self) -> dict:
        # Create a dictionary of all tables to ingest
        ingest_table_dict = {FILE_INVENTORY_TABLE_NAME: self._create_file_metadata_table_dict()}
        # Add workspace attributes to ingest only if exists
        if self.workspace_metadata:
            ingest_table_dict['workspace_metadata'] = self._create_workspace_table_dict()
        # Add all other workspace tables to ingest
        ingest_table_dict.update(
            {
                table_name: self._create_data_table_dict(table_name)
                for table_name in self.metadata_table_names
            }
        )
        return ingest_table_dict


class DataSetName:
    def __init__(self, workspace_name: str, billing_profile: str, tdr: TDR, dataset_name: Optional[str] = None) -> None:
        self.workspace_name = workspace_name
        self.billing_profile = billing_profile
        self.tdr = tdr
        self.dataset_name = dataset_name

    def format_dataset_name(self) -> tuple[str, str]:
        date_string = datetime.now().strftime("%Y%m%d")
        cleaned_name = re.sub(
            "^ANVIL[_]?", "", self.workspace_name, flags=re.IGNORECASE)
        formatted_prefix = re.sub(
            "[^a-zA-Z0-9_]", "_", f"ANVIL_{cleaned_name}")
        return formatted_prefix, date_string

    def get_name(self) -> str:
        if self.dataset_name:
            return self.dataset_name
        dataset_prefix, dataset_suffix = self.format_dataset_name()
        # Duplicating check done again later, but checking if prefix already exists with other date
        existing_datasets = self.tdr.check_if_dataset_exists(
            dataset_name=dataset_prefix,
            billing_profile=self.billing_profile,
        )
        # Check if multiple datasets exist with the same prefix or if dataset exists with different date
        if len(existing_datasets) > 1:
            dataset_info_str = ", ".join(
                [f"{dataset['name']} - {dataset['id']}" for dataset in existing_datasets]
            )
            logging.error(
                f"Set dataset name to use manually. {len(existing_datasets)} datasets found with prefix"
                f" {dataset_prefix}: {dataset_info_str}"
            )
            sys.exit(1)
        if len(existing_datasets) == 1 and existing_datasets[0]["name"] != f"{dataset_prefix}_{dataset_suffix}":
            logging.error(
                f"Set dataset name to use manually. Dataset with prefix {dataset_prefix} already exists:"
                f" {existing_datasets[0]['name']} - {existing_datasets[0]['id']}"
            )
            sys.exit(1)
        return f"{dataset_prefix}_{dataset_suffix}"


def run_filter_and_ingest(
        table_info_dict: dict,
        update_strategy: str,
        workspace_name: str,
        dataset_name: str,
        file_path_flat: bool,
        file_ingest_batch_size: int,
        filter_existing_ids: bool,
        file_to_uuid_dict: Optional[dict] = None,
) -> None:
    table_name = table_info_dict["table_name"]
    ingest_metadata = table_info_dict["ingest_metadata"]
    table_unique_id = table_info_dict["table_unique_id"]
    file_list_bool = table_info_dict["file_list"]
    schema_info = table_info_dict["schema"]

    # Set waiting time to poll and batch size based on if files are being ingested
    if table_name != FILE_INVENTORY_TABLE_NAME:
        waiting_time_to_poll = METADATA_INGEST_WAITING_TIME_TO_POLL
        ingest_batch_size = METADATA_INGEST_BATCH_SIZE
    else:
        waiting_time_to_poll = FILE_INGEST_WAITING_TIME_TO_POLL
        ingest_batch_size = file_ingest_batch_size

    # Filter out all rows that already exist in the dataset and batch ingests to table
    FilterAndBatchIngest(
        tdr=tdr,
        filter_existing_ids=filter_existing_ids,
        unique_id_field=table_unique_id,
        table_name=table_name,
        ingest_metadata=ingest_metadata,
        dataset_id=dataset_id,
        file_list_bool=file_list_bool,
        ingest_waiting_time_poll=waiting_time_to_poll,
        ingest_batch_size=ingest_batch_size,
        bulk_mode=bulk_mode,
        cloud_type=CLOUD_TYPE,
        update_strategy=update_strategy,
        load_tag=f"{workspace_name}-{dataset_name}",
        test_ingest=TEST_INGEST,
        dest_file_path_flat=file_path_flat,
        file_to_uuid_dict=file_to_uuid_dict,
        schema_info=schema_info
    ).run()


if __name__ == "__main__":
    args = get_args()
    billing_project = args.billing_project
    workspace_name = args.workspace_name
    provided_dataset_name = args.dataset_name
    phs_id = args.phs_id
    update_strategy = args.update_strategy
    bulk_mode = args.bulk_mode
    tdr_billing_profile = args.tdr_billing_profile
    file_ingest_batch_size = args.file_ingest_batch_size
    max_backoff_time = args.max_backoff_time
    max_retries = args.max_retries
    dataset_self_hosted = args.dataset_self_hosted
    file_path_flat = args.file_path_flat
    filter_existing_ids = args.filter_existing_ids
    already_added_to_auth_domain = args.already_added_to_auth_domain

    # Initialize the Terra and TDR classes
    token = Token(cloud=TOKEN_TYPE)
    request_util = RunRequest(
        token=token, max_retries=max_retries, max_backoff_time=max_backoff_time)
    terra_workspace = TerraWorkspace(
        billing_project=billing_project, workspace_name=workspace_name, request_util=request_util)
    terra = Terra(request_util=request_util)
    tdr = TDR(request_util=request_util)

    # Get Terra Workspace info
    workspace_info = terra_workspace.get_workspace_info()

    # Get all table names from within a workspace
    entity_info = terra_workspace.get_workspace_entity_info()
    workspace_table_names = [table_name for table_name in entity_info.keys()]

    # Get final dataset name
    dataset_name = DataSetName(tdr=tdr, workspace_name=workspace_name,
                               billing_profile=tdr_billing_profile, dataset_name=provided_dataset_name).get_name()

    workspace_properties_dict = {
        "auth_domains": workspace_info['workspace']['authorizationDomain'],
        "consent_name": workspace_info['workspace']["attributes"]["library:dataUseRestriction"] if workspace_info['workspace']["attributes"].get("library:dataUseRestriction") else "",  # noqa: E501
        "source_workspaces": [workspace_name]
    }

    # Create dict of additional properties for dataset
    additional_properties_dict = {
        "phsId": phs_id,
        "experimentalSelfHosted": dataset_self_hosted,
        "dedicatedIngestServiceAccount": True,
        "experimentalPredictableFileIds": True,
        "enableSecureMonitoring": DATASET_MONITORING,
        "properties": workspace_properties_dict,
    }

    # Check if dataset exists under billing profile and create if not there
    dataset_id = tdr.get_or_create_dataset(
        dataset_name=dataset_name,
        billing_profile=tdr_billing_profile,
        schema=FILE_INVENTORY_DEFAULT_SCHEMA,
        description=f"Ingest of {workspace_name}",
        cloud_platform=CLOUD_TYPE,
        additional_properties_dict=additional_properties_dict
    )

    # Get all schema info within dataset
    data_set_info = tdr.get_dataset_info(
        dataset_id=dataset_id, info_to_include=["DATA_PROJECT"])

    # Get all files in workspace bucket
    workspace_bucket_files = GCPCloudFunctions().list_bucket_contents(
        bucket_name=workspace_info["workspace"]["bucketName"],
        file_strings_to_ignore=["SubsetHailJointCall", ".vds/"]  # Ignore hail files
    )

    # Create workspace attributes for ingestion
    workspace_attributes_ingest_dict = terra_workspace.create_workspace_attributes_ingest_dict(
        workspace_info["workspace"]["attributes"])

    # Create dictionary of dictionaries for each table for ingestion
    # Do this outside because we are ingesting combo of workspace metadata,
    # workspace tables, and file metadata.
    tables_to_ingest = CreateIngestTableInfo(
        file_paths_dict=workspace_bucket_files,
        metadata_table_names=workspace_table_names,
        workspace_metadata=workspace_attributes_ingest_dict,
        terra_workspace=terra_workspace
    ).run()

    # Check and Set up all tables in dataset. Return dict for each table with schema info
    dataset_table_schema_info = SetUpTDRTables(
        tdr=tdr,
        dataset_id=dataset_id,
        table_info_dict=tables_to_ingest
    ).run()
    # Add schema information to tables_to_ingest
    for table_name in dataset_table_schema_info.keys():
        tables_to_ingest[table_name]['schema'] = dataset_table_schema_info[table_name]

    # Ensure dataset SA account is reader on Terra workspace and in auth domain if it exists
    GetPermissionsForWorkspaceIngest(
        terra_workspace=terra_workspace,
        dataset_info=data_set_info,
        added_to_auth_domain=already_added_to_auth_domain
    ).run()

    # Ingest just the files first
    run_filter_and_ingest(
        table_info_dict=tables_to_ingest[FILE_INVENTORY_TABLE_NAME],
        update_strategy=update_strategy,
        workspace_name=workspace_name,
        dataset_name=dataset_name,
        file_ingest_batch_size=file_ingest_batch_size,
        file_path_flat=file_path_flat,
        filter_existing_ids=filter_existing_ids
    )

    # Get all file info from dataset
    existing_file_inventory_metadata = tdr.get_data_set_table_metrics(
        dataset_id=dataset_id, target_table_name=FILE_INVENTORY_TABLE_NAME)

    # Create dictionary to map input file paths to uuids
    file_to_uuid_dict = {
        file_dict['path']: file_dict['file_ref']
        for file_dict in existing_file_inventory_metadata
    }

    for table_name, table_dict in tables_to_ingest.items():
        # Skip file inventory table which should already be ingested
        if table_name != FILE_INVENTORY_TABLE_NAME:
            run_filter_and_ingest(
                table_info_dict=table_dict,
                update_strategy=update_strategy,
                workspace_name=workspace_name,
                dataset_name=dataset_name,
                file_to_uuid_dict=file_to_uuid_dict,
                file_ingest_batch_size=file_ingest_batch_size,
                filter_existing_ids=filter_existing_ids,
                file_path_flat=file_path_flat
            )
