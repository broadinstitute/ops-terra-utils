from utils import TerraWorkspace, TDR, RunRequest, Token, ConvertTerraTableInfoForIngest, \
    GCP, Terra, GetPermissionsForWorkspaceIngest, FILE_INVENTORY_DEFAULT_SCHEMA, \
    SetUpTDRTables, GCPCloudFunctions, FilterAndBatchIngest
import logging
from datetime import datetime
import re
from typing import Optional
import json
import sys

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

TOKEN_TYPE = GCP  # The cloud type for the token
CLOUD_TYPE = GCP  # The cloud type for the TDR dataset and workspace
MAX_RETRIES = 2  # The maximum number of retries for a failed request
MAX_BACKOFF_TIME = 10  # The maximum backoff time for a failed request
# The billing project for the workspace to be moved over
BILLING_PROJECT = "anvil-datastorage"
WORKSPACE_NAME = "ANVIL_CMG_Yale_GRU"  # The workspace name to move over
# If None then will create a default dataset name based off workspace name
DATASET_NAME = None
# Anvil prod billing profile id
TDR_BILLING_PROFILE = "e0e03e48-5b96-45ec-baa4-8cc1ebf74c61"
DATASET_MONITORING = True  # Enable monitoring for dataset
PHS_ID = "phs000744"  # The PHS ID for the dataset
# What to use for update_strategy in ingest. replace, append, or update
UPDATE_STRATEGY = "REPLACE"
# The number of rows to ingest at a time when ingesting files
FILE_INGEST_BATCH_SIZE = 500
# How long to wait between polling for ingest status when ingesting files
FILE_INGEST_WAITING_TIME_TO_POLL = 30
# The number of rows to ingest at a time when ingesting metadata
METADATA_INGEST_BATCH_SIZE = 1000
# How long to wait between polling for ingest status when ingesting metadata
METADATA_INGEST_WAITING_TIME_TO_POLL = 45
BULK_MODE = False  # What to use for bulk_mode in ingest. True or False
TEST_INGEST = False  # Whether to test the ingest by just doing first batch
# Filter for out rows where it already exists within the dataset
FILTER_EXISTING_IDS = True
# If the dataset SA account is already in the workspace auth domain
ALREADY_ADDED_TO_AUTH_DOMAIN = True
# If the file path should be flat or not. Will replace '/' with '_'
DEST_FILE_PATH_FLAT = True

FILE_INVENTORY_TABLE_NAME = "file_inventory"


class CreateIngestTableInfo:
    """Create a list of dictionaries for each table to ingest"""

    def __init__(self, file_paths_dict: list[dict], metadata_table_names: list[str], workspace_metadata: list[dict], terra_workspace: TerraWorkspace):
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
        ingest_table_dict = {
            'workspace_metadata': self._create_workspace_table_dict(),
            FILE_INVENTORY_TABLE_NAME: self._create_file_metadata_table_dict()
        }
        # Add all other workspace tables to ingest
        ingest_table_dict.update(
            {
                table_name: self._create_data_table_dict(table_name)
                for table_name in self.metadata_table_names
            }
        )
        return ingest_table_dict


class DataSetName:
    def __init__(self, workspace_name: str, billing_profile: str, tdr: TDR, dataset_name: Optional[str] = None):
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
            billing_profile=TDR_BILLING_PROFILE
        )
        # Check if multiple datasets exist with the same prefix or if dataset exists with different date
        if len(existing_datasets) > 1:
            dataset_info_str = ', '.join(
                [f"{dataset['name']} - {dataset['id']}" for dataset in existing_datasets]
            )
            logging.error(
                f"Set dataset name to use manually. {len(existing_datasets)} datasets found with prefix {dataset_prefix}: {dataset_info_str}")
            sys.exit(1)
        if len(existing_datasets) == 1 and existing_datasets[0]['name'] != f"{dataset_prefix}_{dataset_suffix}":
            logging.error(
                f"Set dataset name to use manually. Dataset with prefix {dataset_prefix} already exists: {existing_datasets[0]['name']} - {existing_datasets[0]['id']}")
            sys.exit(1)
        return f"{dataset_prefix}_{dataset_suffix}"


def run_filter_and_ingest(table_info_dict: dict, file_to_uuid_dict: Optional[dict] = None) -> None:
    table_name = table_info_dict['table_name']
    ingest_metadata = table_info_dict['ingest_metadata']
    table_unique_id = table_info_dict['table_unique_id']
    file_list_bool = table_info_dict['file_list']
    schema_info = table_info_dict['schema']

    # Set waiting time to poll and batch size based on if files are being ingested
    if table_name != FILE_INVENTORY_TABLE_NAME:
        waiting_time_to_poll = METADATA_INGEST_WAITING_TIME_TO_POLL
        ingest_batch_size = METADATA_INGEST_BATCH_SIZE
    else:
        waiting_time_to_poll = FILE_INGEST_WAITING_TIME_TO_POLL
        ingest_batch_size = FILE_INGEST_BATCH_SIZE

    # Filter out all rows that already exist in the dataset and batch ingests to table
    FilterAndBatchIngest(
        tdr=tdr,
        filter_existing_ids=FILTER_EXISTING_IDS,
        unique_id_field=table_unique_id,
        table_name=table_name,
        ingest_metadata=ingest_metadata,
        dataset_id=dataset_id,
        file_list_bool=file_list_bool,
        ingest_waiting_time_poll=waiting_time_to_poll,
        ingest_batch_size=ingest_batch_size,
        bulk_mode=BULK_MODE,
        cloud_type=CLOUD_TYPE,
        update_strategy=UPDATE_STRATEGY,
        load_tag=f"{WORKSPACE_NAME}-{dataset_name}",
        test_ingest=TEST_INGEST,
        dest_file_path_flat=DEST_FILE_PATH_FLAT,
        file_to_uuid_dict=file_to_uuid_dict,
        schema_info=schema_info
    ).run()


if __name__ == "__main__":
    # Initialize the Terra and TDR classes
    token = Token(cloud=TOKEN_TYPE)
    request_util = RunRequest(
        token=token, max_retries=MAX_RETRIES, max_backoff_time=MAX_BACKOFF_TIME)
    terra_workspace = TerraWorkspace(
        billing_project=BILLING_PROJECT, workspace_name=WORKSPACE_NAME, request_util=request_util)
    terra = Terra(request_util=request_util)
    tdr = TDR(request_util=request_util)

    # Get Terra Workspace info
    workspace_info = terra_workspace.get_workspace_info()

    # Get all table names from within a workspace
    entity_info = terra_workspace.get_workspace_entity_info()
    workspace_table_names = [table_name for table_name in entity_info.keys()]

    # Get final dataset name
    dataset_name = DataSetName(tdr=tdr, workspace_name=WORKSPACE_NAME,
                               billing_profile=TDR_BILLING_PROFILE, dataset_name=DATASET_NAME).get_name()

    workspace_properties_dict = {
        "auth_domains": workspace_info['workspace']['authorizationDomain'],
        "consent_name": workspace_info['workspace']["attributes"]["library:dataUseRestriction"] if workspace_info['workspace']["attributes"].get("library:dataUseRestriction") else "",
        "source_workspaces": [WORKSPACE_NAME]
    }

    # Create dict of additional properties for dataset
    additional_properties_dict = {
        "phsId": PHS_ID,
        "experimentalSelfHosted": True,
        "dedicatedIngestServiceAccount": True,
        "experimentalPredictableFileIds": True,
        "enableSecureMonitoring": DATASET_MONITORING,
        "properties": workspace_properties_dict,
    }

    # Check if dataset exists under billing profile and create if not there
    dataset_id = tdr.get_or_create_dataset(
        dataset_name=dataset_name,
        billing_profile=TDR_BILLING_PROFILE,
        schema=FILE_INVENTORY_DEFAULT_SCHEMA,
        description=f"Ingest of {WORKSPACE_NAME}",
        cloud_platform=CLOUD_TYPE,
        additional_properties_dict=additional_properties_dict
    )

    # Get all schema info within dataset
    data_set_info = tdr.get_data_set_info(
        dataset_id=dataset_id, info_to_include=['DATA_PROJECT'])

    # Get all files in workspace bucket
    workspace_bucket_files = GCPCloudFunctions(
        bucket_name=workspace_info["workspace"]["bucketName"]
    ).list_bucket_contents(file_strings_to_ignore=['SubsetHailJointCall', '.vds/'])  # Ignore hail files

    # Create workspace attributes for ingestion
    workspace_attributes_ingest_dict = terra_workspace.create_workspace_attributes_ingest_dict(
        workspace_info['workspace']['attributes'])

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
        terra=terra,
        dataset_info=data_set_info,
        added_to_auth_domain=ALREADY_ADDED_TO_AUTH_DOMAIN
    ).run()

    # Ingest just the files first
    run_filter_and_ingest(
        table_info_dict=tables_to_ingest[FILE_INVENTORY_TABLE_NAME])

    # Get all file info from dataset
    existing_file_inventory_metadata = tdr.get_data_set_table_metrics(
        dataset_id=dataset_id, target_table_name=FILE_INVENTORY_TABLE_NAME)
    # Create dictionary to map input file paths to uuids
    file_to_uuid_dict = {
        file_dict['file_path']: file_dict['file_ref']
        for file_dict in existing_file_inventory_metadata
    }

    for table_name, table_dict in tables_to_ingest.items():
        # Skip file inventory table which should already be ingested
        if table_name != FILE_INVENTORY_TABLE_NAME:
            run_filter_and_ingest(
                table_dict, file_to_uuid_dict=file_to_uuid_dict)
