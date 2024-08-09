from utils import TerraWorkspace, TDR, RunRequest, Token, ConvertTerraTableInfoForIngest, GCPCloudFunctions, \
    FilterOutSampleIdsAlreadyInDataset, GCP, BatchIngest, InferTDRSchema, Terra, GetPermissionsForWorkspaceIngest, \
    CompareTDRDataset, FILE_INVENTORY_DEFAULT_SCHEMA
import logging
import json
import sys

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

TOKEN_TYPE = GCP
CLOUD_TYPE = GCP
MAX_RETRIES = 2  # The maximum number of retries for a failed request
MAX_BACKOFF_TIME = 10  # The maximum backoff time for a failed request
BILLING_PROJECT = "anvil-datastorage"
WORKSPACE_NAME = "ANVIL_CMG_Yale_GRU"
DATASET_NAME = "SN_TEST_DATASET"
TDR_BILLING_PROFILE = "e0e03e48-5b96-45ec-baa4-8cc1ebf74c61"  # Anvil prod billing profile id
PHS_ID = "phs000744"
DATASET_MONITORING = True
UPDATE_STRATEGY = "REPLACE"
BATCH_SIZE = 1000
BULK_MODE = False
WAITING_TIME_TO_POLL = 120
TEST_INGEST = False
FILTER_EXISTING_IDS = True
LOAD_TAG = f"{WORKSPACE_NAME}-{DATASET_NAME}"


class CreateIngestTable:
    """Create a list of dictionaries for each table to ingest"""
    def __init__(self, file_paths_dict: list[dict], metadata_table_names: list[str], workspace_metadata: dict, terra_workspace: TerraWorkspace):
        self.file_paths_dict = file_paths_dict
        self.metadata_table_names = metadata_table_names
        self.workspace_metadata = workspace_metadata
        self.terra_workspace = terra_workspace

    @staticmethod
    def _create_data_table_dict(table_name: str) -> dict:
        table_unique_id = f"{table_name}_id"
        # Download table metadata from Terra
        table_metadata = terra_workspace.get_gcp_workspace_metrics(entity_type=table_name)
        # Convert table metadata to ingest metadata
        ingest_metadata = ConvertTerraTableInfoForIngest(
            table_metadata=table_metadata,
            tdr_row_id=table_unique_id
        ).run()
        return {
            'table_name': table_name,
            'table_unique_id': table_unique_id,
            'ingest_metadata': ingest_metadata,
            'file_list': False
        }

    def _create_workspace_table_dict(self) -> dict:
        return {
            'table_name': 'workspace_metadata',
            'table_unique_id': 'attribute',
            'ingest_metadata': self.workspace_metadata,
            'file_list': False
        }

    def _create_file_metadata_table_dict(self) -> dict:
        return {
            'table_name': 'file_inventory',
            'table_unique_id': 'file_name',
            'ingest_metadata': self.file_paths_dict,
            'file_list': True
        }

    def run(self):
        # Put file list at top of list so all files get ingested first
        ingest_table_dicts = [self._create_file_metadata_table_dict()] + \
            [
                self._create_data_table_dict(table_name)
                for table_name in self.metadata_table_names
            ] + \
            [self._create_workspace_table_dict()]
        return ingest_table_dicts


if __name__ == "__main__":
    # Initialize the Terra and TDR classes
    token = Token(cloud=TOKEN_TYPE)
    request_util = RunRequest(token=token, max_retries=MAX_RETRIES, max_backoff_time=MAX_BACKOFF_TIME)
    terra_workspace = TerraWorkspace(billing_project=BILLING_PROJECT, workspace_name=WORKSPACE_NAME, request_util=request_util)
    terra = Terra(request_util=request_util)
    tdr = TDR(request_util=request_util)

    # Get Terra Workspace info
    workspace_info = terra_workspace.get_workspace_info()

    # Get all table names from within a workspace
    entity_info = terra_workspace.get_workspace_entity_info()
    workspace_table_names = [table_name for table_name in entity_info.keys()]

    properties_dict = {
        "auth_domains": workspace_info['workspace']['authorizationDomain'],
        "consent_name": workspace_info['workspace']["attributes"]["library:dataUseRestriction"] if workspace_info['workspace']["attributes"].get("library:dataUseRestriction") else "",
        "source_workspaces": [WORKSPACE_NAME]
    }

    # Check if dataset exists under billing profile
    dataset_id = tdr.get_or_create_dataset(
        dataset_name=DATASET_NAME,
        billing_profile=TDR_BILLING_PROFILE,
        schema=FILE_INVENTORY_DEFAULT_SCHEMA,  # Defaults to adding file_inventory table
        staging_area_name=WORKSPACE_NAME,  # Not needed
        phs_id=PHS_ID,  # Not required, for anvil we want to get it from tag or make it as manual input
        monitoring_needed=DATASET_MONITORING,  # bool,
        properties_dict=properties_dict,  # dict. what is it?,
        cloud_platform=CLOUD_TYPE
    )

    # Get all schema info within dataset
    data_set_info = tdr.get_data_set_info(dataset_id=dataset_id, info_to_include=['DATA_PROJECT', 'SCHEMA'])
    tdr_schema_info = {
        table_dict['name']: table_dict['columns']
        for table_dict in data_set_info['schema']['tables']
    }

    # Ensure dataset SA account is reader on Terra workspace and in auth domain if it exists
    GetPermissionsForWorkspaceIngest(terra_workspace=terra_workspace, terra=terra, dataset_info=data_set_info).run()

    # Get all files in workspace bucket
    workspace_bucket_files = GCPCloudFunctions(
        bucket_name=workspace_info["workspace"]["bucketName"]
    ).list_bucket_contents(file_strings_to_ignore=['SubsetHailJointCall', '.vds/'])  # Ignore hail files

    # Create workspace attributes for ingestion
    workspace_attributes_ingest_dict = terra_workspace.create_workspace_attributes_ingest_dict(workspace_info)

    # Create a list of dictionaries for each table for ingestion
    # Do this outside because we are ingesting combo of workspace metadata,
    # workspace tables, and file metadata.
    tables_to_ingest = CreateIngestTable(
        file_paths_dict=workspace_bucket_files,
        metadata_table_names=workspace_table_names,
        workspace_metadata=workspace_attributes_ingest_dict,
        terra_workspace=terra_workspace
    ).run()

    for table_dict in tables_to_ingest:
        table_name = table_dict['table_name']
        ingest_metadata = table_dict['ingest_metadata']
        table_unique_id = table_dict['table_unique_id']
        file_list_bool = table_dict['file_list']

        # Get TDR schema info for tables to ingest
        expected_tdr_schema_dict = InferTDRSchema(
            input_metadata=ingest_metadata,
            table_name=table_name
        ).infer_schema()

        # Check if table already exists in dataset and compare it to schema_info
        # Will be new class that takes in table info and either creates a table or confirms it is up to date
        #if table_dict['table_name'] in tdr_schema_info:
        #    columns_to_update = CompareTDRDataset(
        #        reference_dataset_metadata=expected_tdr_schema_dict,
        #        target_dataset_metadata=tdr_schema_info[table_name]
        #    ).compare_table()
        #    if columns_to_update:
        #        # Patch table schema
        #        pass
        #else:
        #    tdr.create_table(
        #        table_dict=expected_tdr_schema_dict,
        #        dataset_id=dataset_id
        #    )

        if FILTER_EXISTING_IDS:
            # Filter out sample ids that are already in the dataset
            filtered_metrics = FilterOutSampleIdsAlreadyInDataset(
                ingest_metrics=ingest_metadata,
                dataset_id=dataset_id,
                tdr=tdr,
                target_table_name=table_name,
                filter_entity_id=table_unique_id
            ).run()
        else:
            filtered_metrics = ingest_metadata

        # Batch ingest of table to table within dataset
        BatchIngest(
            ingest_metadata=filtered_metrics,
            tdr=tdr,
            target_table_name=table_name,
            dataset_id=dataset_id,
            batch_size=BATCH_SIZE,
            bulk_mode=BULK_MODE,
            cloud_type=CLOUD_TYPE,
            update_strategy=UPDATE_STRATEGY,
            waiting_time_to_poll=WAITING_TIME_TO_POLL,
            test_ingest=TEST_INGEST,
            load_tag=LOAD_TAG,
            file_list_bool=file_list_bool,
            dest_file_path_flat=True
        ).run()

