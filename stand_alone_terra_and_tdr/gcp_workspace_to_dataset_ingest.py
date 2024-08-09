from utils import TerraWorkspace, TDR, RunRequest, Token, ConvertTerraTableInfoForIngest, \
    FilterOutSampleIdsAlreadyInDataset, GCP, BatchIngest
import logging
import json

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

BILLING_PROJECT = "broad-genomics-data"
WORKSPACE_NAME = "sn_DI-339_ingest"
DATA_SET_ID = "0d1c9aea-e944-4d19-83c3-8675f6aa062a"
TARGET_TABLE_NAME = 'sample'  # The name of the table in TDR
# Columns to ignore when ingesting
COLUMNS_TO_IGNORE = ['datarepo_row_id', 'import:timestamp', 'import:snapshot_id', 'tdr:sample_id']
TDR_ROW_ID = 'sample_id'  # What is the column name in the Terra table that corresponds to the TDR row id
CLOUD_TYPE = GCP
BATCH_SIZE = 700  # The number of rows to ingest at a time
WAITING_TIME_TO_POLL = 120  # How long to wait between polling for ingest status
MAX_RETRIES = 5  # The maximum number of retries for a failed request
MAX_BACKOFF_TIME = 500  # The maximum backoff time for a failed request
BULK_MODE = False  # What to use for bulk_mode in ingest. True or False
TEST_INGEST = False  # Whether to test the ingest by just doing first batch
UPDATE_STRATEGY = "replace"  # What to use for update_strategy in ingest. replace, append, or update
FILTER_EXISTING_IDS = False  # Filter for out rows where it already exists within the dataset
LOAD_TAG = "0d1c9aea-e944-4d19-83c3-8675f6aa062a.sample"  # Load tag used for ingest
SAMPLE_IDS_TO_INGEST = ['SM-NEIDM']  # If you want to ingest specific sample ids, put them here

if __name__ == "__main__":
    # Initialize the Terra and TDR classes
    token = Token(cloud=CLOUD_TYPE)
    request_util = RunRequest(token=token, max_retries=MAX_RETRIES, max_backoff_time=MAX_BACKOFF_TIME)
    terra_workspace = TerraWorkspace(billing_project=BILLING_PROJECT, workspace_name=WORKSPACE_NAME, request_util=request_util)
    tdr = TDR(request_util=request_util)

    # Get sample metrics from Terra
    sample_metrics = terra_workspace.get_gcp_workspace_metrics(entity_type="sample")
    logging.info(f"Got {len(sample_metrics)} samples")

    # Convert sample dict into list of usable dicts for ingestion
    updated_metrics = ConvertTerraTableInfoForIngest(
        table_metadata=sample_metrics,
        tdr_row_id=TDR_ROW_ID,
        columns_to_ignore=COLUMNS_TO_IGNORE
    ).run()

    # Use only specific sample ids if provided
    if SAMPLE_IDS_TO_INGEST:
        updated_metrics = [metric for metric in updated_metrics if metric[TDR_ROW_ID] in SAMPLE_IDS_TO_INGEST]

    if FILTER_EXISTING_IDS:
        # Filter out sample ids that are already in the dataset
        filtered_metrics = FilterOutSampleIdsAlreadyInDataset(
            ingest_metrics=updated_metrics,
            dataset_id=DATA_SET_ID,
            tdr=tdr,
            target_table_name=TARGET_TABLE_NAME,
            filter_entity_id=TDR_ROW_ID
        ).run()
    else:
        filtered_metrics = updated_metrics

    BatchIngest(
        ingest_metadata=filtered_metrics,
        tdr=tdr,
        target_table_name=TARGET_TABLE_NAME,
        dataset_id=DATA_SET_ID,
        batch_size=BATCH_SIZE,
        bulk_mode=BULK_MODE,
        cloud_type=CLOUD_TYPE,
        update_strategy=UPDATE_STRATEGY,
        waiting_time_to_poll=WAITING_TIME_TO_POLL,
        test_ingest=TEST_INGEST,
        load_tag=LOAD_TAG
    ).run()
