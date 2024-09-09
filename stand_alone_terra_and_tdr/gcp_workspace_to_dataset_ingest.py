import logging
from argparse import ArgumentParser

from utils import (
    TerraWorkspace,
    TDR, RunRequest,
    Token,
    ConvertTerraTableInfoForIngest,
    FilterOutSampleIdsAlreadyInDataset,
    GCP,
    BatchIngest
)


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

# Columns to ignore when ingesting
COLUMNS_TO_IGNORE = ['datarepo_row_id', 'import:timestamp', 'import:snapshot_id', 'tdr:sample_id']
CLOUD_TYPE = GCP
BATCH_SIZE = 700  # The number of rows to ingest at a time
WAITING_TIME_TO_POLL = 120  # How long to wait between polling for ingest status
MAX_RETRIES = 5  # The maximum number of retries for a failed request
MAX_BACKOFF_TIME = 500  # The maximum backoff time for a failed request
TEST_INGEST = False  # Whether to test the ingest by just doing first batch
FILTER_EXISTING_IDS = False  # Filter for out rows where it already exists within the dataset
LOAD_TAG = "0d1c9aea-e944-4d19-83c3-8675f6aa062a.sample"  # Load tag used for ingest


def get_args():
    parser = ArgumentParser(description="Ingest data into an existing dataset from a GCP workspace")
    parser.add_argument("--billing_project", required=True)
    parser.add_argument("--workspace_name", required=True)
    parser.add_argument("--dataset_id", required=True)
    parser.add_argument("--target_table_name", required=True, help="The name of the table in TDR")
    parser.add_argument(
        "--tdr_row_id",
        required=True,
        help="The name of the column in TDR that corresponds to the TDR row ID"
    )
    parser.add_argument(
        "--update_strategy",
        required=False,
        choices=["REPLACE", "APPEND", "UPDATE"],
        default="REPLACE",
        help="Defaults to REPLACE if not provided"
    )
    parser.add_argument(
        "--sample_ids_to_ingest",
        nargs="*",
        required=False,
        help="Provide a list of sample IDs if not all samples should be ingested"
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
        "--max_retries",
        required=False,
        default=MAX_RETRIES,
        help=f"The maximum number of retries for a failed request. Defaults to {MAX_RETRIES} if not provided"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    billing_project = args.billing_project
    workspace_name = args.workspace_name
    dataset_id = args.dataset_id
    target_table_name = args.target_table_name
    tdr_row_id = args.tdr_row_id
    update_strategy = args.update_strategy
    sample_ids_to_ingest = args.sample_ids_to_ingest
    bulk_mode = args.bulk_mode
    max_retries = args.max_retries

    # Initialize the Terra and TDR classes
    token = Token(cloud=CLOUD_TYPE)
    request_util = RunRequest(token=token, max_retries=MAX_RETRIES, max_backoff_time=MAX_BACKOFF_TIME)
    terra_workspace = TerraWorkspace(billing_project=billing_project, workspace_name=workspace_name, request_util=request_util)
    tdr = TDR(request_util=request_util)

    # Get sample metrics from Terra
    sample_metrics = terra_workspace.get_gcp_workspace_metrics(entity_type="sample")
    logging.info(f"Got {len(sample_metrics)} samples")

    # Convert sample dict into list of usable dicts for ingestion
    updated_metrics = ConvertTerraTableInfoForIngest(
        table_metadata=sample_metrics,
        tdr_row_id=tdr_row_id,
        columns_to_ignore=COLUMNS_TO_IGNORE
    ).run()

    # Use only specific sample ids if provided
    if sample_ids_to_ingest:
        updated_metrics = [metric for metric in updated_metrics if metric[tdr_row_id] in sample_ids_to_ingest]

    if FILTER_EXISTING_IDS:
        # Filter out sample ids that are already in the dataset
        filtered_metrics = FilterOutSampleIdsAlreadyInDataset(
            ingest_metrics=updated_metrics,
            dataset_id=dataset_id,
            tdr=tdr,
            target_table_name=target_table_name,
            filter_entity_id=tdr_row_id,
        ).run()
    else:
        filtered_metrics = updated_metrics

    BatchIngest(
        ingest_metadata=filtered_metrics,
        tdr=tdr,
        target_table_name=target_table_name,
        dataset_id=dataset_id,
        batch_size=BATCH_SIZE,
        bulk_mode=bulk_mode,
        cloud_type=CLOUD_TYPE,
        update_strategy=update_strategy,
        waiting_time_to_poll=WAITING_TIME_TO_POLL,
        test_ingest=TEST_INGEST,
        load_tag=LOAD_TAG,
        file_list_bool=False
    ).run()
