import logging
from argparse import ArgumentParser
from utils import GCP
from utils.terra_util import TerraWorkspace
from utils.tdr_util import (
    TDR,
    ConvertTerraTableInfoForIngest,
    FilterOutSampleIdsAlreadyInDataset,
    BatchIngest,
    SetUpTDRTables
)
from utils.token_util import Token
from utils.request_util import RunRequest


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

# Columns to ignore when ingesting
COLUMNS_TO_IGNORE = ['datarepo_row_id', 'import:timestamp', 'import:snapshot_id', 'tdr:sample_id']
CLOUD_TYPE = GCP
BATCH_SIZE = 700  # The number of rows to ingest at a time
WAITING_TIME_TO_POLL = 120  # How long to wait between polling for ingest status
MAX_RETRIES = 5  # The maximum number of retries for a failed request
MAX_BACKOFF_TIME = 5 * 60  # The maximum backoff time for a failed request
TEST_INGEST = False  # Whether to test the ingest by just doing first batch
FILTER_EXISTING_IDS = False  # Filter for out rows where it already exists within the dataset


def get_args():
    parser = ArgumentParser(description="Ingest data into an existing dataset from a GCP workspace")
    parser.add_argument("--billing_project", required=True)
    parser.add_argument("--workspace_name", required=True)
    parser.add_argument("--dataset_id", required=True)
    parser.add_argument(
        "--terra_table_name",
        required=True,
        help="The name of the Terra table that you'd like to import into TDR"
    )
    parser.add_argument(
        "--target_table_name",
        required=False,
        help="The name of the table in TDR (this can either be an existing table, OR a new one). Will default to "
             "the Terra table name if not provided."
    )
    parser.add_argument(
        "--primary_key_column_name",
        required=True,
        help="The name of the primary key column in Terra"
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
    parser.add_argument(
        "--max_backoff_time",
        required=False,
        default=MAX_BACKOFF_TIME,
        help=f"The maximum backoff time for a failed request (in seconds). Defaults to {MAX_BACKOFF_TIME} seconds if not provided"
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    billing_project = args.billing_project
    workspace_name = args.workspace_name
    dataset_id = args.dataset_id
    terra_table_name = args.terra_table_name
    target_table_name = args.target_table_name if args.target_table_name else args.terra_table_name
    primary_key_column_name = args.primary_key_column_name
    update_strategy = args.update_strategy
    sample_ids_to_ingest = args.sample_ids_to_ingest
    bulk_mode = args.bulk_mode
    max_retries = args.max_retries
    max_backoff_time = args.max_backoff_time

    # Initialize the Terra and TDR classes
    token = Token(cloud=CLOUD_TYPE)
    request_util = RunRequest(token=token, max_retries=max_retries, max_backoff_time=max_backoff_time)
    terra_workspace = TerraWorkspace(billing_project=billing_project, workspace_name=workspace_name, request_util=request_util)
    tdr = TDR(request_util=request_util)

    # Get sample metrics from Terra
    sample_metrics = terra_workspace.get_gcp_workspace_metrics(entity_type=terra_table_name)
    logging.info(f"Got {len(sample_metrics)} samples")

    # Convert sample dict into list of usable dicts for ingestion
    updated_metrics = ConvertTerraTableInfoForIngest(
        table_metadata=sample_metrics,
        tdr_row_id=primary_key_column_name,
        columns_to_ignore=COLUMNS_TO_IGNORE
    ).run()

    # Use only specific sample ids if provided
    if sample_ids_to_ingest:
        updated_metrics = [
            metric for metric in updated_metrics if metric[primary_key_column_name] in sample_ids_to_ingest
        ]

    if FILTER_EXISTING_IDS:
        # Filter out sample ids that are already in the dataset
        filtered_metrics = FilterOutSampleIdsAlreadyInDataset(
            ingest_metrics=updated_metrics,
            dataset_id=dataset_id,
            tdr=tdr,
            target_table_name=target_table_name,
            filter_entity_id=primary_key_column_name,
        ).run()
    else:
        filtered_metrics = updated_metrics

    table_info_dict = {
        target_table_name: {
            "table_name": target_table_name,
            "primary_key": primary_key_column_name,
            "ingest_metadata": filtered_metrics,
            "file_list": False,
            "datePartitionOptions": None
        }

    }
    SetUpTDRTables(tdr=tdr, dataset_id=dataset_id, table_info_dict=table_info_dict).run()

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
        load_tag=f"{billing_project}_{workspace_name}-{dataset_id}",
        file_list_bool=False
    ).run()
