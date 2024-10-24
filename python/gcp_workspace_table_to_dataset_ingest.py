import argparse
import logging

from utils import GCP, comma_separated_list, ARG_DEFAULTS
from utils.terra_utils.terra_util import TerraWorkspace
from utils.tdr_utils.tdr_api_utils import TDR, FilterOutSampleIdsAlreadyInDataset
from utils.tdr_utils.tdr_ingest_utils import (
    ConvertTerraTableInfoForIngest,
    GetPermissionsForWorkspaceIngest,
    BatchIngest
)
from utils.tdr_utils.tdr_table_utils import SetUpTDRTables
from utils.token_util import Token
from utils.request_util import RunRequest


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

# Columns to ignore when ingesting
COLUMNS_TO_IGNORE = ["datarepo_row_id", "import:timestamp", "import:snapshot_id", "tdr:sample_id"]
CLOUD_TYPE = GCP
TEST_INGEST = False  # Whether to test the ingest by just doing first batch
# Filter for out rows where it already exists within the dataset
FILTER_EXISTING_IDS = False


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest data into an existing dataset from a GCP workspace")
    parser.add_argument("--billing_project", required=True)
    parser.add_argument("--workspace_name", required=True)
    parser.add_argument("--dataset_id", required=True)
    parser.add_argument(
        "--terra_tables",
        required=True,
        help="The name(s) of the Terra table(s) that you'd like to import into TDR. Comma separated",
        type=comma_separated_list,
    )
    parser.add_argument(
        "--update_strategy",
        required=False,
        choices=["REPLACE", "APPEND", "UPDATE"],
        default=ARG_DEFAULTS["update_strategy"],
        help="Defaults to REPLACE if not provided"
    )
    parser.add_argument(
        "--records_to_ingest",
        required=False,
        help="A list of records (primary keys) to ingest if not all records should be ingested into TDR",
        type=comma_separated_list,
    )
    parser.add_argument(
        "--bulk_mode",
        action="store_true",
        help="""If used, will use bulk mode for ingest. Using bulk mode for TDR Ingest loads data faster when ingesting
             a large number of files (e.g. more than 10,000 files) at once. The performance does come at the cost of
             some safeguards (such as guaranteed rollbacks and potential recopying of files) and it also forces
             exclusive  locking of the dataset (i.e. you can’t run multiple ingests at once)"""
    )
    parser.add_argument(
        "--max_retries",
        required=False,
        default=ARG_DEFAULTS["max_retries"],
        help="The maximum number of retries for a failed request. " +
             f"Defaults to {ARG_DEFAULTS['max_retries']} if not provided"
    )
    parser.add_argument(
        "--max_backoff_time",
        required=False,
        default=ARG_DEFAULTS["max_backoff_time"],
        help="The maximum backoff time for a failed request (in seconds). " +
             f"Defaults to {ARG_DEFAULTS['max_backoff_time']} if not provided"
    )
    parser.add_argument(
        "--filter_existing_ids",
        action="store_true",
        help="Whether records that exist in the dataset should be re-ingested. Defaults to false"
    )
    parser.add_argument(
        "--batch_size",
        required=False,
        default=ARG_DEFAULTS["batch_size"],
        help=f"""The number of rows to ingest at a time. Defaults to {ARG_DEFAULTS['batch_size']} if not provided"""
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    billing_project = args.billing_project
    workspace_name = args.workspace_name
    dataset_id = args.dataset_id
    terra_tables = args.terra_tables
    update_strategy = args.update_strategy
    records_to_ingest = args.records_to_ingest
    bulk_mode = args.bulk_mode
    max_retries = args.max_retries
    max_backoff_time = args.max_backoff_time
    filter_existing_ids = args.filter_existing_ids
    batch_size = args.batch_size

    # Initialize the Terra and TDR classes
    token = Token(cloud=CLOUD_TYPE)
    request_util = RunRequest(token=token, max_retries=max_retries, max_backoff_time=max_backoff_time)
    terra_workspace = TerraWorkspace(
        billing_project=billing_project, workspace_name=workspace_name, request_util=request_util
    )
    tdr = TDR(request_util=request_util)

    GetPermissionsForWorkspaceIngest(
        terra_workspace=terra_workspace,
        dataset_info=tdr.get_dataset_info(dataset_id=dataset_id),
        added_to_auth_domain=True,
    ).run()
    entity_metrics = terra_workspace.get_workspace_entity_info()

    for terra_table_name in terra_tables:
        target_table_name = terra_table_name

        # Get sample metrics from Terra
        sample_metrics = terra_workspace.get_gcp_workspace_metrics(entity_type=terra_table_name)
        primary_key_column_name = entity_metrics[terra_table_name]["idName"]
        logging.info(f"Got {len(sample_metrics)} samples")

        # Convert sample dict into list of usable dicts for ingestion
        updated_metrics = ConvertTerraTableInfoForIngest(
            table_metadata=sample_metrics,
            tdr_row_id=primary_key_column_name,
            columns_to_ignore=COLUMNS_TO_IGNORE
        ).run()

        # Use only specific sample ids if provided
        if records_to_ingest:
            updated_metrics = [
                metric for metric in updated_metrics if metric[primary_key_column_name] in records_to_ingest
            ]

        if filter_existing_ids:
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
            batch_size=batch_size,
            bulk_mode=bulk_mode,
            cloud_type=CLOUD_TYPE,
            update_strategy=update_strategy,
            waiting_time_to_poll=ARG_DEFAULTS['waiting_time_to_poll'],
            test_ingest=TEST_INGEST,
            load_tag=f"{billing_project}_{workspace_name}-{dataset_id}",
            file_list_bool=False
        ).run()
