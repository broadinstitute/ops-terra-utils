"""Take in billing profile and dataset and recreate the dataset in a new billing profile."""


import logging
import sys
from argparse import ArgumentParser, Namespace

from utils.tdr_util import BatchIngest, TDR
from utils.request_util import RunRequest
from utils.token_util import Token
from utils import GCP


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

DEFAULT_WAITING_TIME_POLL = 120
DEFAULT_BATCH_SIZE = 500


def get_args() -> Namespace:
    parser = ArgumentParser(description="""Copy dataset to new billing profile""")
    parser.add_argument("--new_billing_profile", required=True)
    parser.add_argument("--orig_dataset_id", required=True)
    parser.add_argument(
        "--ingest_batch_size",
        help=f"Batch size for ingest. Default to {DEFAULT_BATCH_SIZE}",
        default=DEFAULT_BATCH_SIZE, type=int
    )
    parser.add_argument("--update_strategy", choices=["REPLACE", "APPEND", "UPDATE"], default="REPLACE")
    parser.add_argument(
        "--new_dataset_name",
        help="If not provided, will use the same name as the original dataset"
    )
    parser.add_argument(
        "--waiting_time_to_poll",
        help=f"default to {DEFAULT_WAITING_TIME_POLL}",
        default=DEFAULT_WAITING_TIME_POLL, type=int
    )
    parser.add_argument(
        "--bulk_mode",
        action="store_true",
        help="""If used, will use bulk mode for ingest. Using bulk mode for TDR Ingest loads data faster when ingesting
        a large number of files (e.g. more than 10,000 files) at once. The performance does come at the cost of some
        safeguards (such as guaranteed rollbacks and potential recopying of files)
        and it also forces exclusive locking of the dataset (i.e. you can’t run multiple ingests at once)."""
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()

    orig_dataset_id, update_strategy, time_to_poll = (
        args.orig_dataset_id, args.update_strategy, args.waiting_time_to_poll
    )
    bulk_mode, ingest_batch_size, billing_profile = args.bulk_mode, args.ingest_batch_size, args.new_billing_profile

    # Initialize the Terra and TDR classes
    token = Token(cloud=GCP)
    request_util = RunRequest(token=token)
    tdr = TDR(request_util=request_util)

    orig_dataset_info = tdr.get_dataset_info(orig_dataset_id)

    # Check dataset id is not already in requested billing profile
    if orig_dataset_info['defaultProfileId'] == billing_profile:
        logging.info(f"Dataset {orig_dataset_id} already in billing profile {billing_profile}")
        sys.exit(0)

    new_dataset_name = args.new_dataset_name if args.new_dataset_name else orig_dataset_info["name"]

    additional_properties = {
        "phsId": orig_dataset_info['phsId'],
        "experimentalSelfHosted": True,
        "dedicatedIngestServiceAccount": True,
        "experimentalPredictableFileIds": True,
        "enableSecureMonitoring": True,
        "tags": orig_dataset_info['tags'],
        "properties": orig_dataset_info['properties'],
    }
    # Check if new dataset already created. If not then create it.
    logging.info(
        f"Searching for and creating new dataset {new_dataset_name} in billing profile {billing_profile} if needed"
    )
    dest_dataset_id = tdr.get_or_create_dataset(
        dataset_name=new_dataset_name,
        billing_profile=billing_profile,
        schema=orig_dataset_info['schema'],
        description='description',
        cloud_platform=GCP,
        additional_properties_dict=additional_properties
    )
    # Assumes if dataset exists then it is with same schema
    dest_dataset_info = tdr.get_dataset_info(dest_dataset_id)

    # Add ingest service account for new dataset to original dataset
    logging.info(
        f"Adding ingest service account for new dataset {new_dataset_name} to original "
        f"dataset {orig_dataset_info['name']}"
    )
    tdr.add_user_to_dataset(
        dataset_id=orig_dataset_id,
        user=dest_dataset_info['ingestServiceAccount'],
        policy='custodian'
    )

    # Go through each table in source dataset and run batch ingest to dest dataset
    orig_dataset_tables = [t['name'] for t in orig_dataset_info['schema']['tables']]
    logging.info(f"Found {len(orig_dataset_tables)} tables in source dataset to ingest")
    for table_name in orig_dataset_tables:
        table_metadata = tdr.get_data_set_table_metrics(orig_dataset_id, table_name)
        logging.info(f"Starting ingest for table {table_name} with total of {len(table_metadata)} rows")
        BatchIngest(
            ingest_metadata=table_metadata,
            tdr=tdr,
            target_table_name=table_name,
            dataset_id=dest_dataset_id,
            batch_size=ingest_batch_size,
            bulk_mode=bulk_mode,
            cloud_type=GCP,
            update_strategy=update_strategy,
            waiting_time_to_poll=time_to_poll,
            test_ingest=False,
            load_tag=f"{orig_dataset_info['name']}-{new_dataset_name}",
            file_list_bool=False
        ).run()
