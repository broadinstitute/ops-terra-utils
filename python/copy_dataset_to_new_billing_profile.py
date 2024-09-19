"""Take in billing profile and dataset and recreate the dataset in a new billing profile."""
import json
import logging
import sys
import os
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
    parser = ArgumentParser(
        description="""Copy dataset to new billing profile""")
    parser.add_argument("--new_billing_profile", required=True)
    parser.add_argument("--orig_dataset_id", required=True)
    parser.add_argument(
        "--ingest_batch_size",
        help=f"Batch size for ingest. Default to {DEFAULT_BATCH_SIZE}",
        default=DEFAULT_BATCH_SIZE, type=int
    )
    parser.add_argument("--update_strategy", choices=["REPLACE", "APPEND", "UPDATE"], default="REPLACE")
    parser.add_argument(
        "--new_dataset_name", required=True,
        help="Cannot be named the same as original dataset"
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


def create_additional_properties(orig_dataset_info: dict) -> dict:
    additional_properties = {
        "experimentalSelfHosted": False,
        "dedicatedIngestServiceAccount": True,
        "experimentalPredictableFileIds": True,
        "enableSecureMonitoring": True
    }
    if orig_dataset_info['phsId']:
        additional_properties['phsId'] = orig_dataset_info['phsId']
    if orig_dataset_info['tags']:
        additional_properties['tags'] = orig_dataset_info['tags']
    if orig_dataset_info['properties']:
        additional_properties['properties'] = orig_dataset_info['properties']
    return additional_properties


class CreateIngestRecords:
    def __init__(self, tdr: TDR, orig_dataset_id: str, table_schema_info: dict, orig_dataset_file_info: dict):
        self.tdr = tdr
        self.orig_dataset_id = orig_dataset_id
        self.table_schema_info = table_schema_info
        self.orig_dataset_file_info = orig_dataset_file_info

    @staticmethod
    def _create_new_file_ref(file_details: dict) -> dict:
        # Get md5 from file details
        md5_checksum = next(
            (
                item['checksum']
                for item in file_details['checksums']
                if item['type'] == 'md5'
            ), None
        )

        return {
            # source path is the full gs path to original file in TDR
            "sourcePath": file_details['fileDetail']['accessUrl'],
            # Keep the same target path
            "targetPath": f"/new/{os.path.basename(file_details['fileDetail']['accessUrl'])}",
            "md5": md5_checksum
        }

    def run(self) -> list[dict]:
        # Get all file ref columns in table
        file_ref_columns = [
            col['name'] for col in self.table_schema_info['columns'] if col['datatype'] == 'fileref']
        table_metadata = tdr.get_data_set_table_metrics(orig_dataset_id, table_dict['name'])
        new_ingest_records = []
        # Go through each row in table
        for row_dict in table_metadata:
            new_row_dict = {}
            # Go through each column in row
            for column in row_dict:
                # Don't include empty columns
                if row_dict[column]:
                    # Check if column is a file ref column
                    if column in file_ref_columns:
                        file_uuid = row_dict[column]
                        # Check if file_uuid is in original dataset
                        if file_uuid:
                            # Create updated dict with new file ref
                            new_row_dict[column] = self._create_new_file_ref(
                                self.orig_dataset_file_info[file_uuid]
                            )
                    else:
                        # Add column to new row dict
                        new_row_dict[column] = row_dict[column]
            # Add new row dict to list of new ingest records
            new_ingest_records.append(new_row_dict)
        return new_ingest_records


if __name__ == "__main__":
    args = get_args()

    orig_dataset_id, update_strategy, time_to_poll = (
        args.orig_dataset_id, args.update_strategy, args.waiting_time_to_poll
    )
    bulk_mode, ingest_batch_size, billing_profile = args.bulk_mode, args.ingest_batch_size, args.new_billing_profile
    new_dataset_name = args.new_dataset_name
    # Initialize the Terra and TDR classes
    token = Token(cloud=GCP)
    request_util = RunRequest(token=token)
    tdr = TDR(request_util=request_util)

    orig_dataset_info = tdr.get_dataset_info(orig_dataset_id)

    # Check dataset id is not already in requested billing profile
    if orig_dataset_info['defaultProfileId'] == billing_profile:
        logging.info(
            f"Dataset {orig_dataset_id} already in billing profile {billing_profile}")
        sys.exit(0)

    additional_properties = create_additional_properties(orig_dataset_info)
    # Check if new dataset already created. If not then create it.
    logging.info(
        f"Searching for and creating new dataset {new_dataset_name} in billing profile {billing_profile} if needed"
    )
    dest_dataset_id = tdr.get_or_create_dataset(
        dataset_name=new_dataset_name,
        billing_profile=billing_profile,
        schema=orig_dataset_info['schema'],
        description=orig_dataset_info['description'],
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
    orig_dataset_tables = [
        table for table in orig_dataset_info['schema']['tables']
    ]
    logging.info(
        f"Found {len(orig_dataset_tables)} tables in source dataset to ingest")

    # Get dict of all files in original dataset
    original_files_info = tdr.create_file_dict(dataset_id=orig_dataset_id, limit=1000)

    for table_dict in orig_dataset_tables:
        # Update UUIDs from dataset metrics to be paths to files
        ingest_records = CreateIngestRecords(
            tdr=tdr,
            orig_dataset_id=orig_dataset_id,
            table_schema_info=table_dict,
            orig_dataset_file_info=original_files_info
        ).run()
        table_name = table_dict['name']
        logging.info(
            f"Starting ingest for table {table_name} with total of {len(ingest_records)} rows")
        BatchIngest(
            ingest_metadata=ingest_records,
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
            file_list_bool=False,
            skip_reformat=True
        ).run()
