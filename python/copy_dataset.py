"""Take in billing profile and dataset and recreate the dataset in a new billing profile."""
import logging
import sys
from argparse import ArgumentParser, Namespace

from utils.tdr_utils.tdr_api_utils import TDR
from utils.tdr_utils.tdr_ingest_utils import FilterAndBatchIngest
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
        description="""This script will copy a dataset to a new billing profile""")
    parser.add_argument("--new_billing_profile", "-nb", required=True)
    parser.add_argument("--orig_dataset_id", "-od", required=True)
    parser.add_argument("--filter_out_existing_ids", action="store_true",
                        help="If used, will filter out existing ids in the dest dataset")
    parser.add_argument(
        "--ingest_batch_size",
        help=f"Batch size for ingest. Default to {DEFAULT_BATCH_SIZE}",
        default=DEFAULT_BATCH_SIZE, type=int
    )
    parser.add_argument("--update_strategy", choices=["REPLACE", "APPEND", "UPDATE"], default="REPLACE")
    parser.add_argument(
        "--new_dataset_name", "-nd", required=True,
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
    """
    Create additional properties for the new dataset based on the original dataset information.

    Args:
        orig_dataset_info (dict): The original dataset information.

    Returns:
        dict: A dictionary containing additional properties for the new dataset.
    """
    additional_properties = {
        "experimentalSelfHosted": False,
        "dedicatedIngestServiceAccount": True,
        "experimentalPredictableFileIds": False,
        "enableSecureMonitoring": True
    }
    if orig_dataset_info.get('phsId'):
        additional_properties['phsId'] = orig_dataset_info['phsId']
    if orig_dataset_info.get('tags'):
        additional_properties['tags'] = orig_dataset_info['tags']
    if orig_dataset_info.get('properties'):
        additional_properties['properties'] = orig_dataset_info['properties']
    return additional_properties


class CreateIngestRecords:
    """
    A class to create ingest records for a new dataset based on the original dataset information.
    """

    def __init__(self, tdr: TDR, orig_dataset_id: str, table_schema_info: dict, orig_dataset_file_info: dict):
        """
        Initialize the CreateIngestRecords class.

        Args:
            tdr (TDR): An instance of the TDR class.
            orig_dataset_id (str): The ID of the original dataset.
            table_schema_info (dict): The schema information of the table.
            orig_dataset_file_info (dict): The file information of the original dataset.
        """
        self.tdr = tdr
        self.orig_dataset_id = orig_dataset_id
        self.table_schema_info = table_schema_info
        self.orig_dataset_file_info = orig_dataset_file_info

    @staticmethod
    def _create_new_file_ref(file_details: dict) -> dict:
        """
        Create a new file reference dictionary based on the file details.

        Args:
            file_details (dict): The details of the file.

        Returns:
            dict: A dictionary containing the new file reference.
        """
        file_ref_dict = {
            # source path is the full gs path to original file in TDR
            "sourcePath": file_details['fileDetail']['accessUrl'],
            # Keep the same target path
            "targetPath": file_details['path'],
        }
        # Get md5 from file details
        md5_checksum = next(
            (
                item['checksum']
                for item in file_details['checksums']
                if item['type'] == 'md5'
            ), None
        )
        if md5_checksum:
            file_ref_dict['md5'] = md5_checksum
        return file_ref_dict

    def run(self) -> list[dict]:
        """
        Run the process to create new ingest records for the new dataset.

        Returns:
            list[dict]: A list of dictionaries containing the new ingest records.
        """
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


class MatchSchemas:
    def __init__(self, orig_dataset_info: dict, dest_dataset_info: dict, dest_dataset_id: str, tdr: TDR):
        """
        Initialize the MatchSchemas class.

        Args:
            orig_dataset_info (dict): The original dataset information.
            dest_dataset_info (dict): The destination dataset information.
            dest_dataset_id (str): The ID of the destination dataset.
            tdr (TDR): An instance of the TDR class.
        """
        self.orig_dataset_info = orig_dataset_info
        self.dest_dataset_info = dest_dataset_info
        self.dest_dataset_id = dest_dataset_id
        self.tdr = tdr

    def run(self) -> None:
        """
        Run the process to match tables between the original and destination datasets and add missing tables.

        Returns:
            None
        """
        tables_to_update = []
        dest_tables = [table['name'] for table in self.dest_dataset_info["schema"]["tables"]]
        # If table exists already assumes it is the same schema
        for table in self.orig_dataset_info["schema"]["tables"]:
            if table['name'] not in dest_tables:
                logging.info(
                    f"Table {table['name']} not found in new dataset {new_dataset_name}. will add table"
                )
                tables_to_update.append(table)
        if tables_to_update:
            logging.info(
                f"Adding {len(tables_to_update)} tables to new dataset {new_dataset_name}"
            )
            self.tdr.update_dataset_schema(
                dataset_id=self.dest_dataset_id,
                tables_to_add=tables_to_update,
                update_note=f"Adding tables to dataset {new_dataset_name}"
            )


if __name__ == "__main__":
    args = get_args()

    orig_dataset_id, update_strategy, time_to_poll = args.orig_dataset_id, args.update_strategy, args.waiting_time_to_poll
    filter_out_existing_ids = args.filter_out_existing_ids
    bulk_mode, ingest_batch_size, billing_profile = args.bulk_mode, args.ingest_batch_size, args.new_billing_profile
    new_dataset_name = args.new_dataset_name
    # Initialize the Terra and TDR classes
    token = Token(cloud=GCP)
    request_util = RunRequest(token=token)
    tdr = TDR(request_util=request_util)

    orig_dataset_info = tdr.get_dataset_info(orig_dataset_id)

    # Check dataset id is not already in requested billing profile
    if orig_dataset_info['name'] == new_dataset_name:
        logging.info(
            f"Dataset {orig_dataset_id} is already named {new_dataset_name}. Exiting")
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
    dest_dataset_info = tdr.get_dataset_info(dest_dataset_id)

    # Check if schema matches and update if needed. Only will be possibly updated if dataset already exists
    MatchSchemas(
        orig_dataset_info=orig_dataset_info,
        dest_dataset_info=dest_dataset_info,
        dest_dataset_id=dest_dataset_id,
        tdr=tdr
    ).run()

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
    original_files_info = tdr.create_file_dict(dataset_id=orig_dataset_id, limit=20000)

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
        FilterAndBatchIngest(
            tdr=tdr,
            filter_existing_ids=filter_out_existing_ids,
            unique_id_field=table_dict['primaryKey'][0],  # Assumes only one primary key
            table_name=table_name,
            ingest_metadata=ingest_records,
            dataset_id=dest_dataset_id,
            file_list_bool=False,
            ingest_waiting_time_poll=time_to_poll,
            ingest_batch_size=ingest_batch_size,
            bulk_mode=False,
            cloud_type=GCP,
            update_strategy=update_strategy,
            load_tag=f"{orig_dataset_info['name']}-{new_dataset_name}",
            skip_reformat=True
        ).run()