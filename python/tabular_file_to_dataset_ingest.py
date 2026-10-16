import argparse
import logging
import os
import re
import tempfile
from typing import Any, Optional

from ops_utils.csv_util import Csv
from ops_utils.gcp_utils import GCPCloudFunctions
from ops_utils.vars import ARG_DEFAULTS
from ops_utils import comma_separated_list
from ops_utils.tdr_utils.tdr_api_utils import TDR, FilterOutSampleIdsAlreadyInDataset
from ops_utils.tdr_utils.tdr_ingest_utils import BatchIngest
from ops_utils.tdr_utils.tdr_table_utils import SetUpTDRTables
from ops_utils.token_util import Token
from ops_utils.request_util import RunRequest
from ops_utils.terra_util import TerraWorkspace


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

TEST_INGEST = False  # Whether to test the ingest by just doing first batch
INT_PATTERN = re.compile(r"^-?(0|[1-9][0-9]*)$")
FLOAT_PATTERN = re.compile(r"^-?(0|[1-9][0-9]*)?\.[0-9]+([eE][-+]?[0-9]+)?$|^-?[0-9]+[eE][-+]?[0-9]+$")


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest data from a tabular file (csv/tsv) in GCS into a table in an existing TDR dataset")
    parser.add_argument(
        "--input_file",
        required=True,
        help="GCS path (gs://...) of the tsv or csv file to ingest. First column must be the primary key"
    )
    parser.add_argument("--dataset_id", required=True)
    parser.add_argument(
        "--target_table_name",
        required=True,
        help="Name of the table in the TDR dataset that the file's data will be ingested into"
    )
    parser.add_argument(
        "--primary_key",
        required=False,
        help="Optional: name of the column to use as the TDR primary key. Defaults to the first column in the file"
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
        type=int,
        help=f"""The number of rows to ingest at a time. Defaults to {ARG_DEFAULTS['batch_size']} if not provided"""
    )
    parser.add_argument(
        "--check_existing_ingested_files",
        action="store_true",
        help="Whether to check if the files have already been ingested into TDR. This will only work if the 'path' " +
        "used for ingest previously was original gs path with 'gs://' removed. Using option will download " +
        "all files for dataset for every single table ingest. It does drastically speed up ingest if files " +
        "have already been ingested."
    )
    parser.add_argument(
        "--all_fields_non_required",
        action="store_true",
        help="If used, all columns in the table will be set as non-required besides the primary key"
    )
    parser.add_argument(
        "--force_disparate_rows_to_string",
        action="store_true",
        help="If used, all rows in a column containing disparate data types will be forced to a string"
    )
    parser.add_argument(
        "--trunc_and_reload",
        action="store_true",
        help="If used, will soft-delete all rows in the target TDR table before ingesting",
    )
    parser.add_argument(
        "--ignore_existing_schema_mismatch",
        action="store_true",
        help="If used, will ignore schema mismatch between the file and existing TDR table and attempt to ingest "
             "anyway and force the file data to match TDR schema",
    )
    parser.add_argument(
        "--service_account_json",
        "-saj",
        type=str,
        help="Path to the service account JSON file. If not provided, will use the default credentials."
    )

    return parser.parse_args()


def convert_value(value: Optional[str]) -> Any:
    """Convert a string from a tabular file into a typed value (None, int, float, bool or str).

    Tabular files have no types so everything is read as a string. Conversion is deliberately conservative
    (e.g. '007' stays a string) so that identifiers are not mangled.
    """
    if value is None:
        return None
    stripped = value.strip()
    if stripped == "":
        return None
    if INT_PATTERN.match(stripped):
        return int(stripped)
    if FLOAT_PATTERN.match(stripped):
        return float(stripped)
    if stripped.lower() in ("true", "false"):
        return stripped.lower() == "true"
    return value


def read_tabular_file(input_file: str, delimiter: str, primary_key: Optional[str]) -> tuple[list[dict], str]:
    """Download the file from GCS and return its rows (as typed dicts) along with the primary key column."""
    gcp = GCPCloudFunctions()
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = os.path.join(tmp_dir, "input_table")
        logging.info(f"Downloading {input_file}")
        gcp.load_blob_from_full_path(input_file).download_to_filename(local_path)
        csv_util = Csv(file_path=local_path, delimiter=delimiter)
        headers = list(csv_util.get_header_order_from_tsv() or [])
        rows = csv_util.create_list_of_dicts_from_tsv()

    if not headers:
        raise ValueError(f"No header row found in {input_file}")
    # Reuse the same header rules as Terra tables, since TDR has the same column naming restrictions
    TerraWorkspace.validate_terra_headers_for_tdr_conversion(table_name=input_file, headers=headers)

    if primary_key:
        if primary_key not in headers:
            raise ValueError(f"Primary key '{primary_key}' is not a column in {input_file}. Columns: {headers}")
        primary_key_column = primary_key
    else:
        primary_key_column = headers[0]
        logging.info(f"No primary key provided. Using first column '{primary_key_column}'")

    typed_rows = [{k: convert_value(v) for k, v in row.items()} for row in rows]

    missing_keys = [i + 2 for i, row in enumerate(typed_rows) if row[primary_key_column] is None]
    if missing_keys:
        raise ValueError(
            f"Primary key column '{primary_key_column}' is empty in {len(missing_keys)} row(s) "
            f"(file line numbers, first 10: {missing_keys[:10]})"
        )
    all_keys = [row[primary_key_column] for row in typed_rows]
    if len(set(all_keys)) != len(all_keys):
        raise ValueError(f"Primary key column '{primary_key_column}' contains duplicate values")
    return typed_rows, primary_key_column


if __name__ == "__main__":
    args = get_args()
    input_file = args.input_file
    dataset_id = args.dataset_id
    target_table_name = args.target_table_name
    update_strategy = args.update_strategy
    records_to_ingest = args.records_to_ingest
    bulk_mode = args.bulk_mode
    max_retries = args.max_retries
    max_backoff_time = args.max_backoff_time
    filter_existing_ids = args.filter_existing_ids
    batch_size = args.batch_size
    check_if_files_already_ingested = args.check_existing_ingested_files
    all_fields_non_required = args.all_fields_non_required
    force_disparate_rows_to_string = args.force_disparate_rows_to_string
    trunc_and_reload = args.trunc_and_reload
    ignore_existing_schema_mismatch = args.ignore_existing_schema_mismatch
    service_account_json = args.service_account_json

    if not input_file.startswith("gs://"):
        raise ValueError(f"input_file must be a GCS path starting with 'gs://'. Got: {input_file}")

    # Infer the delimiter from the file extension: '.csv' is comma-delimited and '.tsv' is tab-delimited
    if input_file.lower().endswith(".csv"):
        delimiter = ","
    elif input_file.lower().endswith(".tsv"):
        delimiter = "\t"
    else:
        raise ValueError(
            f"input_file must end with '.csv' or '.tsv' so the delimiter can be determined. Got: {input_file}"
        )

    # Initialize the TDR classes
    token = Token(service_account_json=service_account_json)
    request_util = RunRequest(token=token, max_retries=max_retries, max_backoff_time=max_backoff_time)
    tdr = TDR(request_util=request_util)

    dataset_info = tdr.get_dataset_info(dataset_id=dataset_id).json()

    # Check if dataset is selfHosted. If it isn't then getting UUIDs for files will not work
    if not dataset_info["selfHosted"] and check_if_files_already_ingested:
        logging.warning("Dataset is not selfHosted. Cannot check if files have already been ingested and use UUIDs")
        check_if_files_already_ingested = False

    # Read the file and validate it before touching the dataset
    updated_metrics, primary_key_column_name = read_tabular_file(
        input_file=input_file, delimiter=delimiter, primary_key=args.primary_key
    )
    logging.info(f"Got {len(updated_metrics)} rows")

    # Use only specific records if provided
    if records_to_ingest:
        updated_metrics = [
            metric for metric in updated_metrics if str(metric[primary_key_column_name]) in records_to_ingest
        ]

    table_info_dict = {
        target_table_name: {
            "table_name": target_table_name,
            "primary_key": primary_key_column_name,
            "ingest_metadata": updated_metrics,
            "datePartitionOptions": None
        }
    }
    SetUpTDRTables(
        tdr=tdr,
        dataset_id=dataset_id,
        table_info_dict=table_info_dict,
        all_fields_non_required=all_fields_non_required,
        force_disparate_rows_to_string=force_disparate_rows_to_string,
        ignore_existing_schema_mismatch=ignore_existing_schema_mismatch
    ).run()

    if trunc_and_reload:
        logging.info(
            f"Requested trunc and reload - all rows in table {target_table_name} in the target TDR dataset will be "
            "soft-deleted if the table contains any data"
        )
        tdr.soft_delete_all_table_entries(dataset_id=dataset_id, table_name=target_table_name)

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

    if check_if_files_already_ingested:
        # Download and create a dictionary of file paths to UUIDs for ingest
        file_uuids_dict = tdr.create_file_uuid_dict_for_ingest_for_experimental_self_hosted_dataset(
            dataset_id=dataset_id)
    else:
        file_uuids_dict = None

    # Only use this to make the file data match what TDR schema is set to if ignore_schema_mismatch is used
    if ignore_existing_schema_mismatch:
        tdr_table_schema_info = tdr.get_table_schema_info(dataset_id=dataset_id, table_name=target_table_name)
    else:
        tdr_table_schema_info = None

    BatchIngest(
        ingest_metadata=filtered_metrics,
        tdr=tdr,
        target_table_name=target_table_name,
        dataset_id=dataset_id,
        batch_size=batch_size,
        bulk_mode=bulk_mode,
        update_strategy=update_strategy,
        waiting_time_to_poll=ARG_DEFAULTS['waiting_time_to_poll'],
        test_ingest=TEST_INGEST,
        load_tag=f"{target_table_name}-{dataset_id}",
        file_to_uuid_dict=file_uuids_dict,
        schema_info=tdr_table_schema_info
    ).run()
