"""
To run locally do python3 /path/to/script.py -a required_arg -t -b optional_arg -c choice1.
You may need to install the required packages with pip install -r requirements.txt. If attempting to run with Terra
will need to set up wdl pointing towards script.
"""
import os
import logging
import pandas as pd
from argparse import ArgumentParser, Namespace

from ops_utils.terra_util import TerraWorkspace
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils.gcp_utils import GCPCloudFunctions
from ops_utils.vars import ARG_DEFAULTS
from copy_gcp_to_gcp import CreateCopyDict


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

def read_list_from_file(file_path: str) -> list[str]:
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]

def get_args() -> Namespace:
    parser = ArgumentParser(description="Copy paired files into Terra workspace bucket and update data table.")
    parser.add_argument("--billing_project", required=True, help="Terra billing project.")
    parser.add_argument("--workspace_name", required=True, help="Terra workspace name.")
    parser.add_argument("--first_file_list", required=True, help="Path to text file of GCS URIs for first file set.")
    parser.add_argument("--second_file_list", required=True, help="Path to text file of GCS URIs for second file set.")
    parser.add_argument("--entity_ids", required=True, help="Path to text file of entity IDs.")
    parser.add_argument("--subdir", required=True, help="Subdirectory under workspace bucket (e.g. cram_crai).")
    parser.add_argument("--first_column_name", required=True, help="Column name for first file in Terra table.")
    parser.add_argument("--second_column_name", required=True, help="Column name for second file in Terra table.")
    parser.add_argument("--table_name", default="sample", help="Terra data table name (default: sample).")
    parser.add_argument("--upload_tsv", action="store_true", help="If set, upload the TSV to the Terra workspace.")

    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()

    # Get arguments
    file_list_1 = read_list_from_file(args.first_file_list)
    file_list_2 = read_list_from_file(args.second_file_list)
    entity_ids = read_list_from_file(args.entity_ids)

    if not (len(file_list_1) == len(file_list_2) == len(entity_ids)):
        raise ValueError("Length of all input lists (first files, second files, entity IDs) must match.")

    # Create token object. This gets your token for the API calls and auto refreshes when needed
    token = Token()
    # Create request object to make API calls and pass in token
    # Can optionally pass in max_retries and max_backoff_time to control retries and backoff time.
    # Defaults to 5 retries and 5 minutes max backoff if not supplied
    request_util = RunRequest(token=token, max_retries=5, max_backoff_time=60)
    # Create TDR and Terra objects to interact with the TDR and Terra with the request_util object
    terra = TerraWorkspace(
        request_util=request_util,
        billing_project=args.billing_project,
        workspace_name=args.workspace_name
    )

    bucket = f"gs://{terra.get_workspace_bucket()}"
    dest_prefix = os.path.join(bucket, args.subdir.strip("/"))

    logging.info(f"Workspace bucket: {bucket}")
    logging.info(f"Copying files to: {dest_prefix}")

    copy_dict_1 = CreateCopyDict(
        source_files=file_list_1,
        destination_path=dest_prefix,
        preserve_structure=False
    ).run()

    copy_dict_2 = CreateCopyDict(
        source_files=file_list_2,
        destination_path=dest_prefix,
        preserve_structure=False
    ).run()

    new_paths_1 = [line["full_destination_path"] for line in copy_dict_1]
    new_paths_2 = [line["full_destination_path"] for line in copy_dict_2]

    gcp = GCPCloudFunctions()

    gcp.multithread_copy_of_files_with_validation(
        files_to_copy=copy_dict_1,
        max_retries=ARG_DEFAULTS["max_retries"],
        workers=ARG_DEFAULTS["multithread_workers"]
    )

    gcp.multithread_copy_of_files_with_validation(
        files_to_copy=copy_dict_2,
        max_retries=ARG_DEFAULTS["max_retries"],
        workers=ARG_DEFAULTS["multithread_workers"]
    )

    entity_col = f"entity:{args.table_name}_id"
    df = pd.DataFrame({
            entity_col: entity_ids,
            args.first_column_name: new_paths_1,
            args.second_column_name: new_paths_2
        })

    tsv_path = f"{args.table_name}_localized_files.tsv"
    df.to_csv(tsv_path, sep="\t", index=False)

    # Optionally upload to Terra workspace data table
    if args.upload_tsv:
        logging.info("Uploading TSV to Terra...")
        terra.upload_metadata_to_workspace_table(tsv_path)
        logging.info("TSV uploaded.")
    else:
        logging.info("Skipping TSV upload (use --upload-tsv to enable).")

    logging.info("Localization script completed.")
