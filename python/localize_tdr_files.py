"""
To run locally do python3 /path/to/script.py -a required_arg -t -b optional_arg -c choice1.
You may need to install the required packages with pip install -r requirements.txt. If attempting to run with Terra
will need to set up wdl pointing towards script.
"""
import os
import logging
import pandas as pd
from argparse import ArgumentParser, Namespace

from ops_utils.terra_util import get_workspace_bucket, upload_metadata_to_workspace_table
from copy_gcp_to_gcp import copy_gcp_to_gcp


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

def read_list_from_file(file_path: str) -> list[str]:
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]

def get_args() -> Namespace:
    parser = argparse.ArgumentParser(
            description="Copy paired files into Terra workspace bucket and update data table."
        )

        parser.add_argument("--billing-project", required=True, help="Terra billing project.")
        parser.add_argument("--workspace-name", required=True, help="Terra workspace name.")
        parser.add_argument("--first-file-list", required=True, help="Path to text file of GCS URIs for first file set.")
        parser.add_argument("--second-file-list", required=True, help="Path to text file of GCS URIs for second file set.")
        parser.add_argument("--entity-ids", required=True, help="Path to text file of entity IDs.")
        parser.add_argument("--subdir", required=True, help="Subdirectory under workspace bucket (e.g. cram_crai).")
        parser.add_argument("--first-column-name", required=True, help="Column name for first file in Terra table.")
        parser.add_argument("--second-column-name", required=True, help="Column name for second file in Terra table.")
        parser.add_argument("--table-name", default="sample", help="Terra data table name (default: sample).")

        return parser.parse_args()


if __name__ == '__main__':
    args = get_args()

    # Get arguments
    file_list_1 = read_list_from_file(args.first_file_list)
    file_list_2 = read_list_from_file(args.second_file_list)
    entity_ids = read_list_from_file(args.entity_ids)

    if not (len(file_list_1) == len(file_list_2) == len(entity_ids)):
        raise ValueError("Length of all input lists (first files, second files, entity IDs) must match.")


    bucket = get_workspace_bucket(args.billing_project, args.workspace_name)
    dest_prefix = os.path.join(bucket, args.subdir.strip("/"))

    print(f"Workspace bucket: {bucket}")
    print(f"Copying files to: {dest_prefix}")

    new_paths_1 = []
    new_paths_2 = []

    for src1, src2 in zip(file_list_1, file_list_2):
        dest1 = f"{dest_prefix}/{os.path.basename(src1)}"
        dest2 = f"{dest_prefix}/{os.path.basename(src2)}"

        copy_gcp_to_gcp(src1, dest1)
        copy_gcp_to_gcp(src2, dest2)

        new_paths_1.append(dest1)
        new_paths_2.append(dest2)

    entity_col = f"entity:{args.table_name}_id"
    df = pd.DataFrame({
            entity_col: entity_ids,
            args.first_column_name: new_paths_1,
            args.second_column_name: new_paths_2
        })

    tsv_path = f"{args.table_name}_localized.tsv"
    df.to_csv(tsv_path, sep="\t", index=False)

    print(f"Uploading TSV to Terra: {tsv_path}")
    upload_entities_tsv(args.billing_project, args.workspace_name, tsv_path)

    print("✅ Localization and table update complete.")

