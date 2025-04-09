from argparse import ArgumentParser, Namespace
from utils.gcp_utils import GCPCloudFunctions
import os
import logging

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


TEMP_LOCAL_FILE = "tmp.md5"


def get_args() -> Namespace:
    parser = ArgumentParser(description="Get a GCP files md5")
    parser.add_argument("--gcp_file_path", "-f", required=True)
    parser.add_argument(
        "--create_cloud_md5_file", "-c", action="store_true",
        help="Create a file with the md5 hash where it is {file_path}.md5 if arg is used")
    parser.add_argument(
        "--output_file", "-o", required=False, default=TEMP_LOCAL_FILE,
        help="Output file for md5 hash locally. If not used then will not write it to a file locally"
    )
    parser.add_argument(
        "--md5_type", "-m", required=False, choices=["hex", "base64"], default="hex",
        help="The type of md5 hash to return. hex = md5sum returns and base63 what gsutil stores. Default is hex"
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()
    gcp_file_path = args.gcp_file_path
    create_cloud_md5_file = args.create_cloud_md5_file
    local_output_file = args.output_file
    md5_type = args.md5_type

    gcp_utils = GCPCloudFunctions()
    file_md5 = gcp_utils.get_object_md5(file_path=gcp_file_path, returned_md5_format=md5_type)

    # If the output file is not the temp file or the create cloud md5 file arg is used
    if local_output_file != TEMP_LOCAL_FILE or create_cloud_md5_file:
        logging.info(f"Writing md5 hash to {local_output_file}")
        with open(local_output_file, "w") as file:
            file.write(file_md5)

        if create_cloud_md5_file:
            logging.info(f"Copying {local_output_file} to {gcp_file_path}.md5")
            gcp_utils.copy_onprem_to_cloud(
                onprem_src_path=local_output_file,
                cloud_dest_path=f"{gcp_file_path}.md5"
            )

        if local_output_file == TEMP_LOCAL_FILE:
            logging.info(f"Removing {TEMP_LOCAL_FILE}")
            os.remove(TEMP_LOCAL_FILE)
