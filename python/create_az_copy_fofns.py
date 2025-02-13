import logging
from argparse import ArgumentParser, Namespace
import os
from typing import List
import pandas as pd
from numpy.distutils.conv_template import header

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(description="split up azure tsv into smaller tsvs with destination path")
    parser.add_argument(
        "--destination_path",
        "-dp",
        help="Path to where files should be copied. Should be in format gs://bucket_name/path/to/folder/ or gs://bucket_name/",
        required=True
    )
    parser.add_argument(
        "--full_az_tsv",
        "-az",
        help="Path to full azure tsv file. Should have headers and columns az_path and dataset_id",
        required=True
    )
    parser.add_argument(
        "--width",
        "-w",
        help="Number of smaller tsvs to split into",
        type=int,
        required=True
    )
    return parser.parse_args()


def split_tsv(input_tsv: str, gcp_destination: str, width: int) -> List[str]:
    # Read the TSV into a DataFrame
    df = pd.read_csv(input_tsv, sep='\t', header='infer')

    # Generate the GCP destination paths
    df['target_url'] = df['az_path'].apply(lambda x: os.path.join(gcp_destination, "/".join(x.split('/')[3:])))

    # Split into smaller dataframes
    split_dfs = [df.iloc[i::width] for i in range(width)]

    output_files = []
    for idx, split_df in enumerate(split_dfs):
        output_filename = f"split_{idx + 1}.tsv"
        logging.info(f"Creating {output_filename}")
        split_df.to_csv(output_filename, sep='\t', index=False, header=['az_path', 'dataset_id', 'target_url'])
        output_files.append(output_filename)

    return output_files


if __name__ == '__main__':
    args = get_args()
    full_az_tsv = args.full_az_tsv
    gcp_destination = args.destination_path
    width = args.width
    output_files = split_tsv(full_az_tsv, gcp_destination, width)
