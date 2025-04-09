import logging
from argparse import ArgumentParser, Namespace
from ops_utils.gcp_utils import GCPCloudFunctions
import pandas as pd

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(description="split up azure tsv into smaller tsvs with destination path")
    parser.add_argument(
        "--full_az_tsv",
        "-az",
        help="Path to full azure tsv file. Should have headers and columns az_path, dataset_id, target_url, and bytes",
        required=True
    )
    parser.add_argument(
        "--width",
        "-w",
        help="Number of smaller tsvs to split into",
        type=int,
        required=True
    )
    parser.add_argument(
        "--max_gb_per_file",
        "-m",
        type=int,
        help="Maximum size in GB for any file",
    )
    parser.add_argument(
        "--skip_too_large_files",
        "-s",
        action='store_true',
        help="Skip files that are too big. If not used will fail whole transfer"
    )
    parser.add_argument(
        "--check_already_copied",
        "-c",
        action='store_true',
        help="Check if file already copied. WIll add extra compute time as it will check each file"
    )
    return parser.parse_args()


class SplitTsv:
    def __init__(
            self,
            input_tsv: str,
            width: int,
            max_gb_per_file: int,
            skip_too_big_files: bool,
            check_already_copied: bool
    ):
        self.input_tsv = input_tsv
        self.width = width
        self.max_gb_per_file = max_gb_per_file
        self.skip_too_big_files = skip_too_big_files
        self.check_already_copied = check_already_copied
        if check_already_copied:
            # Only import if needed
            self.gcp_utils = GCPCloudFunctions()

    def _remove_large_files(self, split_df: pd.DataFrame, index: int) -> pd.DataFrame:
        too_large = split_df["bytes"] > (self.max_gb_per_file * 1024 ** 3)

        if too_large.any():
            over_sized_files = split_df[too_large]
            if self.skip_too_big_files:
                logging.warning(
                    f"Skipping {len(over_sized_files)} entries in split_{index + 1} as "
                    f"they exceed {self.max_gb_per_file} GB.")
                split_df = split_df[~too_large]
        return split_df

    def _already_copied(self, target_url: str, bytes: int) -> bool:
        logging.info(f"Checking if {target_url} has already been copied.")
        if self.gcp_utils.check_file_exists(target_url):
            if self.gcp_utils.get_filesize(target_url) == int(bytes):
                return True
        return False

    def split_tsv(self) -> None:
        df = pd.read_csv(self.input_tsv, sep='\t', header='infer')

        # Check if any row has missing values
        if df.isnull().any().any() or (df == "").any().any():
            raise ValueError("Input TSV contains empty values. Please ensure all columns are filled.")

        # Convert 'bytes' column to numeric
        df["bytes"] = pd.to_numeric(df["bytes"], errors="coerce")

        if self.check_already_copied:
            # Count original number of rows
            original_count = len(df)
            # Remove rows that have already been copied
            df = df[
                ~df.apply(
                    lambda row: self._already_copied(row["target_url"], row["bytes"]), axis=1
                )
            ]
            # Count how many files were removed
            removed_count = original_count - len(df)
            logging.info(f"Removed {removed_count} files that were already copied.")
            if len(df) == 0:
                logging.info("All files have already been copied. Exiting.")
                return

        output_files = []

        # Split into smaller dataframes
        split_dfs = [df.iloc[i::self.width] for i in range(self.width)]

        for idx, split_df in enumerate(split_dfs):
            if split_df.empty:  # Skip empty splits
                continue

            # Remove rows that exceed max_gb_per_file or fail if not allowed
            split_df = self._remove_large_files(split_df, idx)

            # Save split TSV
            if not split_df.empty:  # Ensure we don't create empty files
                output_filename = f"split_{idx + 1}.tsv"
                logging.info(f"Creating {output_filename}")
                split_df.to_csv(output_filename, sep='\t', index=False,
                                header=['az_path', 'dataset_id', 'target_url', 'bytes'])
                output_files.append(output_filename)
                # Save disk size file
                max_bytes = split_df["bytes"].max()
                max_gb = max_bytes / (1024 ** 3)  # Convert bytes to GB
                disk_size_file = f"disk_size_split_{idx + 1}.txt"
                logging.info(f"Creating {disk_size_file}")
                with open(disk_size_file, 'w') as f:
                    # Write max GB to file without decimal
                    f.write(f"{max_gb:.0f}")


if __name__ == '__main__':
    args = get_args()
    full_az_tsv = args.full_az_tsv
    width = args.width
    max_gb_per_file = args.max_gb_per_file
    skip_too_big_files = args.skip_too_large_files
    check_already_copied = args.check_already_copied
    SplitTsv(
        input_tsv=full_az_tsv,
        width=width,
        max_gb_per_file=max_gb_per_file,
        skip_too_big_files=skip_too_big_files,
        check_already_copied=check_already_copied
    ).split_tsv()
