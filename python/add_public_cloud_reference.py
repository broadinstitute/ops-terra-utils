import argparse
import os
import logging
from urllib.parse import urlparse
from pathlib import Path

from utils.gcp_utils import GCPCloudFunctions


class CopyPublicCloudReference:
    BROAD_PUBLIC_REFERENCES_SYNC_BUCKET = "gs://broad-references/"

    def __init__(
            self,
            reference_name: str,
            chrom_sizes_file_location: str,
            annotation_file_location: str,
            star_tar_file_location: str,
            bwa_mem_tar_file_location: str,
            output_cloud_path: str,
            read_me_path: str
    ):
        self.reference_name = reference_name
        self.chrom_sizes_file_location = chrom_sizes_file_location
        self.annotation_file_location = annotation_file_location
        self.star_tar_file_location = star_tar_file_location
        self.bwa_mem_tar_file_location = bwa_mem_tar_file_location
        self.output_cloud_path = output_cloud_path
        self.read_me_path = read_me_path

        self.output_bucket_root_dir = self._get_output_bucket_location()
        self.gcp = GCPCloudFunctions()

    def _get_output_bucket_location(self) -> str:
        parsed_url = urlparse(self.output_cloud_path)
        output_subdir = parsed_url.path.lstrip("/")
        return os.path.join(self.BROAD_PUBLIC_REFERENCES_SYNC_BUCKET, output_subdir)

    def _get_file_output_path(self, source_file: str) -> str:
        file_name = Path(source_file).name
        return os.path.join(self.output_bucket_root_dir, file_name)

    def _get_readme_file_output_path(self) -> str:
        return os.path.join(self.output_bucket_root_dir, "README.txt")

    def copy_files_to_public_bucket(self) -> None:
        gcs_files_to_copy = [
            self.chrom_sizes_file_location,
            self.annotation_file_location,
            self.star_tar_file_location,
            self.bwa_mem_tar_file_location,
        ]

        try:
            # Copy all reference files to destination bucket
            logging.info("Copying references files from source location to Broad bucket")
            for source_file in gcs_files_to_copy:
                destination_path = self._get_file_output_path(source_file=source_file)
                self.gcp.copy_cloud_file(src_cloud_path=source_file, full_destination_path=destination_path)
                logging.info(f"Successfully copied '{source_file}' to '{destination_path}'\n")

            # Copy README
            logging.info("Copying README to public cloud reference bucket")
            read_me_destination = self._get_readme_file_output_path()
            self.gcp.copy_cloud_file(src_cloud_path=self.read_me_path, full_destination_path=read_me_destination)
            logging.info(f"Successfully copied README file to '{read_me_destination}'")

        except Exception as e:
            logging.error(f"Encountered an error while attempting to copy source files to destination file paths: {e}")


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Add a new public cloud reference")
    parser.add_argument("--reference_name", required=True, type=str, help="The name of the reference to add")
    parser.add_argument(
        "--chrom_sizes_file_location",
        required=True,
        type=str,
        help="The current cloud location for the chromosome sizes file"
    )
    parser.add_argument(
        "--annotation_file_location", required=True, type=str,
        help="The current cloud location for the annotations file"
    )
    parser.add_argument(
        "--star_tar_file_location", required=True, type=str,
        help="The current cloud location for the star tarball file"
    )
    parser.add_argument(
        "--bwa_mem_tar_file_location", required=True, type=str,
        help="The current cloud location for the bwa-mem tarball file"
    )
    parser.add_argument("--new_location", required=True, type=str, help="The new cloud location")
    parser.add_argument("--attachment", required=True, type=str, help="The path to the README file")

    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()
    CopyPublicCloudReference(
        reference_name=args.reference_name,
        chrom_sizes_file_location=args.chrom_sizes_file_location,
        annotation_file_location=args.annotation_file_location,
        star_tar_file_location=args.star_tar_file_location,
        bwa_mem_tar_file_location=args.bwa_mem_tar_file_location,
        output_cloud_path=args.new_location,
        read_me_path=args.attachment
    ).copy_files_to_public_bucket()
