import argparse
import os
import logging
from google.cloud import storage
from urllib.parse import urlparse
from pathlib import Path


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
        # The default SA credentials are set in the cloud run job under "Security" -> "Service Account": https://console.cloud.google.com/run/jobs/edit/us-central1/add-public-cloud-reference?project=operations-portal-427515&authuser=0
        self.client = storage.Client()

        self.output_bucket_root_dir = self._get_output_bucket_location()

    def _get_output_bucket_location(self) -> str:
        parsed_url = urlparse(self.output_cloud_path)
        output_subdir = parsed_url.path.lstrip("/")
        return os.path.join(self.BROAD_PUBLIC_REFERENCES_SYNC_BUCKET, output_subdir)

    def _get_file_output_path(self, source_file: str) -> str:
        file_name = Path(source_file).name
        return os.path.join(self.output_bucket_root_dir, file_name)

    def _get_readme_file_output_path(self) -> str:
        return os.path.join(self.output_bucket_root_dir, "README.txt")

    def _copy_source_file_to_destination(self, source_path: str, destination_path: str) -> None:
        src_parsed = urlparse(source_path)
        source_bucket_name = src_parsed.netloc
        source_blob_name = src_parsed.path.lstrip("/")

        dest_parsed = urlparse(destination_path)
        destination_bucket_name = dest_parsed.netloc
        destination_blob_name = dest_parsed.path.lstrip("/")

        source_bucket = self.client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_blob_name)

        destination_bucket = self.client.bucket(destination_bucket_name)

        source_bucket.copy_blob(
            source_blob,
            destination_bucket,
            destination_blob_name
        )

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
                self._copy_source_file_to_destination(source_path=source_file, destination_path=destination_path)
                logging.info(f"Successfully copied '{source_file}' to '{destination_path}'\n")

            # Copy README
            logging.info("Copying README to public cloud reference bucket")
            read_me_destination = self._get_readme_file_output_path()
            self._copy_source_file_to_destination(source_path=self.read_me_path, destination_path=read_me_destination)
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
