import argparse
import os
import logging
from typing import Optional
from pathlib import Path

from utils.gcp_utils import GCPCloudFunctions


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


class CopyPublicCloudReference:
    BROAD_PUBLIC_REFERENCES_SYNC_BUCKET = "broad-references"

    def __init__(
            self,
            chrom_sizes_file_location: Optional[str],
            chrom_sizes_file_destination: Optional[str],
            annotation_file_location: Optional[str],
            annotations_file_destination: Optional[str],
            star_tar_file_location: Optional[str],
            star_tar_file_destination: Optional[str],
            bwa_mem_tar_file_location: Optional[str],
            bwa_mem_tar_file_destination: Optional[str],
            star_readme: Optional[str],
            bwa_mem_readme: Optional[str],

    ):
        self.chrom_sizes_file_location = chrom_sizes_file_location
        self.chrom_sizes_file_destination = chrom_sizes_file_destination
        self.annotation_file_location = annotation_file_location

        print(f'ANNOTATIONS FILE: {self.annotation_file_location}')
        self.annotations_file_destination = annotations_file_destination
        self.star_tar_file_location = star_tar_file_location
        self.star_tar_file_destination = star_tar_file_destination
        self.bwa_mem_tar_file_location = bwa_mem_tar_file_location
        self.bwa_mem_tar_file_destination = bwa_mem_tar_file_destination
        self.star_readme = star_readme
        self.bwa_mem_readme = bwa_mem_readme

        self.gcp = GCPCloudFunctions()

    def _replace_public_bucket_location_with_broad_bucket(self, destination_path: str) -> str:
        return destination_path.replace(
            "gcp-public-data--broad-references", self.BROAD_PUBLIC_REFERENCES_SYNC_BUCKET
        )

    def copy_files_to_public_bucket(self) -> None:
        gcs_file_mapping = [
            {
                "source": self.chrom_sizes_file_location,
                "destination": self.chrom_sizes_file_destination,
            },
            {
                "source": self.annotation_file_location,
                "destination": self.annotations_file_destination,
            },
            {
                "source": self.star_tar_file_location,
                "destination": self.star_tar_file_destination,
            },
            {
                "source": self.bwa_mem_tar_file_location,
                "destination": self.bwa_mem_tar_file_destination,
            },
            {
                "source": self.star_readme,
                "destination": self.star_tar_file_destination,
            },
            {
                "source": self.bwa_mem_readme,
                "destination": self.bwa_mem_tar_file_destination,
            }
        ]

        try:
            # Copy all source files to destination bucket
            logging.info("Copying references files from source location to Broad bucket")
            for file_mapping in gcs_file_mapping:
                source = file_mapping.get("source")
                if source:
                    dest_file_name = Path(file_mapping["source"]).name
                    destination = os.path.join(
                        self._replace_public_bucket_location_with_broad_bucket(
                            destination_path=file_mapping["destination"]
                        ),
                        dest_file_name
                    )
                    #self.gcp.copy_cloud_file(
                    #    src_cloud_path=source, full_destination_path=destination
                    #)
                    logging.info(
                        f"Successfully copied '{source}' to the following filepath: '{destination}'\n")

        except Exception as e:
            logging.error(f"Encountered an error while attempting to copy source files to destination file paths: {e}")


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Add a new public cloud reference")
    parser.add_argument(
        "--chrom_sizes_file_location",
        required=False,
        type=str,
        help="The current cloud location for the chromosome sizes file"
    )
    parser.add_argument(
        "--chrom_sizes_file_destination",
        required=False,
        type=str,
        help="The cloud destination for the chromosome sizes file"
    )
    parser.add_argument(
        "--annotation_file_location",
        required=False,
        type=str,
        help="The current cloud location for the annotations file"
    )
    parser.add_argument(
        "--annotations_file_destination",
        required=False,
        type=str,
        help="The cloud destination for the annotations file"
    )
    parser.add_argument(
        "--star_tar_file_location",
        required=False,
        type=str,
        help="The current cloud location for the star tarball file"
    )
    parser.add_argument(
        "--star_tar_file_destination",
        required=False,
        type=str,
        help="The cloud destination for the star tarball file"
    )
    parser.add_argument(
        "--bwa_mem_tar_file_location",
        required=False,
        type=str,
        help="The current cloud location for the bwa-mem tarball file"
    )
    parser.add_argument(
        "--bwa_mem_tar_file_destination",
        required=False,
        type=str,
        help="The cloud destination for the bwa-mem tarball file"
    )
    parser.add_argument(
        "--star_readme",
        required=False,
        type=str,
        help="The path to the star tar file README file"
    )
    parser.add_argument(
        "--bwa_mem_readme",
        required=False,
        type=str,
        help="The path to the bra-mem tar file README file"
    )

    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()
    CopyPublicCloudReference(
        chrom_sizes_file_location=args.chrom_sizes_file_location,
        chrom_sizes_file_destination=args.chrom_sizes_file_destination,
        annotation_file_location=args.annotation_file_location,
        annotations_file_destination=args.annotations_file_destination,
        star_tar_file_location=args.star_tar_file_location,
        star_tar_file_destination=args.star_tar_file_destination,
        bwa_mem_tar_file_location=args.bwa_mem_tar_file_location,
        bwa_mem_tar_file_destination=args.bwa_mem_tar_file_destination,
        star_readme=args.star_readme,
        bwa_mem_readme=args.bwa_mem_readme,
    ).copy_files_to_public_bucket()
