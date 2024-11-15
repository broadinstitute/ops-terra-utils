from argparse import ArgumentParser, Namespace
from typing import Optional
from utils.gcp_utils import GCPCloudFunctions
import logging
import subprocess

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(description="Re-upload file that so that it has a md5 hash in metadata")
    parser.add_argument(
        "--gcp_file_path",
        "-f",
        required=True
    )
    parser.add_argument(
        "-r",
        "--requester_pays_project",
        required=False,
        help="The project that will be billed for the request. Needs to be used if bucket is set to requester pays"
    )
    return parser.parse_args()


class ReUploadFile:
    def __init__(self, file_path: str, temp_file_path: str, requester_pays_project: Optional[str] = None):
        self.file_path = file_path
        self.temp_file_path = temp_file_path
        self.requester_pays_project = requester_pays_project
        self.gcp_util = GCPCloudFunctions(project=requester_pays_project)

    def _copy_to_temp(self) -> None:
        """Have to run using gcloud and not python library as daisy chain option is not available"""
        logging.info(f"Copying {self.file_path} to {self.temp_file_path} with --daisy-chain option")
        cmd_list = ["gcloud", "storage", "cp", "-D", self.file_path, self.temp_file_path]
        if self.requester_pays_project:
            cmd_list.append(f"--billing-project={self.requester_pays_project}")
        subprocess.run(cmd_list)
        logging.info("Copy completed")

    def _validate_files_same_size(self) -> None:
        original_file_size = self.gcp_util.get_filesize(self.file_path)
        temp_file_size = self.gcp_util.get_filesize(self.temp_file_path)
        if original_file_size != temp_file_size:
            raise ValueError(f"Original file size {original_file_size} does not match temp file size {temp_file_size}")
        logging.info(f"File sizes match for {self.file_path} and {self.temp_file_path}")

    def _move_temp_to_original(self) -> None:
        logging.info(f"Moving {self.temp_file_path} to {self.file_path}")
        self.gcp_util.move_cloud_file(
            src_cloud_path=self.temp_file_path,
            full_destination_path=self.file_path
        )

    def run(self) -> None:
        self._copy_to_temp()
        self._validate_files_same_size()
        self._move_temp_to_original()


if __name__ == '__main__':
    args = get_args()
    gcp_file_path = args.gcp_file_path
    tmp_file_path = f"{gcp_file_path}.tmp"
    requester_pays_project = args.requester_pays_project

    ReUploadFile(
        file_path=gcp_file_path,
        temp_file_path=tmp_file_path,
        requester_pays_project=requester_pays_project
    ).run()
