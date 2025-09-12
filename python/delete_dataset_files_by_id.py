import logging
from argparse import ArgumentParser, Namespace

from ops_utils.request_util import RunRequest
from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.tdr_utils.tdr_job_utils import MonitorTDRJob
from ops_utils.token_util import Token

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    """Parse CLI args for deleting dataset files and related snapshots."""
    parser = ArgumentParser(description="Delete dataset files by ID")
    parser.add_argument("-id", "--dataset_id", required=True)
    parser.add_argument(
        "-f",
        "--file_list",
        required=True,
        help="Path to file with file UUIDs (one per line)",
    )
    parser.add_argument(
        "--service_account_json",
        "-saj",
        type=str,
        help=(
            "Path to service account JSON. Uses default "
            "credentials if omitted."
        ),
    )
    parser.add_argument(
        "--dry_run",
        "-n",
        action="store_true",
        help=(
            "Do not perform deletions; log actions that would be taken."
        ),
    )
    return parser.parse_args()


class DeleteDatasetFilesById:
    """Class to delete files from a TDR dataset by their IDs, handling snapshots."""

    def __init__(self, tdr: TDR, dataset_id: str, file_id_set: set[str], dry_run: bool = False):
        self.tdr = tdr
        self.dataset_id = dataset_id
        self.dry_run = dry_run
        self.file_id_set = file_id_set

    def _delete_snapshots(self) -> None:
        """Delete snapshots that reference any of the provided file IDs."""
        snapshots_resp = self.tdr.get_dataset_snapshots(dataset_id=self.dataset_id)
        snapshot_items = snapshots_resp.json().get('items', [])
        snapshots_to_delete = []
        logging.info(
            "Checking %d snapshots for references",
            len(snapshot_items),
        )
        for snap in snapshot_items:
            snap_id = snap.get('id')
            if not snap_id:
                continue
            snap_files = self.tdr.get_files_from_snapshot(snapshot_id=snap_id)
            snap_file_ids = {
                fd.get('fileId') for fd in snap_files if fd.get('fileId')
            }
            # Use set intersection to check for any matching file IDs
            if snap_file_ids & self.file_id_set:
                snapshots_to_delete.append(snap_id)
        if snapshots_to_delete:
            logging.info(
                f"{"[Dry run] " if self.dry_run else ""}Deleting {len(snapshots_to_delete)} snapshots that reference "
                f"target files")
            if not self.dry_run:
                for snap_id in snapshots_to_delete:
                    job_id = self.tdr.delete_snapshot(snap_id).json()['id']
                    MonitorTDRJob(
                        tdr=self.tdr,
                        job_id=job_id,
                        check_interval=10,
                        return_json=False
                    ).run()
        else:
            logging.info("No snapshots reference the provided file ids")

    def delete_files_and_snapshots(self) -> None:
        self._delete_snapshots()

        logging.info(
            f"{"[Dry run] " if self.dry_run else ""}Submitting delete request for {len(self.file_id_set)} files in "
            f"dataset {self.dataset_id}")
        if not self.dry_run:
            self.tdr.delete_files(
                file_ids=list(self.file_id_set),
                dataset_id=self.dataset_id
            )


if __name__ == '__main__':
    args = get_args()
    service_account_json = args.service_account_json

    token = Token(service_account_json=service_account_json)
    request_util = RunRequest(token=token)
    file_list = args.file_list

    with open(file_list, 'r') as f:
        file_ids = {line.strip() for line in f}

    if not file_ids:
        logging.info("No file ids provided; nothing to delete")
    else:
        logging.info(f"Found {len(file_ids)} file ids in {file_list} to delete")

        DeleteDatasetFilesById(
            tdr=TDR(request_util=request_util),
            dataset_id=args.dataset_id,
            file_id_set=file_ids,
            dry_run=args.dry_run
        ).delete_files_and_snapshots()
