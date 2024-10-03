"""Delete a dataset in TDR"""
import json
import logging
import sys
from argparse import ArgumentParser, Namespace

from utils.tdr_utils.tdr_api_utils import TDR
from utils.tdr_utils.tdr_job_utils import MonitorTDRJob
from utils.request_util import RunRequest
from utils.token_util import Token
from utils import GCP
from collections import defaultdict

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

CLOUD_TYPE = GCP


def get_args() -> Namespace:
    parser = ArgumentParser(description="Delete a snapshot in TDR")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--snapshot_id", "-i")
    input_group.add_argument("--snapshot_id_file", "-f")
    return parser.parse_args()


def delete_snapshots_in_batches(tdr: TDR, snapshot_ids: list[str], batch_size: int = 10, check_interval: int = 5) -> None:
    """
    Delete snapshots in batches, ensuring that snapshots from the same dataset are not batched together.

    Args:
        tdr: TDR instance for interacting with TDR.
        snapshot_ids (list[str]): List of snapshot IDs to delete.
        batch_size (int, optional): Number of snapshots to delete per batch. Defaults to 10.
        check_interval (int, optional): Interval between job status checks. Defaults to 5 seconds.
    """
    # Step 1: Group snapshots by dataset
    dataset_def_dict = defaultdict(list)  # Dictionary to group snapshots by dataset_id

    # Create a dictionary with dataset IDs as keys and a list of snapshot IDs as values
    for snapshot_id in snapshot_ids:
        snapshot_info = tdr.get_snapshot_info(snapshot_id=snapshot_id, continue_not_found=True)
        if snapshot_info:
            for source_dict in snapshot_info['source']:
                # Check if the source is a dataset
                if 'dataset' in source_dict:
                    dataset_id = source_dict['dataset']['id']
                    dataset_def_dict[dataset_id].append(snapshot_id)

    # Step 2: Build batches of snapshots with different dataset IDs
    snapshots_to_delete = []  # Final ordered list of snapshots for batch processing
    # Loop until all no datasets are left with snapshots to delete
    while dataset_def_dict:
        current_batch: list[str] = []
        dataset_ids_to_remove = []

        for dataset_id, snapshots in dataset_def_dict.items():
            if len(current_batch) >= batch_size:
                break  # Stop adding to batch if we reach the batch size

            # Add one snapshot from the current dataset to the batch
            current_batch.append(snapshots.pop(0))
            # Mark dataset to remove if there are no more snapshots left
            if not snapshots:
                dataset_ids_to_remove.append(dataset_id)

        # Remove empty dataset entries outside the loop to avoid
        # modifying the dictionary during iteration
        for dataset_id in dataset_ids_to_remove:
            del dataset_def_dict[dataset_id]

        # Add the batch to the final list
        snapshots_to_delete.append(current_batch)

    # Step 3: Submit and monitor batches
    for batch in snapshots_to_delete:
        logging.info(f"Deleting batch of {len(batch)} snapshots: {batch}")
        tdr.delete_snapshots(
            check_interval=check_interval,
            batch_size=batch_size,
            snapshot_ids=batch
        )
        logging.info(f"Completed deletion of batch: {batch}")

    logging.info("Successfully deleted all snapshots.")


if __name__ == '__main__':
    args = get_args()
    snapshot_id = args.snapshot_id
    snap_shot_id_file = args.snapshot_id_file

    token = Token(cloud=CLOUD_TYPE)
    request_util = RunRequest(token=token)
    tdr = TDR(request_util=request_util)

    if snapshot_id:
        job_id = tdr.delete_snapshot(snapshot_id=snapshot_id)
        MonitorTDRJob(tdr=tdr, job_id=job_id, check_interval=5).run()
    else:
        with open(snap_shot_id_file) as file:
            snapshot_ids = [line.strip() for line in file]
        delete_snapshots_in_batches(tdr=tdr, snapshot_ids=snapshot_ids, batch_size=2, check_interval=5)
