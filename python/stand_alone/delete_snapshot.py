"""Delete a dataset(s) in TDR"""
import logging
from argparse import ArgumentParser, Namespace

from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.tdr_utils.tdr_job_utils import MonitorTDRJob
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
from collections import defaultdict

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

BATCH_SIZE = 10
CHECK_INTERVAL = 20


def get_args() -> Namespace:
    parser = ArgumentParser(description="Delete a snapshot in TDR")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--snapshot_id", "-i")
    input_group.add_argument("--snapshot_id_file", "-f")
    parser.add_argument(
        "--batch_size", "-b", default=BATCH_SIZE, type=int,
        help="Number of snapshots to delete at once if passing in file")
    parser.add_argument(
        "--check_interval", "-c", default=CHECK_INTERVAL, type=int,
        help="Time in seconds to check for deletion completion")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging for batching")
    return parser.parse_args()


class DeleteSnapshots:
    def __init__(self, tdr: TDR, snapshot_ids: list[str], batch_size: int, check_interval: int, verbose: bool):
        self.tdr = tdr
        self.snapshot_ids = snapshot_ids
        self.batch_size = batch_size
        self.check_interval = check_interval
        self.verbose = verbose

    def _create_default_dict(self) -> defaultdict:
        dataset_def_dict = defaultdict(list)  # Dictionary to group snapshots by dataset_id

        # Create a dictionary with dataset IDs as keys and a list of snapshot IDs as values
        for snapshot_id in self.snapshot_ids:
            response = self.tdr.get_snapshot_info(snapshot_id=snapshot_id, continue_not_found=True)
            if response:
                snapshot_info = response.json()
                for source_dict in snapshot_info['source']:
                    # Check if the source is a dataset
                    if 'dataset' in source_dict:
                        dataset_id = source_dict['dataset']['id']
                        dataset_def_dict[dataset_id].append(snapshot_id)
        return dataset_def_dict

    def _create_batches_to_delete(self, dataset_default_dict: defaultdict) -> list[list[str]]:
        # Final ordered list of snapshots for batch processing
        snapshots_to_delete = []
        # Loop until all no datasets are left with snapshots to delete
        while dataset_default_dict:
            current_batch: list[str] = []
            dataset_ids_to_remove = []

            for dataset_id, snapshots in dataset_default_dict.items():
                if len(current_batch) >= self.batch_size:
                    break  # Stop adding to batch if we reach the batch size

                # Add one snapshot from the current dataset to the batch
                current_batch.append(snapshots.pop(0))
                # Mark dataset to remove if there are no more snapshots left
                if not snapshots:
                    dataset_ids_to_remove.append(dataset_id)

            # Remove empty dataset entries outside the loop to avoid
            # modifying the dictionary during iteration
            for dataset_id in dataset_ids_to_remove:
                del dataset_default_dict[dataset_id]

            # Add the batch to the final list
            snapshots_to_delete.append(current_batch)
        return snapshots_to_delete

    def _run_batch_deletes(self, snapshots_batches_to_delete: list[list[str]]) -> None:
        for batch in snapshots_batches_to_delete:
            snapshot_str = ', '.join(batch)
            logging.info(f"Deleting batch of {len(batch)} snapshots: \n{snapshot_str}")
            self.tdr.delete_snapshots(
                check_interval=self.check_interval,
                batch_size=self.batch_size,
                snapshot_ids=batch,
                verbose=self.verbose
            )
            logging.info(f"Completed deletion of batch: {batch}")

        logging.info("Successfully deleted all snapshots.")

    def run(self) -> None:
        dataset_default_dict = self._create_default_dict()
        snapshot_batches_to_delete = self._create_batches_to_delete(dataset_default_dict)
        self._run_batch_deletes(snapshot_batches_to_delete)


if __name__ == '__main__':
    args = get_args()
    snapshot_id = args.snapshot_id
    snap_shot_id_file = args.snapshot_id_file
    batch_size = args.batch_size
    check_interval = args.check_interval
    verbose = args.verbose

    token = Token()
    request_util = RunRequest(token=token)
    tdr = TDR(request_util=request_util)

    if snapshot_id:
        job_id = tdr.delete_snapshot(snapshot_id=snapshot_id).json()["id"]
        MonitorTDRJob(tdr=tdr, job_id=job_id, check_interval=check_interval, return_json=False).run()
    else:
        with open(snap_shot_id_file) as file:
            snapshot_ids = [line.strip() for line in file]
        DeleteSnapshots(
            tdr=tdr,
            snapshot_ids=snapshot_ids,
            batch_size=batch_size,
            check_interval=check_interval,
            verbose=verbose
        ).run()
