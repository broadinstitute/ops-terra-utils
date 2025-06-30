from ops_utils.terra_util import TerraWorkspace
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils.csv_util import Csv
import logging
from argparse import ArgumentParser, Namespace

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

SAMPLE_SET_TSV = "sample_set_entity.tsv"
FAILED = "FAILED"


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="Get information for files in workspace bucket and upload metata to file_metadata table")
    parser.add_argument("--workspace_name", "-w", required=True, type=str)
    parser.add_argument("--billing_project", "-b", required=True, type=str)
    parser.add_argument("--sample_set_append", "-sa", type=str, required=True)
    parser.add_argument("--max_per_sample_set", "-mps", type=int, default=2000)
    parser.add_argument("--upload", "-u", action='store_true')
    return parser.parse_args()


class FailedSamplesProcessor:
    def __init__(self, terra_workspace: TerraWorkspace):
        """
        Initialize with a TerraWorkspace object

        Args:
            terra_workspace: TerraWorkspace instance to retrieve data from
        """
        self.terra_workspace = terra_workspace

    def get_failed_samples_by_rp(self) -> dict:
        """
        Get a dictionary of failed samples grouped by research project

        Returns:
            dict: Dictionary with RP as key and list of terra_ids as values
        """
        # Get all sample metrics from the workspace
        sample_metrics = self.terra_workspace.get_gcp_workspace_metrics(entity_type='sample')

        # Initialize the result dictionary
        failed_samples_by_rp = {}

        # Process each sample
        for sample_dict in sample_metrics:
            # Check if the sample has failed status
            if sample_dict['attributes'].get('latest_status') == FAILED:
                terra_id = sample_dict['name']
                rp = sample_dict['attributes']['rp']

                # Add to dictionary, creating a new list if RP doesn't exist yet
                if rp not in failed_samples_by_rp:
                    failed_samples_by_rp[rp] = [terra_id]
                else:
                    failed_samples_by_rp[rp].append(terra_id)

        return failed_samples_by_rp


def write_failed_samples_tsv(
    failed_sample_dict: dict,
    sample_set_append: str,
    max_per_sample_set: int
) -> None:
    """
    Write a TSV file with columns sample_set_id and sample, batching samples per RP.
    """
    logging.info(f"Creating {SAMPLE_SET_TSV} with max {max_per_sample_set} samples per set")
    all_batch_samples = []
    for rp, samples in failed_sample_dict.items():
        logging.info(f"Processing {rp} with {len(samples)} failed samples")
        for batch_int, i in enumerate(range(0, len(samples), max_per_sample_set), 1):
            batch_samples = samples[i:i + max_per_sample_set]
            sample_set_id = f"{rp}_{sample_set_append}_batch_{batch_int}"
            for sample in batch_samples:
                all_batch_samples.append({"membership:sample_set_id": sample_set_id, "sample": sample})
    Csv(file_path=SAMPLE_SET_TSV, delimiter='\t').create_tsv_from_list_of_dicts(
        header_list=["membership:sample_set_id", "sample"],
        list_of_dicts=all_batch_samples,
    )


if __name__ == '__main__':
    args: Namespace = get_args()
    billing_project: str = args.billing_project
    workspace_name: str = args.workspace_name
    sample_set_append: str = args.sample_set_append
    max_per_sample_set: int = args.max_per_sample_set
    upload_to_workspace: bool = args.upload

    # Create token object. This gets your token for the API calls and auto refreshes when needed
    token = Token()
    # Create request object to make API calls and pass in token
    request_util = RunRequest(token=token)
    # Create TDR and Terra objects to interact with the TDR and Terra with the request_util object
    terra_workspace = TerraWorkspace(
        request_util=request_util,
        billing_project=billing_project,
        workspace_name=workspace_name
    )
    failed_sample_dict = FailedSamplesProcessor(terra_workspace=terra_workspace).get_failed_samples_by_rp()
    write_failed_samples_tsv(failed_sample_dict, sample_set_append, max_per_sample_set)
    if upload_to_workspace:
        logging.info(f"Uploading {SAMPLE_SET_TSV} to workspace {billing_project}/{workspace_name}")
        terra_workspace.upload_metadata_to_workspace_table(entities_tsv=SAMPLE_SET_TSV)
    else:
        logging.info(f"Skipping upload. Created {SAMPLE_SET_TSV} with failed samples.")
