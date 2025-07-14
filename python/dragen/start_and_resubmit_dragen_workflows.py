"""
Script to manage Dragen workflow submissions and resubmissions in Terra workspaces.
"""

import argparse
import logging
import csv
from typing import Union

from ops_utils.token_util import Token
from ops_utils.request_util import RunRequest
from ops_utils.terra_util import TerraWorkspace
from ops_utils.gcp_utils import GCPCloudFunctions

from dragen.dragen_utils import GetSampleInfo, CreateSampleTsv, SAMPLE_TSV


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

MAX_SAMPLES_PER_BATCH = 2000
RUNNING_STATUS = "RUNNING"
FAILED_STATUS = "FAILED"
STATUS_COLUMN_HEADER = "latest_status"
MAX_RETRIES = 3


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start and resubmit Dragen workflows in Terra.")
    parser.add_argument("--ref_trigger_path", required=True, help="GCP path to the reference trigger file")
    parser.add_argument("--ref_dragen_config", required=True, help="GCP path to the reference Dragen config file")
    parser.add_argument("--ref_batch_config", required=True, help="GCP path to the reference batch config file")
    parser.add_argument("--output_bucket", required=True,
                        help="GCS bucket to store output files. Do not include 'gs://' prefix.")
    parser.add_argument("--google_project_id", required=True,
                        help="The Google Project ID where the Dragen workflows will run.")
    parser.add_argument("--data_type", required=True, help="Data type for the Dragen samples",
                        choices=["wgs", "bge", "rna"])
    parser.add_argument("--dragen_version", required=True, help="The Dragen pipeline version")
    parser.add_argument("--billing_project", required=True, help="The Terra workspace's billing project")
    parser.add_argument("--workspace_name", required=True, help="The Terra workspace name")
    parser.add_argument(
        "--samples_per_batch",
        type=int,
        default=MAX_SAMPLES_PER_BATCH,
        required=False,
        help=f"The maximum number of samples per batch to run. Defaults to {MAX_SAMPLES_PER_BATCH}",
    )
    parser.add_argument(
        "--batch_processing_start_date",
        required=True,
        help="Start date for batch processing. Should be in the format YYYY-MM-DD",
    )
    parser.add_argument(
        "--batch_processing_end_date",
        required=False,
        default="2035-10-30",
        help="End date for batch processing. Default is 2035-10-30. If provided, should be in the format YYYY-MM-DD"
    )
    parser.add_argument(
        "--sample_id_column",
        required=True,
        help="Column to use as sample ID input for WDL (for example, `sample_id`, or `collaborator_sample_id`")
    parser.add_argument(
        "--max_retries",
        required=False,
        default=MAX_RETRIES,
        help=f"Maximum number of retries for failed submissions. Default is {MAX_RETRIES}."
    )
    return parser.parse_args()


class FindSamplesForSubmission:
    def __init__(self, workspace_metadata: list[dict], samples_per_batch: int, sample_id_column: str, max_retries: int):
        self.workspace_metadata = workspace_metadata
        self.samples_per_batch = samples_per_batch
        self.sample_id_column = sample_id_column
        self.max_retries = max_retries

    def _find_running_samples(self) -> list:
        """
        Identify samples with RUNNING status.
        """
        return [
            row for row in self.workspace_metadata
            if row["attributes"].get(STATUS_COLUMN_HEADER) == RUNNING_STATUS
        ]

    def _create_batch(self) -> Union[None, tuple[str, list[dict]]]:
        """
        Create a batch of samples for submission.
        Only samples from a single research project (RP) are included in each batch.
        Prioritize non-submitted samples, and top up from failed samples if needed.
        """
        # Find all unique research projects (RPs) in workspace_metadata
        research_projects = sorted(set(row["attributes"]["rp"] for row in self.workspace_metadata))

        for rp in research_projects:
            # Filter samples by this research project that are below the max attempt account
            samples_for_rp = [row for row in self.workspace_metadata if (
                row["attributes"]["rp"] == rp and row["attributes"]["attempts"] < self.max_retries)]

            # Split into not-submitted and failed samples for this rp
            not_submitted_samples = [row for row in samples_for_rp if not row["attributes"].get(STATUS_COLUMN_HEADER)]
            failed_samples = [row for row in samples_for_rp if row["attributes"].get(
                STATUS_COLUMN_HEADER) == FAILED_STATUS]

            # Nothing to submit for this rp; try next rp
            if not (not_submitted_samples or failed_samples):
                continue

            # Figure out how many failed samples to top up with
            num_needed_from_failed = self.samples_per_batch - len(not_submitted_samples)

            if num_needed_from_failed > 0:
                batch = not_submitted_samples + failed_samples[:num_needed_from_failed]
                logging.info(
                    f"RP '{rp}': Selected {len(not_submitted_samples)} not-submitted samples "
                    f"and {min(num_needed_from_failed, len(failed_samples))} failed samples "
                    f"for a total of {len(batch)}."
                )
            else:
                batch = not_submitted_samples[:self.samples_per_batch]
                logging.info(
                    f"RP '{rp}': Selected {self.samples_per_batch} not-submitted samples and 0 failed samples "
                    f"for a total of {len(batch)} samples."
                )

            if batch:
                if self.sample_id_column == "sample_id":
                    samples_and_cram_paths = [{
                        "sample_id": row["name"],
                        "cram_path": row["attributes"]["cram_path"]
                    } for row in batch]
                else:
                    samples_and_cram_paths = [
                        {
                            "sample_id": row["attributes"][self.sample_id_column],
                            "cram_path": row["attributes"]["cram_path"]
                        } for row in batch
                    ]
                # We only ever want to return ONE batch, so we can return here
                return rp, samples_and_cram_paths

        # If the loop finishes, and we never returned: no batch found
        logging.info("No samples to submit across any research project")
        return None

    def create_sample_batch(self) -> Union[None, tuple[str, list[dict]]]:
        running_samples = self._find_running_samples()
        if running_samples:
            raise Exception(
                f"Found {len(running_samples)} samples that are still running. Will not submit new samples."
            )
        else:
            return self._create_batch()


class DragenConfigGenerator:
    def __init__(
            self,
            ref_dragen_config_gcs: str,
            ref_batch_config_gcs: str,
            output_bucket: str,
            research_project: str,
            data_type: str,
            gcp_util: GCPCloudFunctions,
    ):

        self.ref_dragen_config_gcs = ref_dragen_config_gcs
        self.ref_batch_config_gcs = ref_batch_config_gcs
        self.output_bucket = output_bucket
        self.research_project = research_project
        self.data_type = data_type
        self.gcp_util = gcp_util

    def generate_updated_dragen_and_batch_configs(self) -> tuple[str, str]:
        # Build the output path
        output_path = f"{self.output_bucket}/{self.research_project}"

        dragen_config_contents = self.gcp_util.read_file(cloud_path=self.ref_dragen_config_gcs)
        batch_config_contents = self.gcp_util.read_file(cloud_path=self.ref_batch_config_gcs)

        dragen_config_filename = "dragen_config.json"
        batch_config_filename = "batch_config.json"

        # Replace the necessary contents in the Dragen config
        with open(dragen_config_filename, "w") as dragen_config:
            updated_contents = dragen_config_contents.replace("__OUT_PATH__", f'{output_path}')
            dragen_config.write(updated_contents)

        # Replace the necessary contents in the batch config
        with open(batch_config_filename, "w") as batch_config:
            updated_contents = batch_config_contents.replace("__DATA_TYPE__", f'{self.data_type}')
            batch_config.write(updated_contents)

        logging.info("Successfully wrote updated Dragen and batch config files")
        return dragen_config_filename, batch_config_filename


class TriggerDragenWorkflows:
    def __init__(
            self,
            ref_trigger_path: str,
            dragen_config_path: str,
            batch_config_path: str,
            sample_manifest_path: str,
            google_project_id: str,
            data_type: str,
            dragen_version: str,
            gcp_util: GCPCloudFunctions,
    ):
        self.ref_trigger_path = ref_trigger_path
        self.dragen_config_path = dragen_config_path
        self.batch_config_path = batch_config_path
        self.sample_manifest_path = sample_manifest_path
        self.google_project_id = google_project_id
        self.data_type = data_type
        self.dragen_version = dragen_version
        self.gcp_util = gcp_util

    def _generate_source_destination_paths(self) -> list[dict]:
        return [
            {
                "source": self.dragen_config_path,
                "local_source": True,
                "destination": f"gs://{self.google_project_id}-config/"
            },
            {
                "source": self.batch_config_path,
                "local_source": True,
                "destination": f"gs://{self.google_project_id}-trigger/{self.data_type}/{self.dragen_version}/"
            },
            {
                "source": self.ref_trigger_path,
                "local_source": False,
                "destination": f"gs://{self.google_project_id}-trigger/{self.data_type}/{self.dragen_version}/"
            },
            {
                "source": self.sample_manifest_path,
                "local_source": True,
                "destination": f"gs://{self.google_project_id}-trigger/{self.data_type}/input_list/"
            }
        ]

    def copy_files(self) -> None:
        for item in self._generate_source_destination_paths():
            source = item["source"]
            destination = item["destination"]
            local_source = item["local_source"]

            if local_source:
                self.gcp_util.upload_blob(
                    destination_path=destination,
                    source_file=source,
                )
            else:
                self.gcp_util.copy_cloud_file(
                    src_cloud_path=source,
                    full_destination_path=destination,
                )


if __name__ == "__main__":
    args = get_args()

    if args.samples_per_batch > MAX_SAMPLES_PER_BATCH:
        raise ValueError(f"The samples per batch cannot exceed the maximum allowed value of {MAX_SAMPLES_PER_BATCH}.")

    gsc_output_bucket = args.output_bucket.strip(
        "gs://") if args.output_bucket.startswith("gs://") else args.output_bucket

    token = Token()
    request_util = RunRequest(token=token)
    gcp_util = GCPCloudFunctions()

    # Collect sample information from BigQuery
    sample_metadata = GetSampleInfo(
        google_project=args.google_project_id,
        maximum_run_date=args.batch_processing_end_date,
        minimum_run_date=args.batch_processing_start_date,
        data_type=args.data_type,
    ).run()

    # Create a tsv in the format required for Terra upload
    CreateSampleTsv(
        samples_dict=sample_metadata,
        output_tsv=SAMPLE_TSV
    ).create_tsv()

    # Upload the TSV to the specified Terra workspace that includes the batch status from BigQuery
    logging.info(f"Uploading {SAMPLE_TSV} to workspace {args.billing_project}/{args.workspace_name}")
    workspace = TerraWorkspace(
        billing_project=args.billing_project,
        workspace_name=args.workspace_name,
        request_util=request_util
    )
    workspace.upload_metadata_to_workspace_table(entities_tsv=SAMPLE_TSV)

    # Collect ALL workspace metadata to get the status of all samples
    workspace_metrics = workspace.get_gcp_workspace_metrics(entity_type="sample", remove_dicts=True)

    # Create a batch of samples for submission to Batch
    research_project, dragen_sample_batch = FindSamplesForSubmission(  # type: ignore[misc]
        workspace_metadata=workspace_metrics,
        samples_per_batch=args.samples_per_batch,
        sample_id_column=args.sample_id_column,
        max_retries=args.max_retries,
    ).create_sample_batch()

    if dragen_sample_batch:
        sample_manifest_filename = "sample_processing_manifest.txt"

        # Write the batch sample manifest
        headers = [f for f in dragen_sample_batch[0].keys()]
        with open(sample_manifest_filename, "w") as f:
            writer = csv.DictWriter(f, fieldnames=headers, delimiter=" ")
            writer.writeheader()
            writer.writerows(dragen_sample_batch)

        # Generate the Dragen Config files
        dragen_config_file, batch_config_file = DragenConfigGenerator(
            ref_dragen_config_gcs=args.ref_dragen_config,
            ref_batch_config_gcs=args.ref_batch_config,
            output_bucket=gsc_output_bucket,
            research_project=research_project,
            data_type=args.data_type,
            gcp_util=gcp_util,
        ).generate_updated_dragen_and_batch_configs()

        # Trigger the Dragen workflow submission by coping files to their expected output locations
        TriggerDragenWorkflows(
            ref_trigger_path=args.ref_trigger_path,
            dragen_config_path=dragen_config_file,
            batch_config_path=batch_config_file,
            sample_manifest_path=sample_manifest_filename,
            google_project_id=args.google_project_id,
            data_type=args.data_type,
            dragen_version=args.dragen_version,
            gcp_util=gcp_util,
        ).copy_files()
