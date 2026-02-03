import csv
import io
import json
import argparse
import logging
from google.cloud import storage

from ops_utils.request_util import RunRequest
from ops_utils.terra_util import TerraWorkspace
from ops_utils.token_util import Token

"""
Given a source workspace, a sample mapping csv (with subject IDs and consent codes), and a consent 
code-to-workspace mapping csv, this script creates a JSON mapping file that can be used as input 
to the Terra multisample VCF splitting workflow.
"""


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser("")
    parser.add_argument("--source_workspace_name", required=True, type=str)
    parser.add_argument("--sample_mapping_bucket", required=True, type=str,
                        help="Bucket where the sample mapping csv is stored in GCS")
    parser.add_argument("--sample_mapping_path", required=True, type=str,
                        help="Path to where the sample mapping csv is stored in GCS")
    parser.add_argument("--consent_code_workspace_mapping_map", required=False, type=str,
                        help="Path to local consent code-to-workspace mapping csv")
    return parser.parse_args()


class CreateMapping:
    def __init__(self, source_workspace_name, bucket_name: str, blob_name: str, consent_code_mapping: list[dict], request_util: RunRequest):
        self.source_workspace_name = source_workspace_name
        self.blob_name = blob_name
        self.bucket_name = bucket_name
        self.consent_code_mapping = consent_code_mapping
        self.request_util = request_util

    def read_csv_from_gcs(self):
        file_contents = []
        client = storage.Client()
        # Get the bucket + blob
        bucket = client.bucket(self.bucket_name)
        blob = bucket.blob(self.blob_name)

        # Download blob as text
        csv_data = blob.download_as_text()
        # Wrap the string in a file-like object
        csv_file = io.StringIO(csv_data)

        reader = csv.DictReader(csv_file)
        for row in reader:
            file_contents.append(row)
        logging.info("Successfully gathered sample-to-consent code mapping from GCS")
        return file_contents

    def create_sample_mapping(self, sample_mapping_file_contents: list[dict]):
        sample_mapping_records = []
        for row in sample_mapping_file_contents:
            subject_id = row["subject_id"]
            consent_code = row["consent"]
            matching_record = [x for x in self.consent_code_mapping if x["Consent Code"] == f"c{consent_code}"]
            if matching_record:
                sample_mapping_records.append(
                    {
                        "subject_id": subject_id,
                        "consent_code": matching_record[0]["Consent Code"],
                        "destination_workspace_name": matching_record[0]["AnVIL Dataset Name"],
                    }
                )
        logging.info("Successfully mapped samples to consent codes and destination workspaces")
        return sample_mapping_records

    def get_workspace_bucket(self, workspace_name: str) -> str:
        workspace = TerraWorkspace(
            billing_project="anvil-datastorage",
            workspace_name=workspace_name,
            request_util=self.request_util
        )
        return workspace.get_workspace_bucket()

    def construct_json(self, sample_workspace_mapping):
        final_mapping_contents = []

        consent_codes = set([c["consent_code"] for c in sample_workspace_mapping])

        for consent_code in consent_codes:
            samples = [s["subject_id"] for s in sample_workspace_mapping if s["consent_code"] == consent_code]
            workspace_name = [s["destination_workspace_name"]
                              for s in sample_workspace_mapping if s["consent_code"] == consent_code][0]
            destination_workspace_bucket = self.get_workspace_bucket(workspace_name=workspace_name)
            final_mapping_contents.append(
                {
                    "prefix": consent_code,
                    "destination_workspace_name": workspace_name,
                    "destination_workspace_bucket": destination_workspace_bucket,
                    "samples": samples,
                }
            )

        output_json_path = f"{self.source_workspace_name}_to_anvil_sample_mapping.json"
        with open(output_json_path, "w") as json_file:
            json.dump(final_mapping_contents, json_file, indent=4)
        logging.info(
            f"Created mapping JSON to be used as input to Terra multisample VCF splitting workflow. Located here: '{output_json_path}'"
        )

    def run(self):
        sample_mapping_file_contents = self.read_csv_from_gcs()
        sample_to_workspace_mapping = self.create_sample_mapping(
            sample_mapping_file_contents=sample_mapping_file_contents)
        self.construct_json(sample_workspace_mapping=sample_to_workspace_mapping)


if __name__ == "__main__":
    args = get_args()
    auth_token = Token()
    request_util = RunRequest(token=auth_token)

    consent_code_mapping = []
    with open(args.consent_code_workspace_mapping_map) as mapping_file:
        reader = csv.DictReader(mapping_file, delimiter=",")
        for row in reader:
            if row["AnVIL Dataset Name"].startswith(args.source_workspace_name):
                consent_code_mapping.append(row)

    CreateMapping(
        source_workspace_name=args.source_workspace_name,
        bucket_name=args.sample_mapping_bucket,
        blob_name=args.sample_mapping_path,
        consent_code_mapping=consent_code_mapping,
        request_util=request_util
    ).run()
