import csv
import re
import sys
import os


from utils import GCP
from utils.gcp_utils import GCPCloudFunctions
from utils.terra_utils.terra_util import TerraWorkspace
from utils.token_util import Token
from utils.request_util import RunRequest

#BILLING_PROJECT = "broad-dsp-ionis-data"
#WORKSPACE_NAME = "Color_Ionis_PCRFreeWGS"
SAMPLES_TSV_PATH = "sample.tsv"
GVCF_COLUMN_HEADER = "gvcf"
GVCF_INDEX_COLUMN_HEADER = "gvcf_index"
PRIMARY_KEY_COLUMN_HEADER = "sample_id"

# TODO change this back after testing
BILLING_PROJECT = "ops-integration-billing"
WORKSPACE_NAME = "Color_Ionis_PCRFreeWGS_COPY"


class ParseMetrics:
    def __init__(self):
        token = Token(cloud=GCP)
        self.request_util = RunRequest(token=token, max_retries=5, max_backoff_time=60)
        self.terra_workspace = TerraWorkspace(
            request_util=self.request_util,
            billing_project=BILLING_PROJECT,
            workspace_name=WORKSPACE_NAME
        )
        self.gcp = GCPCloudFunctions()

    def _get_sample_ids_missing_gvcfs(self) -> list[str]:
        samples_missing_gvcfs_links = []
        sample_metrics = self.terra_workspace.get_gcp_workspace_metrics(entity_type="sample", remove_dicts=True)
        for sample in sample_metrics:
            if not (
                    sample["attributes"].get(GVCF_COLUMN_HEADER) and sample["attributes"].get(GVCF_INDEX_COLUMN_HEADER)
            ):
                samples_missing_gvcfs_links.append(sample["name"])
        print(
            f"Found {len(samples_missing_gvcfs_links)} samples without gvcf links. Will look for corresponding paths."
        )
        return samples_missing_gvcfs_links

    def _get_all_gvcf_paths(self) -> list[str]:
        #bucket_name = self.terra_workspace.get_workspace_bucket()
        # TODO undo this after testing
        bucket_name = TerraWorkspace(
            request_util=self.request_util,
            billing_project="broad-dsp-ionis-data",
            workspace_name="Color_Ionis_PCRFreeWGS"
        ).get_workspace_bucket()

        gvcf_paths = self.gcp.list_bucket_contents(
            bucket_name=bucket_name, file_extensions_to_include=[".vcf.gz"], file_name_only=True
        )
        return [a["path"] for a in gvcf_paths]

    def _match_sample_to_gvcf_path(self, sample_ids: list[str], gvcf_paths: list[str]) -> list[dict]:
        sample_gvcf_path_mapping = []

        for sample_id in sample_ids:
            sample_alias = sample_id.split("_")[1]
            matching_paths = []
            # Check and gather all paths that contain the sample alias in it
            for gvcf_path in gvcf_paths:
                if re.search(sample_alias, gvcf_path):
                    matching_paths.append(gvcf_path)
            if len(matching_paths) != 1:
                print(
                    f"Expected to find 1 path for sample {sample_alias}, instead found {len(matching_paths)} Exiting!"
                )
                sys.exit(1)
            else:
                gvcf_path = matching_paths[0]
                gvcf_index = f"{gvcf_path}.tbi"
                if not (file_size: = self.gcp.get_filesize(target_path=gvcf_index)):
                    print(f"gvcf index path does not exist: {gvcf_index}")
                    sys.exit(1)
                sample_gvcf_path_mapping.append(
                    {
                        PRIMARY_KEY_COLUMN_HEADER: sample_id,
                        GVCF_COLUMN_HEADER: gvcf_path,
                        GVCF_INDEX_COLUMN_HEADER: gvcf_index
                    }
                )
        return sample_gvcf_path_mapping

    def _update_workspace_metadata(self):
        res = self.terra_workspace.upload_metadata_to_workspace_table(entities_tsv=SAMPLES_TSV_PATH)
        print(res)

    def run(self):
        samples_missing_gvcf_links = self._get_sample_ids_missing_gvcfs()
        all_gvcf_paths = self._get_all_gvcf_paths()
        mapping = self._match_sample_to_gvcf_path(sample_ids=samples_missing_gvcf_links, gvcf_paths=all_gvcf_paths)
        if len(samples_missing_gvcf_links) != len(mapping):
            print("A gvcf was not found for each sample!")
            sys.exit(1)
        if mapping:
            with open(SAMPLES_TSV_PATH, "w", newline="") as samples_tsv:
                writer = csv.DictWriter(samples_tsv, fieldnames=mapping[0].keys(), delimiter="\t")
                writer.writeheader()
                writer.writerows(mapping)
            print("Updating workspace metadata now")
            self._update_workspace_metadata()
        else:
            print("Found zero samples missing a gvcf link. Exiting")
            sys.exit(1)


if __name__ == '__main__':
    ParseMetrics().run()
