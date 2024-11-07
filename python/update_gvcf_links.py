import csv
import re
import sys
import os
from pathlib import Path

from utils import GCP
from utils.gcp_utils import GCPCloudFunctions
from utils.terra_utils.terra_util import TerraWorkspace
from utils.token_util import Token
from utils.request_util import RunRequest

BILLING_PROJECT = "broad-dsp-ionis-data"
WORKSPACE_NAME = "Color_Ionis_PCRFreeWGS"
SAMPLES_TSV_PATH = "sample.tsv"
GVCF_COLUMN_HEADER = "gvcf"
GVCF_INDEX_COLUMN_HEADER = "gvcf_index"
PRIMARY_KEY_COLUMN_HEADER = "sample_id"


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
                    sample["attributes"].get(GVCF_COLUMN_HEADER)
                    and sample["attributes"].get(GVCF_INDEX_COLUMN_HEADER)
                    and sample["attributes"].get("collaborator_sample_id")
                    and sample["attributes"].get("crai_path")
                    and sample["attributes"].get("cram_path")
                    and sample["attributes"].get("data_type")
                    and sample["attributes"].get("md5_path")
                    and sample["attributes"].get("project")
                    and sample["attributes"].get("research_project")
                    and sample["attributes"].get("sample")
                    and sample["attributes"].get("version")
            ):
                samples_missing_gvcfs_links.append(sample["name"])
        print(
            f"Found {len(samples_missing_gvcfs_links)} samples with missing metadata. Will look for corresponding paths."
        )
        return samples_missing_gvcfs_links

    def _get_all_gvcf_paths(self) -> list[str]:
        bucket_name = self.terra_workspace.get_workspace_bucket()
        gvcf_paths = self.gcp.list_bucket_contents(
            bucket_name=bucket_name, file_extensions_to_include=[".vcf.gz"], file_name_only=True
        )
        return [a["path"] for a in gvcf_paths]

    def _get_all_cram_paths(self) -> list[str]:
        bucket_name = self.terra_workspace.get_workspace_bucket()

        cram_paths = self.gcp.list_bucket_contents(
            bucket_name=bucket_name, file_extensions_to_include=[".cram"], file_name_only=True
        )
        return [a["path"] for a in cram_paths]

    def _match_sample_to_file_paths(self, sample_ids: list[str], gvcf_paths: list[str], cram_paths: list[str]) -> list[dict]:
        sample_gvcf_path_mapping = []

        for sample_id in sample_ids:
            pattern = r'^(RP-\d+)_([^\v]+)_v(\d+)_([\w]+)_GCP$'
            match = re.match(pattern, sample_id)

            project = match.group(1)
            sample_alias = match.group(2)
            version = match.group(3)
            data_type = match.group(4)

            matching_gvcf_paths = []
            matching_cram_paths = []
            # Check and gather all paths that contain the sample alias in it
            for gvcf_path in gvcf_paths:
                if re.search(sample_alias, gvcf_path):
                    matching_gvcf_paths.append(gvcf_path)
            for cram_path in cram_paths:
                file_name = Path(cram_path).name
                if file_name == f"{sample_alias}.cram":
                    matching_cram_paths.append(cram_path)

            if len(matching_gvcf_paths) != 1 and len(matching_cram_paths) != 1:
                print(
                    f"Expected to find 1 gvcf and 1 cram path for sample {sample_alias}, instead found {len(matching_gvcf_paths)} gvcfs and {len(matching_cram_paths)} crams. Matching gvcf paths: {matching_cram_paths}. Exiting!"
                )
                sys.exit(1)

            else:
                gvcf_path = matching_gvcf_paths[0]
                gvcf_index = f"{gvcf_path}.tbi"
                file_size = self.gcp.get_filesize(target_path=gvcf_index)
                if not file_size:
                    print(f"gvcf index path does not exist: {gvcf_index}")
                    sys.exit(1)

                cram_path = matching_cram_paths[0]
                crai_path = f"{cram_path}.crai"
                md5_path = f"{cram_path}.md5"
                crai_path_file_size = self.gcp.get_filesize(target_path=crai_path)
                md5_path_file_size = self.gcp.get_filesize(target_path=md5_path)
                if not crai_path_file_size or not md5_path_file_size:
                    print(f"either crai path or md5 doesn't exist: {md5_path}, {crai_path}")

                sample_gvcf_path_mapping.append(
                    {
                        PRIMARY_KEY_COLUMN_HEADER: sample_id,
                        GVCF_COLUMN_HEADER: gvcf_path,
                        GVCF_INDEX_COLUMN_HEADER: gvcf_index,
                        "project": project,
                        "version": version,
                        "sample": sample_alias,
                        "collaborator_sample_id": sample_alias,
                        "data_type": data_type,
                        "md5_path": md5_path,
                        "cram_path": cram_path,
                        "crai_path": crai_path,
                        "research_project": project,
                    }
                )
        return sample_gvcf_path_mapping

    def _update_workspace_metadata(self):
        res = self.terra_workspace.upload_metadata_to_workspace_table(entities_tsv=SAMPLES_TSV_PATH)
        print(res)

    def run(self):
        samples_missing_gvcf_links = self._get_sample_ids_missing_gvcfs()
        all_gvcf_paths = self._get_all_gvcf_paths()
        all_cram_paths = self._get_all_cram_paths()
        mapping = self._match_sample_to_file_paths(
            sample_ids=samples_missing_gvcf_links, gvcf_paths=all_gvcf_paths, cram_paths=all_cram_paths)
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
