from argparse import ArgumentParser, Namespace
import logging
import os
from ops_utils.terra_util import TerraWorkspace
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils.tdr_utils.tdr_ingest_utils import FilterAndBatchIngest
from ops_utils.tdr_utils.tdr_api_utils import TDR
from ops_utils.csv_util import Csv
import time


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

DRAGEN_VERSION = "07.021.604.3.7.8"


class GetSampleInfo:
    def __init__(self, sample_set: str, terra_workspace: TerraWorkspace, dragen_version: str):
        self.sample_set = sample_set
        self.terra_workspace = terra_workspace
        self.dragen_version = dragen_version

    def _get_sample_ids(self) -> list[str]:
        """Get sample ids from only specific sample set"""
        sample_set_metadata = self.terra_workspace.get_gcp_workspace_metrics(entity_type="sample_set")
        for sample_set_dict in sample_set_metadata:
            if sample_set_dict['name'] == self.sample_set:
                return [
                    sample_dict['entityName']
                    for sample_dict in sample_set_dict['attributes']['samples']['items']
                ]
        raise Exception(f"Sample set not found: {self.sample_set}")

    def _convert_to_tdr_dict(self, sample_dict: dict) -> dict:
        """Convert sample metadata to TDR format."""
        safe_sample_str = str(sample_dict['collaborator_sample_id']).replace(' ', '_')
        file_path_prefix = os.path.join(sample_dict['output_path'], safe_sample_str)
        return {
            "analysis_date": sample_dict["last_attempt"],
            "collaborator_participant_id": str(sample_dict["collaborator_participant_id"]),
            "collaborator_sample_id": str(sample_dict["collaborator_sample_id"]),
            "contamination_rate": sample_dict["contamination_rate"],
            "genome_crai_path": f"{file_path_prefix}.cram.crai",
            "genome_cram_md5_path": f"{file_path_prefix}.cram.md5sum",
            "genome_cram_path": f"{file_path_prefix}.cram",
            "data_type": sample_dict["data_type"],
            "exome_gvcf_md5_path": f"{file_path_prefix}.hard-filtered.gvcf.gz.md5sum",
            "exome_gvcf_index_path": f"{file_path_prefix}.hard-filtered.gvcf.gz.tbi",
            "exome_gvcf_path": f"{file_path_prefix}.hard-filtered.gvcf.gz",
            "mapping_metrics_file": f"{file_path_prefix}.mapping_metrics.csv",
            "mean_target_coverage": sample_dict["mean_target_coverage"],
            "percent_target_bases_at_10x": sample_dict["percent_target_bases_at_10x"],
            "percent_callability": sample_dict["percent_callability"],
            "percent_wgs_bases_at_1x": sample_dict["percent_wgs_bases_at_1x"],
            # This is not always available in the sample metadata
            "reported_sex": sample_dict.get("reported_sex", ""),
            "research_project": sample_dict["rp"],
            "root_sample_id": sample_dict["root_sample_id"],
            "sample_id": str(sample_dict["root_sample_id"]),
            "bge_single_sample_vcf_path": f"{file_path_prefix}.hard-filtered.vcf.gz",
            "bge_single_sample_vcf_index_path": f"{file_path_prefix}.hard-filtered.vcf.gz.tbi",
            "bge_single_sample_vcf_md5_path": f"{file_path_prefix}.hard-filtered.vcf.gz.md5sum",
            "chimera_rate": sample_dict["chimera_rate"],
            "mapped_reads": sample_dict["mapped_reads"],
            "total_bases": sample_dict["total_bases"],
            "pdo": sample_dict["pdo"],
            "product": sample_dict["product"],
            "mean_off_target_coverage": sample_dict["mean_off_target_coverage"],
            "exome_coverage_region_1_metrics": f"{file_path_prefix}.qc-coverage-region-1_coverage_metrics.csv",
            "off_target_coverage_region_2_metrics": f"{file_path_prefix}.qc-coverage-region-2_coverage_metrics.csv",
            "wgs_coverage_region_3_metrics": f"{file_path_prefix}.qc-coverage-region-3_coverage_metrics.csv",
            "variant_calling_metrics_file": f"{file_path_prefix}.vc_metrics.csv",
            "dragen_version": self.dragen_version
        }

    def _get_sample_metadata(self, sample_ids: list[str]) -> list[dict]:
        """Get sample metadata for specific sample ids"""
        sample_metadata = self.terra_workspace.get_gcp_workspace_metrics(entity_type="sample")
        return [
            self._convert_to_tdr_dict(sample_dict['attributes'])
            for sample_dict in sample_metadata
            if sample_dict['entityType'] == 'sample'
            and sample_dict['name'] in sample_ids
        ]

    def run(self) -> list[dict]:
        sample_ids = self._get_sample_ids()
        return self._get_sample_metadata(sample_ids)


def get_args() -> Namespace:
    argparser = ArgumentParser(description=__doc__)
    argparser.add_argument("--sample_set", "-s", required=True)
    argparser.add_argument("--billing_project", "-b")
    argparser.add_argument("--workspace_name", "-w")
    argparser.add_argument("--dry_run", "-d", action='store_true')
    argparser.add_argument("--filter_existing_ids", "-f", action='store_true',
                           help="Filter out sample_ids that already exist in the TDR dataset")
    argparser.add_argument("--unique_id_field", "-u", default="sample_id",
                           help="Field to use for filtering existing sample_ids")
    argparser.add_argument("--table_name", "-t", default="sample")
    argparser.add_argument("--dataset_id", "-i", required=True,
                           help="ID of the TDR dataset to ingest data into")
    argparser.add_argument("--ingest_waiting_time_poll", "-p", type=int, default=180,
                           help="Time in seconds to wait between polling for ingest status")
    argparser.add_argument("--ingest_batch_size", "-bs", type=int, default=1000,
                           help="Number of rows to batch for ingesting into TDR")
    argparser.add_argument("--bulk_mode", "-m", action="store_true",
                           help="Use bulk mode for ingesting data into TDR")
    argparser.add_argument("--update_strategy", "-us", choices=["replace", "merge", "append"], default="replace",
                           help="Strategy for updating existing data in TDR")
    argparser.add_argument("--dragen_version", "-v", default=DRAGEN_VERSION,
                           help=f"Version of DRAGEN used for processing samples, defaults to {DRAGEN_VERSION}")
    return argparser.parse_args()


if __name__ == "__main__":
    args = get_args()
    sample_set = args.sample_set
    billing_project = args.billing_project
    workspace_name = args.workspace_name
    dry_run = args.dry_run
    filter_existing_ids = args.filter_existing_ids
    unique_id_field = args.unique_id_field
    table_name = args.table_name
    dataset_id = args.dataset_id
    ingest_waiting_time_poll = args.ingest_waiting_time_poll
    ingest_batch_size = args.ingest_batch_size
    bulk_mode = args.bulk_mode
    update_strategy = args.update_strategy
    dragen_version = args.dragen_version

    token = Token()
    request_util = RunRequest(token=token)
    terra_workspace = TerraWorkspace(
        request_util=request_util,
        billing_project=billing_project,
        workspace_name=workspace_name
    )

    tdr_sample_metadata = GetSampleInfo(
        sample_set=sample_set,
        terra_workspace=terra_workspace,
        dragen_version=dragen_version
    ).run()
    if dry_run:
        logging.info(f"Writing to ingest.tsv and not ingesting to dataset {dataset_id}")
        Csv(file_path="ingest.tsv", delimiter='\t').create_tsv_from_list_of_dicts(
            header_list=tdr_sample_metadata[0].keys(),
            list_of_dicts=tdr_sample_metadata,
        )
        exit(0)

    tdr_util = TDR(request_util=request_util)

    dataset_info = tdr_util.get_dataset_info(dataset_id).json()
    service_account = dataset_info['ingestServiceAccount']
    terra_workspace.update_user_acl(
        email=service_account,
        access_level='READER',
    )
    FilterAndBatchIngest(
        tdr=TDR(request_util=request_util),
        filter_existing_ids=filter_existing_ids,
        unique_id_field=unique_id_field,
        table_name=table_name,
        ingest_metadata=tdr_sample_metadata,
        dataset_id=dataset_id,
        ingest_waiting_time_poll=ingest_waiting_time_poll,
        ingest_batch_size=ingest_batch_size,
        bulk_mode=bulk_mode,
        update_strategy=update_strategy,
        load_tag=f"{dataset_id}.{table_name}"
    ).run()
