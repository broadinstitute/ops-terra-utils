import logging
from argparse import ArgumentParser, Namespace
from ops_utils.token_util import Token
from ops_utils.request_util import RunRequest
from ops_utils.terra_util import TerraWorkspace

from python.dragen.dragen_utils import GetSampleInfo, CreateSampleTsv, SAMPLE_TSV


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(description='Query BQ table with GCS bucket object metadata inventory.')

    parser.add_argument('-g', '--gcp_project', type=str, help='Google project used for BigQuery.',
                        choices=['gp-cloud-dragen-dev', 'gp-cloud-dragen-prod', 'dsp-cloud-dragen-stanley'], required=True)
    parser.add_argument('-d', '--data_type', type=str, choices=['wgs', 'bge'],
                        help='bge assumes data was delivered by ops and sample_id concats multiple columns. wgs assumes sample id = collab_id',
                        required=True)
    parser.add_argument('-mi', '--min_start_date', type=str,
                        help='tasks created after this time. YYYY-MM-DD', default="2008-10-30")
    parser.add_argument('-ma', '--max_start_date', type=str,
                        help='tasks created before this time. YYYY-MM-DD', default="2035-10-30")
    parser.add_argument('-w', '--workspace_name', type=str, help='Terra workspace name', required=True)
    parser.add_argument('-b', '--billing_project', type=str, help='Terra billing project name', required=True)
    parser.add_argument("--service_account_json", "-saj", type=str,
                        help="Path to the service account JSON file. If not provided, will use the default credentials.")
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    gcp_project = args.gcp_project
    billing_project = args.billing_project
    workspace_name = args.workspace_name
    min_start_date = args.min_start_date
    max_start_date = args.max_start_date
    data_type = args.data_type
    service_account_json = args.service_account_json

    samples_dict = GetSampleInfo(
        google_project=gcp_project,
        maximum_run_date=max_start_date,
        minimum_run_date=min_start_date,
        data_type=data_type
    ).run()
    CreateSampleTsv(samples_dict=samples_dict, output_tsv=SAMPLE_TSV).create_tsv()
    token = Token(service_account_json=service_account_json)
    request_util = RunRequest(token=token)
    logging.info(f"Uploading {SAMPLE_TSV} to workspace {billing_project}/{workspace_name}")
    TerraWorkspace(
        billing_project=billing_project,
        workspace_name=workspace_name,
        request_util=request_util
    ).upload_metadata_to_workspace_table(entities_tsv=SAMPLE_TSV)
    logging.info(f"Finished uploading {SAMPLE_TSV} to workspace {billing_project}/{workspace_name}")
