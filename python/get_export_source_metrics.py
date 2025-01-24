import logging
import os
from argparse import ArgumentParser, Namespace
import statistics


from utils.tdr_utils.tdr_api_utils import TDR
from utils.terra_utils.terra_util import TerraWorkspace
from utils.azure_utils import AzureBlobDetails
from utils.request_util import RunRequest
from utils.token_util import Token
from utils.csv_util import Csv
from utils.gcp_utils import GCPCloudFunctions


def get_args() -> Namespace:
    parser = ArgumentParser(
        description="""Perform validation and collect metrics for azure to gcp export""")
    parser.add_argument(
        "-f",
        "--input_tsv",
        required=True
    )
    parser.add_argument(
        "-t",
        "--target",
        required=True,
        choices=["workspace", "dataset"]
    )
    parser.add_argument(
        "-n",
        "--ticket_number",
        required=False
    )
    return parser.parse_args()


def human_readable_size(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def collect_file_size_metrics(file_dicts, size_key):
    list_of_file_sizes = [file[size_key] for file in file_dicts]
    largest_file = max(list_of_file_sizes)
    mean_file_size = statistics.mean(list_of_file_sizes)
    total_export_size = sum(list_of_file_sizes)
    return human_readable_size(largest_file), \
        human_readable_size(mean_file_size), \
        human_readable_size(total_export_size), \
        len(list_of_file_sizes)


def validate_export_buckets(csv_dicts, request_util):
    for row in csv_dicts:
        workspace_client = TerraWorkspace(request_util=request_util,
                                          billing_project=row['destination_billing_project'],
                                          workspace_name=row['destination_workspace_name'])
        workspace_bucket = workspace_client.get_workspace_bucket()
        if workspace_bucket != row['export_bucket']:
            logging.error(f"Export bucket {row['export_bucket']} does not match workspace bucket {workspace_bucket}")


if __name__ == "__main__":
    args = get_args()
    token = Token(cloud='gcp')
    request_util = RunRequest(token=token, max_retries=1)
    tdr_client = TDR(request_util=request_util)
    csv_dicts = Csv(file_path=args.input_tsv, delimiter='\t').create_list_of_dicts_from_tsv()
    validate_export_buckets(csv_dicts=csv_dicts, request_util=request_util)
    collected_size_metrics = []
    match args.target:
        case  "dataset":
            for row in csv_dicts:
                file_list = tdr_client.get_data_set_files(dataset_id=row['source_dataset_id'])
                largest_file, mean_file_size, total_export_size, number_of_files = collect_file_size_metrics(file_list, 'size')  # noqa: E501
                collected_size_metrics.append({'DATASET_ID': row['DATASET_ID'],
                                               'LARGEST_FILE_SIZE': largest_file,
                                               'MEAN_FILE_SIZE': mean_file_size,
                                               'TOTAL_EXPORT_SIZE': total_export_size,
                                               'FILE_COUNT': number_of_files})
        case "workspace":
            print('looping through input tsv')
            for row in csv_dicts:
                workspace_client = TerraWorkspace(request_util=request_util,
                                                  billing_project=row['source_billing_project'],
                                                  workspace_name=row['source_workspace_name'])
                workspace_client.set_azure_terra_variables()
                sas_token = workspace_client.retrieve_sas_token(2400)
                az_blob_client = AzureBlobDetails(
                    account_url=workspace_client.account_url,
                    sas_token=sas_token,
                    container_name=workspace_client.storage_container)
                az_blobs = az_blob_client.get_blob_details(max_per_page=1000)
                largest_file, mean_file_size, total_export_size, number_of_files = collect_file_size_metrics(az_blobs, 'size_in_bytes')  # noqa: E501
                collected_size_metrics.append({'DATASET_ID': row['DATASET_ID'],
                                               'LARGEST_FILE_SIZE': largest_file,
                                               'MEAN_FILE_SIZE': mean_file_size,
                                               'TOTAL_EXPORT_SIZE': total_export_size,
                                               'FILE_COUNT': number_of_files})
    report_path = f'{args.target}_metrics.csv'
    Csv(file_path=report_path, delimiter=',').create_tsv_from_list_of_dicts(collected_size_metrics)

    if args.ticket_number:
        project_id = os.environ.get("project_variable")
        secret_name = os.environ.get("secret_name")
        zendesk_token = GCPCloudFunctions().read_secret(project=project_id, secret_path=secret_name)
        # TODO: Add zendesk util to add comment / attachment to ticket once svc account created.
        print('will upload to zendesk ticket')
