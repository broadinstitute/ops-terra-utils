import csv 
from google.cloud import storage
import hashlib
import base64
import binascii
from ast import literal_eval
from argparse import ArgumentParser, Namespace
from utils.tdr_utils.tdr_api_utils import TDR
from utils.request_util import RunRequest
from utils.token_util import Token

def get_args() -> Namespace:
    parser = ArgumentParser(
        description="""validation of TDR file export to GCP bucket""")

    parser.add_argument("-id", "--dataset_id", required=True,
                        help="ID of dataset from export")
    parser.add_argument("-b", "--bucket_id", required=True,
                        help="Google bucket to check")
    parser.add_argument("-o", "--output_file", required=False,
                        default="~/dataset_export_validation.csv",
                        help="Output file for validation results")
    return parser.parse_args()




if __name__ == "__main__":
    args = get_args()
    token = Token(cloud='gcp')
    request_util = RunRequest(token=token)
    tdr_client = TDR(request_util=request_util)
    gcp_storage_client = storage.Client()
    gcp_bucket = gcp_storage_client.bucket(args.bucket_id)
    file_list = tdr_client.get_data_set_files(dataset_id=args.dataset_id)
    checks = []
    for row in file_list:
        target_blob = gcp_bucket.get_blob(row['path'].removeprefix('/'))
        # Transform GCP md5 hash to match TDR md5 checksum
        blob_converted_md5 = binascii.hexlify(base64.urlsafe_b64decode(target_blob.md5_hash)).decode()
        tdr_md5 = next(checksum['checksum'] for checksum in row['checksums'] if checksum['type'] == 'md5')
        sizes_match = target_blob.size == int(row['size'])
        
        check_dict = {
            "file": row['path'],
            "file_exists_in_gcp": target_blob.exists(),
            "file_sizes_match": sizes_match,
            "md5_match": tdr_md5 == blob_converted_md5
        }
        checks.append(check_dict)

    with open(args.output_file, 'w', newline='') as checks_file:
        writer = csv.DictWriter(checks_file, fieldnames=checks[0].keys())
        writer.writeheader()
        for check in checks:
            writer.writerow(check)