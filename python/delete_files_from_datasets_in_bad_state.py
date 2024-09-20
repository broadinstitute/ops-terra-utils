from utils.tdr_util import TDR, MonitorTDRJob
from utils.request_util import RunRequest
from utils.token_util import Token
from utils import GCP
from argparse import ArgumentParser, Namespace
import logging
import requests
import re

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(description="Delete files from datasets in bad state")
    parser.add_argument("-id", "--dataset_id", required=True)
    parser.add_argument("-l", "--file_query_limit", default=20000, type=int)
    return parser.parse_args()


class DeleteFilesFromDatasetsInBadState:
    TDR_LINK = "https://data.terra.bio/api/repository/v1"
    
    def __init__(self, request_util: RunRequest, dataset_id: str, tdr: TDR, limit: int):
        self.request_util = request_util
        self.dataset_id = dataset_id
        self.limit = limit
        self.tdr = tdr

    @staticmethod
    def get_file_uuid_from_request(request_json: dict) -> str:
        """Extract the file UUID from a request JSON response"""
        # Regular expression to match UUID format
        uuid_pattern = r'\b[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}\b'
        # Search for the first UUID in the text
        match = re.search(uuid_pattern, request_json['message'])
        return match.group(0)  # Return the matched UUID as a string

    def delete_file(self, file_id: str):
        logging.info(f"Could not list all file because of file, {file_id}, in bad state. Attempting to delete file.")
        job_id = self.tdr.delete_file(file_id=file_id, dataset_id=self.dataset_id)
        MonitorTDRJob(tdr=self.tdr, job_id=job_id, check_interval=10).run()

    def find_and_delete_files_in_bad_state(self):
        batch = 1
        offset = 0
        metadata: list = []
        while True:
            # Loop through all files in the dataset and delete any files in bad state
            logging.info(f"Retrieving {(batch - 1) * self.limit} to {batch * self.limit} records in metadata")
            uri = f"{self.TDR_LINK}/datasets/{self.dataset_id}/files?offset={offset}&limit={self.limit}"
            # Run the request outside the backoff decorator so can catch specific failures
            response = requests.get(
                uri,
                headers=self.request_util.create_headers(),
            )
            # Check if it is specific failure like
            # Directory entry refers to non-existent file (fileId = 180ccfb4-f2e8-4bbe-a264-74f3e7549fbd)
            if response.status_code == 500 and 'Directory entry refers to non-existent file' in response.text:
                file_uuid = self.get_file_uuid_from_request(response.json())
                self.delete_file(file_id=file_uuid)
                logging.info("Attempting to retrieve same batch again after deletion")

            # If there is a different status code or message does not include Directory entry refers to non-existent file
            elif 300 <= response.status_code or response.status_code < 200:
                logging.info("Failed to retrieve files with different status code " +
                             "then 500 and/or message did not include Directory entry refers to non-existent file")
                print(response.text)
                response.raise_for_status()

            # If no more files, break the loop
            elif not response:
                logging.info(f"No more results to retrieve, found {len(metadata)} total records")
                break

            # If there are more files to retrieve and no failures, extend the metadata list
            else:
                metadata.extend(response)
                # Increment the offset by limit for the next page
                offset += self.limit
                batch += 1
        return metadata


if __name__ == '__main__':
    args = get_args()
    dataset_id = args.dataset_id
    limit = args.file_query_limit

    token = Token(cloud=GCP)
    request_util = RunRequest(token=token, max_retries=1, max_backoff_time=10)
    tdr = TDR(request_util=request_util)

    all_files = DeleteFilesFromDatasetsInBadState(
        request_util=request_util,
        dataset_id=dataset_id,
        tdr=tdr,
        limit=limit
    ).find_and_delete_files_in_bad_state()
    logging.info(f"Found {len(all_files)} files in dataset {dataset_id} in good state")
