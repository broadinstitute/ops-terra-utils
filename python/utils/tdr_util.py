import json
import logging
import requests
import re
import time
import sys
import pandas as pd
import numpy as np
import pytz
from typing import Any, Optional, Union
from urllib.parse import unquote
from pydantic import ValidationError
from dateutil import parser
from dateutil.parser import ParserError
from datetime import datetime, date

from .request_util import GET, POST, DELETE
from .tdr_api_schema.create_dataset_schema import CreateDatasetSchema
from .tdr_api_schema.update_dataset_schema import UpdateSchema
from .terra_util import TerraWorkspace
from . import GCP, AZURE  # import from __init__.py


# Used when creating a new dataset
FILE_INVENTORY_DEFAULT_SCHEMA = {
    "tables": [
        {
            "name": "file_inventory",
            "columns": [
                {
                    "name": "name",
                    "datatype": "string",
                    "array_of": False,
                    "required": True
                },
                {
                    "name": "path",
                    "datatype": "string",
                    "array_of": False,
                    "required": True
                },
                {
                    "name": "content_type",
                    "datatype": "string",
                    "array_of": False,
                    "required": True
                },
                {
                    "name": "file_extension",
                    "datatype": "string",
                    "array_of": False,
                    "required": True
                },
                {
                    "name": "size_in_bytes",
                    "datatype": "integer",
                    "array_of": False,
                    "required": True
                },
                {
                    "name": "md5_hash",
                    "datatype": "string",
                    "array_of": False,
                    "required": True
                },
                {
                    "name": "file_ref",
                    "datatype": "fileref",
                    "array_of": False,
                    "required": True
                }
            ]
        }
    ]
}


class TDR:
    TDR_LINK = "https://data.terra.bio/api/repository/v1"

    def __init__(self, request_util: Any):
        self.request_util = request_util

    def get_data_set_files(self, dataset_id: str, batch_query: bool = True, limit: int = 1000) -> list[dict]:
        """Get all files in a dataset. Azure seems like it has issues with batch query, so set to false for now for
        Azure.

        Returns json like below:
        {
    "fileId": "cf198fcc-3564-46ad-b46f-8gbc3711a866",
    "collectionId": "0d1c9aea-e935-4d25-83c3-8675f6aa062a",
    "path": "/fc831123-5657-4c7d-b778-e30b4793321b/0000113c-e46c-4772-b2af-ef73a5c1aa32/SM-XXXXX.vcf.gz.md5sum",
    "size": 34,
    "checksums": [
        {
            "checksum": "g10e4e8e",
            "type": "crc32c"
        },
        {
            "checksum": "29bd10731cbfcf4cfabfff4cba063d9c",
            "type": "md5"
        }
    ],
    "created": "2024-07-27T17:26:09.724Z",
    "description": null,
    "fileType": "file",
    "fileDetail": {
        "datasetId": "0d1c9aea-e944-4d19-83c3-8675f6aa123a",
        "mimeType": null,
        "accessUrl": "gs://datarepo-34a4ac45-bucket/0d1c9aea-e944-4d19-83c3-8675f6aa062a/cf198fcc-3564-46ad-b73f-8bbc3711a866/SM-XXXXX.vcf.gz.md5sum",  # noqa: E501
        "loadTag": "0d1c9aea-e944-4d19-83c3-8675f6aa123a"
    },
    "directoryDetail": null
}
        """
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}/files"
        if batch_query:
            return self._get_response_from_batched_endpoint(uri=uri, limit=limit)
        else:
            return self.request_util.run_request(uri=f"{uri}", method=GET).json()

    def create_file_dict(self, dataset_id: str, limit: int = 1000) -> dict:
        """Create a dictionary of all files in a dataset where key is the file uuid."""
        return {
            file_dict['fileId']: file_dict
            for file_dict in self.get_data_set_files(dataset_id=dataset_id, limit=limit)
        }

    def get_sas_token(self, snapshot_id: str = "", dataset_id: str = "") -> dict:
        if snapshot_id:
            uri = f"{self.TDR_LINK}/snapshots/{snapshot_id}?include=ACCESS_INFORMATION"
            response = self.request_util.run_request(uri=uri, method=GET)
            snapshot_info = json.loads(response.text)
            sas_token = snapshot_info["accessInformation"]["parquet"]["sasToken"]
        elif dataset_id:
            uri = f"{self.TDR_LINK}/datasets/{dataset_id}?include=ACCESS_INFORMATION"
            response = self.request_util.run_request(uri=uri, method=GET)
            snapshot_info = json.loads(response.text)
            sas_token = snapshot_info["accessInformation"]["parquet"]["sasToken"]
        else:
            raise ValueError("Must provide either snapshot_id or dataset_id")

        sas_expiry_time_pattern = re.compile(r"se.+?(?=\&sp)")
        expiry_time_str = sas_expiry_time_pattern.search(sas_token)
        time_str = unquote(expiry_time_str.group()).replace("se=", "")  # type: ignore[union-attr]

        return {"sas_token": sas_token, "expiry_time": time_str}

    def delete_file(self, file_id: str, dataset_id: str) -> str:
        """Delete a file from a dataset. Return delete job id"""
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}/files/{file_id}"
        response = self.request_util.run_request(uri=uri, method=DELETE)
        job_id = json.loads(response.text)['id']
        logging.info(f"Submitted delete job {job_id} for file {file_id}")
        return job_id

    def delete_files(self, file_ids: list[str], dataset_id: str, batch_size_to_delete_files: int = 100) -> None:
        """Delete multiple files from a dataset in batches and monitor delete jobs until completion for each batch.
        Will submit batch of delete jobs, monitor all, and then proceed to next batch until all files are deleted."""
        logging.info(f"Deleting {len(file_ids)} files from dataset {dataset_id}")
        total_files = len(file_ids)
        job_ids = []

        # Process files in batches
        for i in range(0, total_files, batch_size_to_delete_files):
            current_batch = file_ids[i:i + batch_size_to_delete_files]
            logging.info(
                f"Submitting delete jobs for batch {i // batch_size_to_delete_files + 1} with {len(current_batch)} "
                f"files."
            )

            # Submit delete jobs for the current batch
            for file_id in current_batch:
                job_id = self.delete_file(file_id=file_id, dataset_id=dataset_id)
                job_ids.append(job_id)
            # Monitor delete jobs for the current batch
            logging.info(f"Monitoring {len(current_batch)} delete jobs in batch {i // batch_size_to_delete_files + 1}")
            for job_id in job_ids:
                MonitorTDRJob(tdr=self, job_id=job_id, check_interval=5).run()
            logging.info(
                f"Completed deletion for batch {i // batch_size_to_delete_files + 1} with {len(current_batch)} files."
            )

        logging.info(f"Successfully deleted {total_files} files from dataset {dataset_id}")

    def add_user_to_dataset(self, dataset_id: str, user: str, policy: str) -> None:
        """Add user to dataset."""
        if policy not in ["steward", "custodian", "snapshot_creator"]:
            raise ValueError(
                f"Policy {policy} is not valid. Must be READER, WRITER, or OWNER")
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}/policies/{policy}/members"
        member_dict = {"email": user}
        logging.info(f"Adding user {user} to dataset {dataset_id} with policy {policy}")
        self.request_util.run_request(
            uri=uri, method=POST, data=json.dumps(member_dict))

    def delete_dataset(self, dataset_id: str) -> None:
        """Delete dataset."""
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}"
        logging.info(f"Deleting dataset {dataset_id}")
        response = self.request_util.run_request(uri=uri, method=DELETE)
        job_id = response.json()['id']
        MonitorTDRJob(tdr=self, job_id=job_id, check_interval=30).run()

    def delete_snapshot(self, snapshot_id: str) -> None:
        """Delete snapshot."""
        uri = f"{self.TDR_LINK}/snapshots/{snapshot_id}"
        logging.info(f"Deleting snapshot {snapshot_id}")
        response = self.request_util.run_request(uri=uri, method=DELETE)
        job_id = response.json()['id']
        MonitorTDRJob(tdr=self, job_id=job_id, check_interval=30).run()

    def _yield_existing_datasets(
            self, filter: Optional[str] = None, batch_size: int = 100, direction: str = "asc"
    ) -> Any:
        """Get all datasets in TDR. Filter can be dataset name"""
        offset = 0
        if filter:
            filter_str = f"&filter={filter}"
        else:
            filter_str = ""
        while True:
            logging.info(f"Searching for datasets with filter {filter_str} in batches of {batch_size}")
            uri = f"{self.TDR_LINK}/datasets?offset={offset}&limit={batch_size}&sort=created_date&direction={direction}{filter_str}"  # noqa: E501
            response = self.request_util.run_request(uri=uri, method=GET)
            datasets = response.json()["items"]
            if not datasets:
                break
            for dataset in datasets:
                yield dataset
            offset += batch_size
            break

    def check_if_dataset_exists(self, dataset_name: str, billing_profile: Optional[str]) -> list[dict]:
        matching_datasets = []
        # If exists then get dataset id
        for dataset in self._yield_existing_datasets(filter=dataset_name):
            if billing_profile:
                if dataset["defaultProfileId"] == billing_profile:
                    logging.info(f"Dataset {dataset['name']} already exists under billing profile {billing_profile}")
                    dataset_id = dataset["id"]
                    logging.info(f"Dataset ID: {dataset_id}")
                    matching_datasets.append(dataset)
                else:
                    logging.warning(
                        f"Dataset {dataset['name']} exists but is under {dataset['defaultProfileId']} " +
                        f"and not under billing profile {billing_profile}"
                    )
            else:
                matching_datasets.append(dataset)
        return matching_datasets

    def get_dataset_info(self, dataset_id: str, info_to_include: Optional[list[str]] = None) -> dict:
        """Get dataset info"""
        acceptable_include_info = [
            "SCHEMA",
            "ACCESS_INFORMATION",
            "PROFILE",
            "PROPERTIES",
            "DATA_PROJECT",
            "STORAGE",
            "SNAPSHOT_BUILDER_SETTING"
        ]
        if info_to_include:
            if not all(info in acceptable_include_info for info in info_to_include):
                raise ValueError(f"info_to_include must be a subset of {acceptable_include_info}")
            include_string = '&include='.join(info_to_include)
        else:
            include_string = ""
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}?include={include_string}"
        response = self.request_util.run_request(uri=uri, method=GET)
        return json.loads(response.text)

    def get_table_schema_info(self, dataset_id: str, table_name: str) -> Union[dict, None]:
        """get schema information on one table within dataset"""
        dataset_info = self.get_dataset_info(dataset_id=dataset_id, info_to_include=["SCHEMA"])
        for table in dataset_info["schema"]["tables"]:
            if table["name"] == table_name:
                return table
        return None

    def get_job_result(self, job_id: str) -> dict:
        """retrieveJobResult"""
        uri = f"{self.TDR_LINK}/jobs/{job_id}/result"
        response = self.request_util.run_request(uri=uri, method=GET)
        return json.loads(response.text)

    def ingest_dataset(self, dataset_id: str, data: dict) -> dict:
        """Load data into TDR with ingestDataset."""
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}/ingest"
        response = self.request_util.run_request(
            uri=uri,
            method=POST,
            content_type="application/json",
            data=data
        )
        return json.loads(response.text)

    def get_data_set_table_metrics(
            self, dataset_id: str, target_table_name: str, query_limit: int = 1000
    ) -> list[dict]:
        """Use yield data_set_metrics and get all metrics returned in one list"""
        return [
            metric
            for metric in self._yield_data_set_metrics(
                dataset_id=dataset_id,
                target_table_name=target_table_name,
                query_limit=query_limit
            )
        ]

    def _yield_data_set_metrics(self, dataset_id: str, target_table_name: str, query_limit: int = 1000) -> Any:
        """Yield all entity metrics from dataset."""
        search_request = {
            "offset": 0,
            "limit": query_limit,
            "sort": "datarepo_row_id"
        }
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}/data/{target_table_name}"
        while True:
            batch_number = int((search_request["offset"] / query_limit)) + 1  # type: ignore[operator]
            response = self.request_util.run_request(
                uri=uri,
                method=POST,
                content_type="application/json",
                data=json.dumps(search_request)
            )
            if not response or not response.json()["result"]:
                break
            logging.info(
                f"""Downloading batch {batch_number} of max {query_limit} records from {target_table_name} table in
                dataset {dataset_id}"""
            )
            for record in response.json()["result"]:
                yield record
            search_request["offset"] += query_limit  # type: ignore[operator]

    def get_data_set_sample_ids(self, dataset_id: str, target_table_name: str, entity_id: str) -> list[str]:
        """Get existing ids from dataset."""
        data_set_metadata = self._yield_data_set_metrics(dataset_id=dataset_id, target_table_name=target_table_name)
        return [str(sample_dict[entity_id]) for sample_dict in data_set_metadata]

    def get_job_status(self, job_id: str) -> requests.Response:
        """retrieveJobStatus"""
        # first check job status - retrieveJob
        uri = f"{self.TDR_LINK}/jobs/{job_id}"
        response = self.request_util.run_request(uri=uri, method=GET)
        return response

    def get_data_set_file_uuids_from_metadata(self, dataset_id: str) -> list[str]:
        data_set_info = self.get_dataset_info(dataset_id=dataset_id, info_to_include=["SCHEMA"])
        all_metadata_file_uuids = []
        tables = 0
        for table in data_set_info["schema"]["tables"]:
            tables += 1
            table_name = table["name"]
            logging.info(f"Getting all file information for {table_name}")
            # Get just columns where datatype is fileref
            file_columns = [column["name"] for column in table["columns"] if column["datatype"] == "fileref"]
            data_set_metrics = self.get_data_set_table_metrics(dataset_id=dataset_id, target_table_name=table_name)
            # Get unique list of file uuids
            file_uuids = list(
                set(
                    [
                        value for metric in data_set_metrics for key, value in metric.items() if key in file_columns
                    ]
                )
            )
            logging.info(f"Got {len(file_uuids)} file uuids from table '{table_name}'")
            all_metadata_file_uuids.extend(file_uuids)
            # Make full list unique
            all_metadata_file_uuids = list(set(all_metadata_file_uuids))
        logging.info(f"Got {len(all_metadata_file_uuids)} file uuids from {tables} total table(s)")
        return all_metadata_file_uuids

    def get_or_create_dataset(
            self,
            dataset_name: str,
            billing_profile: str,
            schema: dict,
            description: str,
            cloud_platform: str,
            additional_properties_dict: Optional[dict] = None
    ) -> str:
        existing_data_sets = self.check_if_dataset_exists(dataset_name, billing_profile)
        if existing_data_sets:
            if len(existing_data_sets) > 1:
                raise ValueError(
                    f"Multiple datasets found with name {dataset_name} under billing_profile: "
                    f"{json.dumps(existing_data_sets, indent=4)}"
                )

            dataset_id = existing_data_sets[0]["id"]
        if not existing_data_sets:
            logging.info("Did not find existing dataset")
            # Create dataset
            dataset_id = self.create_dataset(
                schema=schema,
                cloud_platform=cloud_platform,
                dataset_name=dataset_name,
                description=description,
                profile_id=billing_profile,
                additional_dataset_properties=additional_properties_dict
            )
        return dataset_id

    def create_dataset(  # type: ignore[return]
            self,
            schema: dict,
            cloud_platform: str,
            dataset_name: str,
            description: str,
            profile_id: str,
            additional_dataset_properties: Optional[dict] = None
    ) -> Optional[str]:

        dataset_properties = {
            "name": dataset_name,
            "description": description,
            "defaultProfileId": profile_id,
            "region": "us-central1",
            "cloudPlatform": cloud_platform,
            "schema": schema
        }

        if additional_dataset_properties:
            dataset_properties.update(additional_dataset_properties)
        try:
            CreateDatasetSchema(**dataset_properties)
        except ValidationError as e:
            raise ValueError(f"Schema validation error: {e}")
        uri = f"{self.TDR_LINK}/datasets"
        logging.info(f"Creating dataset {dataset_name} under billing profile {profile_id}")
        response = self.request_util.run_request(
            method=POST,
            uri=uri,
            data=json.dumps(dataset_properties),
            content_type="application/json"
        )
        job_id = response.json()["id"]
        completed = MonitorTDRJob(tdr=self, job_id=job_id, check_interval=30).run()
        if completed:
            dataset_id = self.get_job_result(job_id)["id"]
            logging.info(f"Successfully created dataset {dataset_name}: {dataset_id}")
            return dataset_id

    def update_dataset_schema(  # type: ignore[return]
            self, dataset_id: str,
            update_note: str,
            tables_to_add: Optional[list[dict]] = None,
            relationships_to_add: Optional[list[dict]] = None,
            columns_to_add: Optional[list[dict]] = None
    ) -> Optional[str]:
        """Update dataset schema."""
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}/updateSchema"
        request_body: dict = {"description": f"{update_note}", "changes": {}}
        if tables_to_add:
            request_body["changes"]["addTables"] = tables_to_add
        if relationships_to_add:
            request_body["changes"]["addRelationships"] = relationships_to_add
        if columns_to_add:
            request_body["changes"]["addColumns"] = columns_to_add
        try:
            UpdateSchema(**request_body)
        except ValidationError as e:
            raise ValueError(f"Schema validation error: {e}")

        response = self.request_util.run_request(
            uri=uri,
            method=POST,
            content_type="application/json",
            data=json.dumps(request_body)
        )
        job_id = response.json()["id"]
        completed = MonitorTDRJob(tdr=self, job_id=job_id, check_interval=30).run()
        if completed:
            dataset_id = self.get_job_result(job_id)["id"]
            logging.info(f"Successfully ran schema updates in dataset {dataset_id}")
            return dataset_id

    def _get_response_from_batched_endpoint(self, uri: str, limit: int = 1000) -> list[dict]:
        """Helper method for all GET endpoints that require batching. Given the URI and the limit (optional), will
        loop through batches until all metadata is retrieved. NOTE: when providing the URI, provide only the BASE
        URI (i.e. without the query params for offset or limit)."""
        batch = 1
        offset = 0
        metadata: list = []
        while True:
            logging.info(f"Retrieving {(batch - 1) * limit} to {batch * limit} records in metadata")
            response = self.request_util.run_request(uri=f"{uri}?offset={offset}&limit={limit}", method=GET).json()

            # If no more files, break the loop
            if not response:
                logging.info(
                    f"No more results to retrieve, found {len(metadata)} total records")
                break

            metadata.extend(response)
            # Increment the offset by limit for the next page
            offset += limit
            batch += 1
        return metadata

    def get_files_from_snapshot(self, snapshot_id: str, limit: int = 1000) -> list[dict]:
        """Returns all the metadata about files in a given snapshot. Not all files can be returned at once, so the API
        is used repeatedly until all "batches" have been returned"""
        uri = f"{self.TDR_LINK}/snapshots/{snapshot_id}/files"
        return self._get_response_from_batched_endpoint(uri=uri, limit=limit)


class MonitorTDRJob:
    def __init__(self, tdr: TDR, job_id: str, check_interval: int):
        self.tdr = tdr
        self.job_id = job_id
        self.check_interval = check_interval

    def run(self) -> bool:
        """Monitor ingest until completion."""
        while True:
            ingest_response = self.tdr.get_job_status(self.job_id)
            if ingest_response.status_code == 202:
                logging.info(f"TDR job {self.job_id} is still running")
                # Check every x seconds if ingest is still running
                time.sleep(self.check_interval)
            elif ingest_response.status_code == 200:
                response_json = json.loads(ingest_response.text)
                if response_json["job_status"] == "succeeded":
                    logging.info(f"TDR job {self.job_id} succeeded")
                    return True
                else:
                    logging.error(f"TDR job {self.job_id} failed")
                    job_result = self.tdr.get_job_result(self.job_id)
                    raise ValueError(
                        f"Status code {ingest_response.status_code}: {response_json}\n{job_result}")
            else:
                logging.error(f"TDR job {self.job_id} failed")
                job_result = self.tdr.get_job_result(self.job_id)
                raise ValueError(
                    f"Status code {ingest_response.status_code}: {ingest_response.text}\n{job_result}")


class StartAndMonitorIngest:
    def __init__(
            self, tdr: TDR,
            ingest_records: list[dict],
            target_table_name: str,
            dataset_id: str,
            load_tag: str,
            bulk_mode: bool,
            update_strategy: str,
            waiting_time_to_poll: int
    ):
        self.tdr = tdr
        self.ingest_records = ingest_records
        self.target_table_name = target_table_name
        self.dataset_id = dataset_id
        self.load_tag = load_tag
        self.bulk_mode = bulk_mode
        self.update_strategy = update_strategy
        self.waiting_time_to_poll = waiting_time_to_poll

    def _create_ingest_dataset_request(self) -> Any:
        """Create the ingestDataset request body."""
        # https://support.terra.bio/hc/en-us/articles/23460453585819-How-to-ingest-and-update-TDR-data-with-APIs
        load_dict = {
            "format": "array",
            "records": self.ingest_records,
            "table": self.target_table_name,
            "resolve_existing_files": "true",
            "updateStrategy": self.update_strategy,
            "load_tag": self.load_tag,
            "bulkMode": "true" if self.bulk_mode else "false"
        }
        return json.dumps(load_dict)  # dict -> json

    def run(self) -> None:
        ingest_request = self._create_ingest_dataset_request()
        logging.info(f"Starting ingest to {self.dataset_id}")
        ingest_response = self.tdr.ingest_dataset(dataset_id=self.dataset_id, data=ingest_request)
        MonitorTDRJob(
            tdr=self.tdr,
            job_id=ingest_response["id"],
            check_interval=self.waiting_time_to_poll
        ).run()


class ReformatMetricsForIngest:
    """Reformat metrics for ingest.
    If file_list is True, then it is a list of file paths and formats differently assumes input json for that will be
    like below or similar for azure:
    {
                "file_name": blob.name,
                "file_path": f"gs://{self.bucket_name}/{blob.name}",
                "content_type": blob.content_type or guess_type(blob.name)[0] or "application/octet-stream",
                "file_extension": os.path.splitext(blob.name)[1],
                "size_in_bytes": blob.size,
                "md5_hash": blob.md5_hash
            }
    """

    def __init__(
            self,
            ingest_metadata: list[dict],
            cloud_type: str,
            storage_container: Optional[str] = None,
            sas_token_string: Optional[str] = None,
            file_list: bool = False,
            dest_file_path_flat: bool = False,
            file_to_uuid_dict: Optional[dict] = None,
            schema_info: Optional[dict] = None
    ):
        self.file_list = file_list
        self.ingest_metadata = ingest_metadata
        self.cloud_type = cloud_type
        self.sas_token_string = sas_token_string
        self.file_prefix = {GCP: "gs://", AZURE: "https://"}[cloud_type]
        self.workspace_storage_container = storage_container
        self.dest_file_path_flat = dest_file_path_flat
        self.file_to_uuid_dict = file_to_uuid_dict
        # Used if you want to provide schema info for tables to make sure values match.
        # Should be dict with key being column name and value being dict with datatype
        self.schema_info = schema_info

    def _add_file_ref(self, file_details: dict) -> None:
        """Create file ref for ingest."""
        file_details["file_ref"] = {
            "sourcePath": file_details["file_path"],
            # https://some_url.blob.core.windows.net/container_name/dir/file.txt
            # Remove url and container name with. Result will be /dir/file.txt
            "targetPath": self._format_relative_tdr_path(file_details["file_path"]),
            "description": f"Ingest of {file_details['file_path']}",
            "mimeType": file_details["content_type"]
        }

    def _format_relative_tdr_path(self, cloud_path: str) -> str:
        """Format cloud path to TDR path"""
        if self.cloud_type == GCP:
            # Cloud path will be gs://bucket/path/to/file convert to /path/to/file
            relative_path = "/".join(cloud_path.split("/")[3:])
        else:
            # Cloud path will be https://landing_zone/storage_account/path/to/file convert to /path/to/file
            split_path = cloud_path.split("/")
            file_path_storage_container = split_path[3]
            if file_path_storage_container != self.workspace_storage_container:
                raise ValueError(
                    f"{cloud_path} storage container {file_path_storage_container} does not match workspace storage "
                    f"container {self.workspace_storage_container}. SAS token will not work"
                )
            relative_path = "/".join(split_path[4:])
        if self.dest_file_path_flat:
            return "/" + relative_path.replace(
                "/", "_"
            ).replace("#", "").replace("?", "")
        else:
            # Target paths in TDR must start with a leading slash
            return f"/{relative_path}"

    def _check_and_format_file_path(self, column_value: str) -> tuple[Any, bool]:
        """Check if column value is a gs:// path and reformat to TDR's dataset relative path. if file_to_uuid_dict is
        provided then it will add existing uuid. If file_to_uuid_dict provided and file not found then will warn and
        return None"""
        valid = True
        if isinstance(column_value, str):
            # If it is a file path then reformat to TDR's dataset relative path
            if column_value.startswith(self.file_prefix):
                # If file to uuid provided then get uuid there
                if self.file_to_uuid_dict:
                    uuid = self.file_to_uuid_dict.get(column_value)
                    if uuid:
                        column_value = uuid
                    else:
                        logging.warning(
                            f"File {column_value} not found in file_to_uuid_dict, which should include all files "
                            f"in dataset."
                        )
                        column_value = None  # type: ignore[assignment]
                        valid = False
                else:
                    # If azure sas token will be '?{sas_token}', if gcp it just be file path
                    source_dest_mapping = {
                        "sourcePath": f"{column_value}{self.sas_token_string}" if self.cloud_type == AZURE else column_value,  # noqa: E501
                        "targetPath": self._format_relative_tdr_path(column_value)
                    }
                    return source_dest_mapping, valid
        return column_value, valid

    def _validate_and_update_column_for_schema(self, column_name: str, column_value: Any) -> tuple[str, bool]:
        """Check if column matches what schema expects and attempt to update if not. Changes to string at the end"""
        valid = True
        if self.schema_info:
            if column_name in self.schema_info.keys():
                expected_data_type = self.schema_info[column_name]["datatype"]
                if expected_data_type == "string" and not isinstance(column_value, str):
                    try:
                        column_value = str(column_value)
                    except ValueError:
                        logging.warning(f"Column {column_name} with value {column_value} is not a string")
                        valid = False
                if expected_data_type in ["int64", "integer"] and not isinstance(column_value, int):
                    try:
                        column_value = int(column_value)
                    except ValueError:
                        logging.warning(f"Column {column_name} with value {column_value} is not an integer")
                        valid = False
                if expected_data_type == "float64" and not isinstance(column_value, float):
                    try:
                        column_value = float(column_value)
                    except ValueError:
                        logging.warning(f"Column {column_name} with value {column_value} is not a float")
                        valid = False
                if expected_data_type == "boolean" and not isinstance(column_value, bool):
                    try:
                        column_value = bool(column_value)
                    except ValueError:
                        logging.warning(f"Column {column_name} with value {column_value} is not a boolean")
                        valid = False
                if expected_data_type in ["datetime", "date", "time"] and not isinstance(column_value, datetime):
                    try:
                        column_value = parser.parse(column_value)
                    except ValueError:
                        logging.warning(f"Column {column_name} with value {column_value} is not a datetime")
                        valid = False
                if expected_data_type == "array" and not isinstance(column_value, list):
                    valid = False
                    logging.warning(f"Column {column_name} with value {column_value} is not a list")
                if expected_data_type == "bytes" and not isinstance(column_value, bytes):
                    valid = False
                    logging.warning(f"Column {column_name} with value {column_value} is not bytes")
                if expected_data_type == "fileref" and column_value.startswith(self.file_prefix):
                    valid = False
                    logging.warning(f"Column {column_name} with value {column_value} is not a file path")
        # Ingest should be able to convert from string to correct format
        return str(column_value), valid

    def _reformat_metric(self, row_dict: dict) -> Optional[dict]:
        """Reformat metric for ingest."""
        reformatted_dict = {}
        # Set to make sure row valid and should be included
        row_valid = True
        #  If a specific file list is provided, then add file ref. Different than all other ingests
        if self.file_list:
            self._add_file_ref(row_dict)
            reformatted_dict = row_dict
        else:
            # Go through each value in row and reformat if needed
            for key, value in row_dict.items():
                # Ignore where there is no value
                if value:
                    # If schema info passed in then check if column matches what
                    # schema expect and attempt to update if not
                    if self.schema_info:
                        value, valid = self._validate_and_update_column_for_schema(key, value)
                        if not valid:
                            row_valid = False
                    # If it is a list go through each item and recreate items in list
                    if isinstance(value, list):
                        updated_value_list = []
                        for item in value:
                            update_value, valid = self._check_and_format_file_path(item)
                            if not valid:
                                row_valid = False
                            updated_value_list.append(update_value)
                        reformatted_dict[key] = updated_value_list
                    update_value, valid = self._check_and_format_file_path(value)
                    if not valid:
                        row_valid = False
                    reformatted_dict[key] = update_value
        # add in timestamp
        reformatted_dict["last_modified_date"] = datetime.now(
            tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S")
        # Only return if file list or row is valid
        if row_valid:
            return reformatted_dict
        else:
            logging.info(f"Row {json.dumps(row_dict, indent=4)} not valid and will not be included in ingest")
            return None

    def run(self) -> list[dict]:
        reformatted_metrics = []
        # Do not do list comprehension as we need to check if row is valid based on what is returned
        for row_dict in self.ingest_metadata:
            reformatted_row = self._reformat_metric(row_dict)
            if reformatted_row:
                reformatted_metrics.append(reformatted_row)
        return reformatted_metrics


class SetUpTDRTables:
    """dict of dicts containing table info list;
    expected columns are table_name, primary_key,
    ingest metadata, table_unique_id and key should be table name"""

    def __init__(self, tdr: TDR, dataset_id: str, table_info_dict: dict):
        self.tdr = tdr
        self.dataset_id = dataset_id
        self.table_info_dict = table_info_dict

    @staticmethod
    def _compare_table(reference_dataset_table: dict, target_dataset_table: list[dict], table_name: str) -> list[dict]:
        """Compare tables between two datasets."""
        logging.info(f"Comparing table {reference_dataset_table['name']} to existing target table")
        columns_to_update = []
        # Convert target table to dict for easier comparison
        target_dataset_table_dict = {
            col["name"]: col for col in target_dataset_table}
        # Go through each column in reference table and see if it exists and if so, is it the same in target table
        for column_dict in reference_dataset_table["columns"]:
            # Check if column exists in target table already
            if column_dict["name"] not in target_dataset_table_dict.keys():
                column_dict["action"] = "add"
                columns_to_update.append(column_dict)
            else:
                # Terrible way of checking, but file_inventory has file paths set as a string so we can see full path
                # and the class to assume what a column should be see the google cloud path and assumes it's a file ref.
                # Skipping check of this specific table for that reason
                if table_name != "file_inventory":
                    # Check if column exists but is not set up the same
                    if column_dict != target_dataset_table_dict[column_dict["name"]]:
                        column_dict["action"] = "modify"
                        columns_to_update.append(column_dict)
        return columns_to_update

    @staticmethod
    def _compare_dataset_relationships(
            reference_dataset_relationships: dict, target_dataset_relationships: list
    ) -> list[dict]:
        dataset_relationships_to_modify = []
        for dataset in reference_dataset_relationships:
            if dataset not in target_dataset_relationships:
                dataset_relationships_to_modify.append(dataset)
        return dataset_relationships_to_modify

    def run(self) -> dict:
        data_set_info = self.tdr.get_dataset_info(dataset_id=self.dataset_id, info_to_include=['SCHEMA'])
        existing_tdr_table_schema_info = {
            table_dict["name"]: table_dict["columns"]
            for table_dict in data_set_info["schema"]["tables"]
        }
        tables_to_create = []
        valid = True
        # Loop through all expected tables to see if exist and match schema. If not then create one.
        for ingest_table_name, ingest_table_dict in self.table_info_dict.items():
            # Get TDR schema info for tables to ingest
            expected_tdr_schema_dict = InferTDRSchema(
                input_metadata=ingest_table_dict["ingest_metadata"],
                table_name=ingest_table_name
            ).infer_schema()

            # If unique id then add to table json
            if ingest_table_dict.get("primary_key"):
                expected_tdr_schema_dict["primaryKey"] = [ingest_table_dict["primary_key"]]

            # add table to ones to create if it does not exist
            if ingest_table_name not in existing_tdr_table_schema_info:
                tables_to_create.append(expected_tdr_schema_dict)
            else:
                # Compare columns
                columns_to_update = self._compare_table(
                    reference_dataset_table=expected_tdr_schema_dict,
                    target_dataset_table=existing_tdr_table_schema_info[ingest_table_name],
                    table_name=ingest_table_name
                )
                if columns_to_update:
                    # If any updates needed nothing is done for whole ingest
                    valid = False
                    for column_to_update_dict in columns_to_update:
                        logging.warning(
                            f"Columns needs updates in {ingest_table_name}: "
                            f"{json.dumps(column_to_update_dict, indent=4)}"
                        )
                else:
                    logging.info(f"Table {ingest_table_name} exists and is up to date")
        if valid:
            #  Does nothing with relationships for now
            if tables_to_create:
                tables_string = ", ".join(
                    [table["name"] for table in tables_to_create]
                )
                logging.info(f"Table(s) {tables_string} do not exist in dataset. Will attempt to create")
                self.tdr.update_dataset_schema(
                    dataset_id=self.dataset_id,
                    update_note=f"Creating tables in dataset {self.dataset_id}",
                    tables_to_add=tables_to_create
                )
            else:
                logging.info("All tables in dataset exist and are up to date")
            # Return schema info for all existing tables after creation
            data_set_info = self.tdr.get_dataset_info(
                dataset_id=self.dataset_id, info_to_include=["SCHEMA"])
            # Return dict with key being table name and value being dict of columns with key being
            # column name and value being column info
            return {
                table_dict["name"]: {
                    column_dict["name"]: column_dict
                    for column_dict in table_dict["columns"]
                }
                for table_dict in data_set_info["schema"]["tables"]
            }
        else:
            logging.error("Tables need manual updating. Exiting")
            sys.exit(1)


class BatchIngest:
    def __init__(
            self,
            ingest_metadata: list[dict],
            tdr: TDR, target_table_name: str,
            dataset_id: str,
            batch_size: int,
            file_list_bool: bool,
            bulk_mode: bool,
            cloud_type: str,
            terra_workspace: Optional[TerraWorkspace] = None,
            update_strategy: str = "replace",
            waiting_time_to_poll: int = 60,
            sas_expire_in_secs: int = 3600,
            test_ingest: bool = False,
            load_tag: Optional[str] = None,
            dest_file_path_flat: bool = False,
            file_to_uuid_dict: Optional[dict] = None,
            schema_info: Optional[dict] = None,
            skip_reformat: bool = False
    ):
        self.ingest_metadata = ingest_metadata
        self.tdr = tdr
        self.target_table_name = target_table_name
        self.dataset_id = dataset_id
        self.cloud_type = cloud_type
        # terra_workspace only used if ingesting Azure data where you need to create sas tokens from workspace
        self.terra_workspace = terra_workspace
        self.batch_size = batch_size
        self.update_strategy = update_strategy
        self.bulk_mode = bulk_mode
        self.waiting_time_to_poll = waiting_time_to_poll
        self.sas_expire_in_secs = sas_expire_in_secs
        # Used if you want to run first batch and then exit after success
        self.test_ingest = test_ingest
        self.load_tag = load_tag
        self.file_list_bool = file_list_bool
        self.dest_file_path_flat = dest_file_path_flat
        self.file_to_uuid_dict = file_to_uuid_dict
        # Used if you want to provide schema info for tables to make sure values match.
        # Should be dict with key being column name and value being dict with datatype
        self.schema_info = schema_info
        # Use if input is already formatted correctly for ingest
        self.skip_reformat = skip_reformat

    def _reformat_metadata(self, metrics_batch: list[dict]) -> list[dict]:
        if self.cloud_type == AZURE:
            sas_token = self.terra_workspace.retrieve_sas_token(  # type: ignore[union-attr]
                sas_expiration_in_secs=self.sas_expire_in_secs)
            cloud_container = self.terra_workspace.storage_container  # type: ignore[union-attr]
            return ReformatMetricsForIngest(
                ingest_metadata=metrics_batch,
                cloud_type=self.cloud_type,
                storage_container=cloud_container,
                sas_token_string=sas_token,
                file_list=self.file_list_bool,
                dest_file_path_flat=self.dest_file_path_flat,
                file_to_uuid_dict=self.file_to_uuid_dict,
                schema_info=self.schema_info
            ).run()
        elif self.cloud_type == GCP:
            return ReformatMetricsForIngest(
                ingest_metadata=metrics_batch,
                cloud_type=self.cloud_type,
                file_list=self.file_list_bool,
                dest_file_path_flat=self.dest_file_path_flat,
                file_to_uuid_dict=self.file_to_uuid_dict,
                schema_info=self.schema_info
            ).run()
        else:
            raise ValueError(f"Cloud type {self.cloud_type} not supported. Must be {GCP} or {AZURE}")

    def run(self) -> None:
        logging.info(
            f"Batching {len(self.ingest_metadata)} total rows into batches of {self.batch_size} for ingest")
        total_batches = len(self.ingest_metadata) // self.batch_size + 1
        for i in range(0, len(self.ingest_metadata), self.batch_size):
            batch_number = i // self.batch_size + 1
            logging.info(f"Starting ingest batch {batch_number} of {total_batches} into table {self.target_table_name}")
            metrics_batch = self.ingest_metadata[i:i + self.batch_size]
            if self.skip_reformat:
                reformatted_batch = metrics_batch
            else:
                reformatted_batch = self._reformat_metadata(metrics_batch)

            if self.load_tag:
                load_tag = self.load_tag
            else:
                load_tag = f"{self.dataset_id}.{self.target_table_name}"
            # Start actual ingest
            if reformatted_batch:
                StartAndMonitorIngest(
                    tdr=self.tdr,
                    ingest_records=reformatted_batch,
                    target_table_name=self.target_table_name,
                    dataset_id=self.dataset_id,
                    load_tag=load_tag,
                    bulk_mode=self.bulk_mode,
                    update_strategy=self.update_strategy,
                    waiting_time_to_poll=self.waiting_time_to_poll
                ).run()
                logging.info(f"Completed batch ingest of {len(reformatted_batch)} rows")
                if self.test_ingest:
                    logging.info("First batch completed, exiting since test_ingest was used")
                    sys.exit(0)
            else:
                logging.info("No rows to ingest in this batch after reformatting")
        logging.info("Whole Ingest completed")


class ConvertTerraTableInfoForIngest:
    """Converts each row of table metadata into a dictionary that can be ingested into TDR.

    Input looks like
[{
  "attributes": {
    "some_metric": 99.99,
    "some_file_path": "gs://path/to/file",
    "something_to_exclude": "exclude_me"
  },
  "entityType": "sample",
  "name": "SM-MVVVV"
}]

converts to

[{
  "sample_id": "SM-MVVVV",
  "some_metric": 99.99,
  "some_file_path": "gs://path/to/file"
}]
"""

    def __init__(self, table_metadata: list[dict], tdr_row_id: str = 'sample_id', columns_to_ignore: list[str] = []):
        self.table_metadata = table_metadata
        self.tdr_row_id = tdr_row_id
        self.columns_to_ignore = columns_to_ignore

    def run(self) -> list[dict]:
        return [
            {
                self.tdr_row_id: row["name"],
                **{k: v for k, v in row["attributes"].items() if k not in self.columns_to_ignore}
            }
            for row in self.table_metadata
        ]


class InferTDRSchema:
    PYTHON_TDR_DATA_TYPE_MAPPING = {
        str: "string",
        "fileref": "fileref",
        bool: "boolean",
        bytes: "bytes",
        date: "date",
        datetime: "datetime",
        float: "float64",
        np.float64: "float64",
        int: "int64",
        np.int64: "int64",
        time: "time",
    }

    def __init__(self, input_metadata: list[dict], table_name: str):
        self.input_metadata = input_metadata
        self.table_name = table_name

    @staticmethod
    def _check_type_consistency(key_value_type_mappings: dict) -> None:
        """Receives a dictionary where the key is the header, and the value is a list of values
        for the header. Checks if all values for a given header are of the same type. Doesn't return anything, but
        raises an Exception if types do not match and provides the headers with types that are different"""
        matching = []

        for header, values_for_header in key_value_type_mappings.items():
            # find one value that's non-none to get the type to check against
            type_to_match_against = type(
                [v for v in values_for_header if v][0])

            # check if all the values in the list that are non-none match the type of the first entry
            all_values_matching = all(  # noqa: E721
                type(v) == type_to_match_against for v in values_for_header if v is not None)
            matching.append({header: all_values_matching})

        # Returns true if all headers are determined to be "matching"
        problematic_headers = [
            d.keys()
            for d in matching
            if not list(d.values())[0]
        ]

        if problematic_headers:
            raise Exception(
                f"Not all values for the following headers are of the same type: {problematic_headers}")

    def _python_type_to_tdr_type_conversion(self, value_for_header: Any) -> str:
        az_filref_regex = "^https.*sc-.*"
        gcp_fileref_regex = "^gs://.*"

        # Find potential file references
        if isinstance(value_for_header, str):
            az_match = re.search(pattern=az_filref_regex,
                                 string=value_for_header)
            gcp_match = re.search(
                pattern=gcp_fileref_regex, string=value_for_header)
            if az_match or gcp_match:
                return self.PYTHON_TDR_DATA_TYPE_MAPPING["fileref"]

        # Try to parse times and dates
        try:
            date_or_time = parser.parse(value_for_header)
            return self.PYTHON_TDR_DATA_TYPE_MAPPING[type(date_or_time)]
        except (TypeError, ParserError):
            pass

        if isinstance(value_for_header, list):
            # check for potential list of filerefs
            for v in value_for_header:
                if isinstance(v, str):
                    az_match = re.search(pattern=az_filref_regex, string=v)
                    gcp_match = re.search(pattern=gcp_fileref_regex, string=v)
                    if az_match or gcp_match:
                        return self.PYTHON_TDR_DATA_TYPE_MAPPING["fileref"]

            non_none_entry_in_list = [a for a in value_for_header if a][0]
            return self.PYTHON_TDR_DATA_TYPE_MAPPING[type(non_none_entry_in_list)]

        # if none of the above special cases apply, just pass the type of the value to determine the TDR type
        return self.PYTHON_TDR_DATA_TYPE_MAPPING[type(value_for_header)]

    def _format_column_metadata(self, key_value_type_mappings: dict) -> list[dict]:
        """Generates the metadata for each column's header name, data type, and whether it's an array of values"""
        columns = []

        for header, values_for_header in key_value_type_mappings.items():
            # if the ANY of the values for a given header is a list, we assume that column contains arrays of values
            array_of = True if any(isinstance(v, list)
                                   for v in values_for_header) else False
            # find either the first item that's non-None, or the first non-empty list
            data_type = self._python_type_to_tdr_type_conversion(
                [a for a in values_for_header if a][0])

            column_metadata = {
                "name": header,
                "datatype": data_type,
                "array_of": array_of,
            }
            columns.append(column_metadata)

        return columns

    @staticmethod
    def _gather_required_and_non_required_headers(metadata_df: Any, dataframe_headers: list[str]) -> list[dict]:
        """Takes in the original dataframe and determines whether each header is required or not. A header is required
        if all values are populated for that header. Otherwise, if some values are missing, or all values are missing,
        it's not considered a required header."""

        header_requirements = []

        na_replaced = metadata_df.replace({None: np.nan})
        for header in dataframe_headers:
            all_none = na_replaced[header].isna().all()
            some_none = na_replaced[header].isna().any()
            # if all rows are none for a given column, we set the default type to "string" type in TDR
            if all_none:
                header_requirements.append({"name": header, "required": False, "data_type": "string"})
            elif some_none:
                header_requirements.append({"name": header, "required": False})
            else:
                header_requirements.append({"name": header, "required": True})

        return header_requirements

    @staticmethod
    def _reformat_metadata(cleaned_metadata: list[dict]) -> dict:
        """Creates a dictionary where the key is the header name, and the value is a list of all values for that header
        using the newly cleaned data"""

        key_value_type_mappings = {}
        unique_headers = {key for row in cleaned_metadata for key in row}

        for header in unique_headers:
            for row in cleaned_metadata:
                value = row[header]
                if header not in key_value_type_mappings:
                    key_value_type_mappings[header] = [value]
                else:
                    key_value_type_mappings[header].append(value)
        return key_value_type_mappings

    def infer_schema(self) -> dict:
        logging.info(f"Inferring schema for table {self.table_name}")
        # create the dataframe
        metadata_df = pd.DataFrame(self.input_metadata)
        # Replace all nan with None
        metadata_df = metadata_df.where(pd.notnull(metadata_df), None)

        # find all headers that need to be renamed if they have "entity" in them and rename the headers
        headers_to_be_renamed = [{h: h.split(":")[1] for h in list(metadata_df.columns) if h.startswith("entity")}][0]
        metadata_df = metadata_df.rename(columns=headers_to_be_renamed)

        # start by gathering the column metadata by determining which headers are required or not
        column_metadata = self._gather_required_and_non_required_headers(metadata_df, list(metadata_df.columns))

        # drop columns where ALL values are None, but keep rows where some values are None
        # we keep the rows where some values are none because if we happen to have a different column that's none in
        # every row, we could end up with no data at the end
        all_none_columns_dropped_df = metadata_df.dropna(axis=1, how="all")
        cleaned_metadata = all_none_columns_dropped_df.to_dict(orient="records")
        key_value_type_mappings = self._reformat_metadata(cleaned_metadata)

        # check to see if all values corresponding to a header are of the same type
        self._check_type_consistency(key_value_type_mappings)

        columns = self._format_column_metadata(key_value_type_mappings)

        # combine the information about required headers with the data types that were collected
        for header_metadata in column_metadata:
            matching_metadata = [d for d in columns if d["name"] == header_metadata["name"]]
            if matching_metadata:
                header_metadata.update(matching_metadata[0])

        tdr_tables_json = {
            "name": self.table_name,
            "columns": column_metadata,
        }
        return tdr_tables_json


class GetPermissionsForWorkspaceIngest:
    def __init__(self, terra_workspace: TerraWorkspace, dataset_info: dict, added_to_auth_domain: bool = False):
        self.terra_workspace = terra_workspace
        self.dataset_info = dataset_info
        self.added_to_auth_domain = added_to_auth_domain

    def run(self) -> None:
        # Ensure dataset SA account is reader on Terra workspace.
        tdr_sa_account = self.dataset_info["ingestServiceAccount"]
        self.terra_workspace.update_user_acl(email=tdr_sa_account, access_level="READER")

        # Check if workspace has auth domain
        workspace_info = self.terra_workspace.get_workspace_info()
        auth_domain_list = workspace_info["workspace"]["authorizationDomain"]
        # Attempt to add tdr_sa_account to auth domain
        if auth_domain_list:
            for auth_domain_dict in auth_domain_list:
                auth_domain = auth_domain_dict["membersGroupName"]
                logging.info(f"TDR SA account {tdr_sa_account} needs to be added to auth domain group {auth_domain}")
            if self.added_to_auth_domain:
                logging.info("added_to_auth_domain has been set to true so assuming account has already been added")
            else:
                logging.info(
                    "Please add TDR SA account to auth domain group to allow access to workspace and then rerun with "
                    "added_to_auth_domain=True"
                )
                sys.exit(0)


class FilterAndBatchIngest:
    def __init__(
            self,
            tdr: TDR,
            filter_existing_ids: bool,
            unique_id_field: str,
            table_name: str,
            ingest_metadata: list[dict],
            dataset_id: str,
            file_list_bool: bool,
            ingest_waiting_time_poll: int,
            ingest_batch_size: int,
            bulk_mode: bool,
            cloud_type: str,
            update_strategy: str,
            load_tag: str,
            test_ingest: bool = False,
            dest_file_path_flat: bool = False,
            file_to_uuid_dict: Optional[dict] = None,
            sas_expire_in_secs: int = 3600,
            schema_info: Optional[dict] = None,
            terra_workspace: Optional[TerraWorkspace] = None
    ):
        self.tdr = tdr
        self.filter_existing_ids = filter_existing_ids
        self.unique_id_field = unique_id_field
        self.table_name = table_name
        self.ingest_metadata = ingest_metadata
        self.dataset_id = dataset_id
        self.file_list_bool = file_list_bool
        self.ingest_waiting_time_poll = ingest_waiting_time_poll
        self.ingest_batch_size = ingest_batch_size
        self.bulk_mode = bulk_mode
        self.cloud_type = cloud_type
        self.update_strategy = update_strategy
        self.load_tag = load_tag
        self.test_ingest = test_ingest
        self.dest_file_path_flat = dest_file_path_flat
        self.sas_expire_in_secs = sas_expire_in_secs
        self.terra_workspace = terra_workspace
        self.file_to_uuid_dict = file_to_uuid_dict
        # Used if you want to provide schema info for tables to make sure values match.
        # Should be dict with key being column name and value being dict with datatype
        self.schema_info = schema_info

    def run(self) -> None:
        if self.filter_existing_ids:
            # Filter out sample ids that are already in the dataset
            filtered_metrics = FilterOutSampleIdsAlreadyInDataset(
                ingest_metrics=self.ingest_metadata,
                dataset_id=self.dataset_id,
                tdr=self.tdr,
                target_table_name=self.table_name,
                filter_entity_id=self.unique_id_field
            ).run()
        else:
            filtered_metrics = self.ingest_metadata
        # If there are metrics to ingest then ingest them
        if filtered_metrics:
            # Batch ingest of table to table within dataset
            logging.info(f"Starting ingest of {self.table_name} into {self.dataset_id}")
            BatchIngest(
                ingest_metadata=filtered_metrics,
                tdr=self.tdr,
                target_table_name=self.table_name,
                dataset_id=self.dataset_id,
                batch_size=self.ingest_batch_size,
                bulk_mode=self.bulk_mode,
                cloud_type=self.cloud_type,
                update_strategy=self.update_strategy,
                waiting_time_to_poll=self.ingest_waiting_time_poll,
                test_ingest=self.test_ingest,
                load_tag=self.load_tag,
                file_list_bool=self.file_list_bool,
                dest_file_path_flat=self.dest_file_path_flat,
                file_to_uuid_dict=self.file_to_uuid_dict,
                schema_info=self.schema_info
            ).run()


class FilterOutSampleIdsAlreadyInDataset:
    def __init__(
            self,
            ingest_metrics: list[dict],
            dataset_id: str, tdr: TDR,
            target_table_name: str,
            filter_entity_id: str
    ):
        self.ingest_metrics = ingest_metrics
        self.tdr = tdr
        self.dataset_id = dataset_id
        self.target_table_name = target_table_name
        self.filter_entity_id = filter_entity_id

    def run(self) -> list[dict]:
        # Get all sample ids that already exist in dataset
        logging.info(
            f"Getting all {self.filter_entity_id} that already exist in table {self.target_table_name} in "
            f"dataset {self.dataset_id}"
        )

        data_set_sample_ids = self.tdr.get_data_set_sample_ids(
            dataset_id=self.dataset_id,
            target_table_name=self.target_table_name,
            entity_id=self.filter_entity_id
        )
        # Filter out rows that already exist in dataset
        filtered_ingest_metrics = [
            row
            for row in self.ingest_metrics
            if str(row[self.filter_entity_id]) not in data_set_sample_ids
        ]
        if len(filtered_ingest_metrics) < len(self.ingest_metrics):
            logging.info(
                f"Filtered out {len(self.ingest_metrics) - len(filtered_ingest_metrics)} rows that already exist in "
                f"dataset"
            )

            if filtered_ingest_metrics:
                return filtered_ingest_metrics
            else:
                logging.info("All rows filtered out as they all exist in dataset, nothing to ingest")
                return []
        else:
            logging.info("No rows were filtered out as they all do not exist in dataset")
            return filtered_ingest_metrics
