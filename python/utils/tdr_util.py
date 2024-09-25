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
from datetime import datetime, date

from .request_util import GET, POST, DELETE
from .tdr_api_schema.create_dataset_schema import CreateDatasetSchema
from .tdr_api_schema.update_dataset_schema import UpdateSchema
from .terra_util import TerraWorkspace
from . import GCP, AZURE  # import from __init__.py


# Can be used when creating a new dataset
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
    """
    A class to interact with the Terra Data Repository (TDR) API.

    Attributes:
        TDR_LINK (str): The base URL for the TDR API.
        request_util (Any): Utility for making HTTP requests.
    """
    TDR_LINK = "https://data.terra.bio/api/repository/v1"

    def __init__(self, request_util: Any):
        """
        Initialize the TDR class.

        Args:
            request_util (Any): Utility for making HTTP requests.
        """
        self.request_util = request_util

    def get_data_set_files(self, dataset_id: str, limit: int = 1000) -> list[dict]:
        """
        Get all files in a dataset.

        Args:
            dataset_id (str): The ID of the dataset.
            limit (int, optional): The maximum number of records to retrieve per batch. Defaults to 1000.

        Returns:
            list[dict]: A list of dictionaries containing the metadata of the files in the dataset.
        """
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}/files"
        logging.info(f"Getting all files in dataset {dataset_id}")
        return self._get_response_from_batched_endpoint(uri=uri, limit=limit)

    def create_file_dict(self, dataset_id: str, limit: int = 20000) -> dict:
        """
        Create a dictionary of all files in a dataset where the key is the file UUID.

        Args:
            dataset_id (str): The ID of the dataset.
            limit (int, optional): The maximum number of records to retrieve per batch. Defaults to 20000.

        Returns:
            dict: A dictionary where the key is the file UUID and the value is the file metadata.
        """
        return {
            file_dict['fileId']: file_dict
            for file_dict in self.get_data_set_files(dataset_id=dataset_id, limit=limit)
        }

    def get_sas_token(self, snapshot_id: str = "", dataset_id: str = "") -> dict:
        """
        Get the SAS token for a snapshot or dataset.

        Args:
            snapshot_id (str, optional): The ID of the snapshot. Defaults to "".
            dataset_id (str, optional): The ID of the dataset. Defaults to "".

        Returns:
            dict: A dictionary containing the SAS token and its expiry time.

        Raises:
            ValueError: If neither snapshot_id nor dataset_id is provided.
        """
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
        """
        Delete a file from a dataset.

        Args:
            file_id (str): The ID of the file to be deleted.
            dataset_id (str): The ID of the dataset.

        Returns:
            str: The job ID of the delete operation.
        """
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}/files/{file_id}"
        response = self.request_util.run_request(uri=uri, method=DELETE)
        job_id = json.loads(response.text)['id']
        logging.info(f"Submitted delete job {job_id} for file {file_id}")
        return job_id

    def delete_files(self, file_ids: list[str], dataset_id: str, batch_size_to_delete_files: int = 100) -> None:
        """
        Delete multiple files from a dataset in batches and monitor delete jobs until completion for each batch.

        Args:
            file_ids (list[str]): A list of file IDs to be deleted.
            dataset_id (str): The ID of the dataset.
            batch_size_to_delete_files (int, optional): The number of files to delete per batch. Defaults to 100.
        """
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
        """
        Add a user to a dataset with a specified policy.

        Args:
            dataset_id (str): The ID of the dataset.
            user (str): The email of the user to be added.
            policy (str): The policy to be assigned to the user. Must be one of "steward", "custodian", or "snapshot_creator".

        Raises:
            ValueError: If the policy is not valid.
        """
        if policy not in ["steward", "custodian", "snapshot_creator"]:
            raise ValueError(f"Policy {policy} is not valid. Must be READER, WRITER, or OWNER")
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}/policies/{policy}/members"
        member_dict = {"email": user}
        logging.info(f"Adding user {user} to dataset {dataset_id} with policy {policy}")
        self.request_util.run_request(
            uri=uri,
            method=POST,
            data=json.dumps(member_dict), content_type="application/json"
        )

    def delete_dataset(self, dataset_id: str) -> None:
        """
        Delete a dataset.

        Args:
            dataset_id (str): The ID of the dataset to be deleted.
        """
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}"
        logging.info(f"Deleting dataset {dataset_id}")
        response = self.request_util.run_request(uri=uri, method=DELETE)
        job_id = response.json()['id']
        MonitorTDRJob(tdr=self, job_id=job_id, check_interval=30).run()

    def delete_snapshot(self, snapshot_id: str) -> None:
        """
        Delete a snapshot.

        Args:
            snapshot_id (str): The ID of the snapshot to be deleted.
        """
        uri = f"{self.TDR_LINK}/snapshots/{snapshot_id}"
        logging.info(f"Deleting snapshot {snapshot_id}")
        response = self.request_util.run_request(uri=uri, method=DELETE)
        job_id = response.json()['id']
        MonitorTDRJob(tdr=self, job_id=job_id, check_interval=30).run()

    def _yield_existing_datasets(
            self, filter: Optional[str] = None, batch_size: int = 100, direction: str = "asc"
    ) -> Any:
        """
        Get all datasets in TDR, optionally filtered by dataset name.

        Args:
            filter (Optional[str]): A filter string to match dataset names. Defaults to None.
            batch_size (int): The number of datasets to retrieve per batch. Defaults to 100.
            direction (str): The direction to sort the datasets by creation date. Defaults to "asc".

        Yields:
            Any: A generator yielding datasets.
        """
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
        """
        Check if a dataset exists by name and optionally by billing profile.

        Args:
            dataset_name (str): The name of the dataset to check.
            billing_profile (Optional[str]): The billing profile ID to match. Defaults to None.

        Returns:
            list[dict]: A list of matching datasets.
        """
        matching_datasets = []
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
        """
        Get information about a dataset.

        Args:
            dataset_id (str): The ID of the dataset.
            info_to_include (Optional[list[str]]): A list of additional information to include. Defaults to None.

        Returns:
            dict: A dictionary containing the dataset information.

        Raises:
            ValueError: If info_to_include contains invalid information types.
        """
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
        """
        Get schema information for a specific table within a dataset.

        Args:
            dataset_id (str): The ID of the dataset.
            table_name (str): The name of the table.

        Returns:
            Union[dict, None]: A dictionary containing the table schema information, or None if the table is not found.
        """
        dataset_info = self.get_dataset_info(dataset_id=dataset_id, info_to_include=["SCHEMA"])
        for table in dataset_info["schema"]["tables"]:
            if table["name"] == table_name:
                return table
        return None

    def get_job_result(self, job_id: str) -> dict:
        """
        Retrieve the result of a job.

        Args:
            job_id (str): The ID of the job.

        Returns:
            dict: A dictionary containing the job result.
        """
        uri = f"{self.TDR_LINK}/jobs/{job_id}/result"
        response = self.request_util.run_request(uri=uri, method=GET)
        return json.loads(response.text)

    def ingest_to_dataset(self, dataset_id: str, data: dict) -> dict:
        """
        Load data into a TDR dataset.

        Args:
            dataset_id (str): The ID of the dataset.
            data (dict): The data to be ingested.

        Returns:
            dict: A dictionary containing the response from the ingest operation.
        """
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}/ingest"
        logging.info(
            "If recently added TDR SA to source bucket/dataset/workspace and you receive a 400/403 error, " +
            "it can sometimes take up to 12/24 hours for permissions to propagate. Try rerunning the script later.")
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
        """
        Retrieve all metrics for a specific table within a dataset.

        Args:
            dataset_id (str): The ID of the dataset.
            target_table_name (str): The name of the target table.
            query_limit (int, optional): The maximum number of records to retrieve per batch. Defaults to 1000.

        Returns:
            list[dict]: A list of dictionaries containing the metrics for the specified table.
        """
        return [
            metric
            for metric in self._yield_data_set_metrics(
                dataset_id=dataset_id,
                target_table_name=target_table_name,
                query_limit=query_limit
            )
        ]

    def _yield_data_set_metrics(self, dataset_id: str, target_table_name: str, query_limit: int = 1000) -> Any:
        """
        Yield all entity metrics from a dataset.

        Args:
            dataset_id (str): The ID of the dataset.
            target_table_name (str): The name of the target table.
            query_limit (int, optional): The maximum number of records to retrieve per batch. Defaults to 1000.

        Yields:
            Any: A generator yielding dictionaries containing the metrics for the specified table.
        """
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
                f"Downloading batch {batch_number} of max {query_limit} records from {target_table_name} table " +
                f"dataset {dataset_id}"
            )
            for record in response.json()["result"]:
                yield record
            search_request["offset"] += query_limit  # type: ignore[operator]

    def get_data_set_sample_ids(self, dataset_id: str, target_table_name: str, entity_id: str) -> list[str]:
        """
        Get existing IDs from a dataset.

        Args:
            dataset_id (str): The ID of the dataset.
            target_table_name (str): The name of the target table.
            entity_id (str): The entity ID to retrieve.

        Returns:
            list[str]: A list of entity IDs from the specified table.
        """
        data_set_metadata = self._yield_data_set_metrics(dataset_id=dataset_id, target_table_name=target_table_name)
        return [str(sample_dict[entity_id]) for sample_dict in data_set_metadata]

    def get_job_status(self, job_id: str) -> requests.Response:
        """
        Retrieve the status of a job.

        Args:
            job_id (str): The ID of the job.

        Returns:
            requests.Response: The response object containing the job status.
        """
        uri = f"{self.TDR_LINK}/jobs/{job_id}"
        response = self.request_util.run_request(uri=uri, method=GET)
        return response

    def get_data_set_file_uuids_from_metadata(self, dataset_id: str) -> list[str]:
        """
        Get all file UUIDs from the metadata of a dataset.

        Args:
            dataset_id (str): The ID of the dataset.

        Returns:
            list[str]: A list of file UUIDs from the dataset metadata.
        """
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
        """
        Get or create a dataset.

        Args:
            dataset_name (str): The name of the dataset.
            billing_profile (str): The billing profile ID.
            schema (dict): The schema of the dataset.
            description (str): The description of the dataset.
            cloud_platform (str): The cloud platform for the dataset.
            additional_properties_dict (Optional[dict], optional): Additional properties for the dataset. Defaults to None.

        Returns:
            str: The ID of the dataset.

        Raises:
            ValueError: If multiple datasets with the same name are found under the billing profile.
        """
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
        """
        Create a new dataset.

        Args:
            schema (dict): The schema of the dataset.
            cloud_platform (str): The cloud platform for the dataset.
            dataset_name (str): The name of the dataset.
            description (str): The description of the dataset.
            profile_id (str): The billing profile ID.
            additional_dataset_properties (Optional[dict], optional): Additional properties for the dataset. Defaults to None.

        Returns:
            Optional[str]: The ID of the created dataset, or None if creation failed.

        Raises:
            ValueError: If the schema validation fails.
        """
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
        """
        Update the schema of a dataset.

        Args:
            dataset_id (str): The ID of the dataset.
            update_note (str): A note describing the update.
            tables_to_add (Optional[list[dict]], optional): A list of tables to add. Defaults to None.
            relationships_to_add (Optional[list[dict]], optional): A list of relationships to add. Defaults to None.
            columns_to_add (Optional[list[dict]], optional): A list of columns to add. Defaults to None.

        Returns:
            Optional[str]: The ID of the updated dataset, or None if the update failed.

        Raises:
            ValueError: If the schema validation fails.
        """
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
        """
        Helper method for all GET endpoints that require batching. Given the URI and the limit (optional), will
        loop through batches until all metadata is retrieved.

        Args:
            uri (str): The base URI for the endpoint (without query params for offset or limit).
            limit (int, optional): The maximum number of records to retrieve per batch. Defaults to 1000.

        Returns:
            list[dict]: A list of dictionaries containing the metadata retrieved from the endpoint.
        """
        batch = 1
        offset = 0
        metadata: list = []
        while True:
            logging.info(f"Retrieving {(batch - 1) * limit} to {batch * limit} records in metadata")
            response_json = self.request_util.run_request(uri=f"{uri}?offset={offset}&limit={limit}", method=GET).json()

            # If no more files, break the loop
            if not response_json:
                logging.info(f"No more results to retrieve, found {len(metadata)} total records")
                break

            metadata.extend(response_json)
            # Increment the offset by limit for the next page
            offset += limit
            batch += 1
        return metadata

    def get_files_from_snapshot(self, snapshot_id: str, limit: int = 1000) -> list[dict]:
        """
        Returns all the metadata about files in a given snapshot. Not all files can be returned at once, so the API
        is used repeatedly until all "batches" have been returned.

        Args:
            snapshot_id (str): The ID of the snapshot.
            limit (int, optional): The maximum number of records to retrieve per batch. Defaults to 1000.

        Returns:
            list[dict]: A list of dictionaries containing the metadata of the files in the snapshot.
        """
        uri = f"{self.TDR_LINK}/snapshots/{snapshot_id}/files"
        return self._get_response_from_batched_endpoint(uri=uri, limit=limit)


class MonitorTDRJob:
    """
    A class to monitor the status of a TDR job until completion.

    Attributes:
        tdr (TDR): An instance of the TDR class.
        job_id (str): The ID of the job to be monitored.
        check_interval (int): The interval in seconds to wait between status checks.
    """

    def __init__(self, tdr: TDR, job_id: str, check_interval: int):
        """
        Initialize the MonitorTDRJob class.

        Args:
            tdr (TDR): An instance of the TDR class.
            job_id (str): The ID of the job to be monitored.
            check_interval (int): The interval in seconds to wait between status checks.
        """
        self.tdr = tdr
        self.job_id = job_id
        self.check_interval = check_interval

    def run(self) -> bool:
        """
        Monitor the job until completion.

        Returns:
            bool: True if the job succeeded, raises an error otherwise.
        """
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
    """
    A class to start and monitor the ingestion of records into a TDR dataset.
    """

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
        """
        Initialize the StartAndMonitorIngest class.

        Args:
            tdr (TDR): An instance of the TDR class.
            ingest_records (list[dict]): The records to be ingested.
            target_table_name (str): The name of the target table.
            dataset_id (str): The ID of the dataset.
            load_tag (str): A tag to identify the load.
            bulk_mode (bool): Flag indicating if bulk mode should be used.
            update_strategy (str): The strategy for updating existing records.
            waiting_time_to_poll (int): The time to wait between polling for job status.
        """
        self.tdr = tdr
        self.ingest_records = ingest_records
        self.target_table_name = target_table_name
        self.dataset_id = dataset_id
        self.load_tag = load_tag
        self.bulk_mode = bulk_mode
        self.update_strategy = update_strategy
        self.waiting_time_to_poll = waiting_time_to_poll

    def _create_ingest_dataset_request(self) -> Any:
        """
        Create the ingestDataset request body.

        Returns:
            Any: The request body for ingesting the dataset.
        """
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
        """
        Run the ingestion process and monitor the job until completion.
        """
        ingest_request = self._create_ingest_dataset_request()
        logging.info(f"Starting ingest to {self.dataset_id}")
        ingest_response = self.tdr.ingest_to_dataset(dataset_id=self.dataset_id, data=ingest_request)
        MonitorTDRJob(
            tdr=self.tdr,
            job_id=ingest_response["id"],
            check_interval=self.waiting_time_to_poll
        ).run()


class ReformatMetricsForIngest:
    """
    Reformat metrics for ingest.
    If file_list is True, then it is a list of file paths and formats differently. Assumes input JSON for that will be
    like below or similar for Azure:
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
        """
        Initialize the ReformatMetricsForIngest class.

        Args:
            ingest_metadata (list[dict]): The metadata to be ingested.
            cloud_type (str): The type of cloud (GCP or AZURE).
            storage_container (Optional[str], optional): The storage container name. For Azure only. Defaults to None.
            sas_token_string (Optional[str], optional): The SAS token string for Azure. Defaults to None.
            file_list (bool, optional): Flag indicating if the input is a list of file paths. Defaults to False.
            dest_file_path_flat (bool, optional): Flag indicating if the destination file path should be flattened. Defaults to False.
            file_to_uuid_dict (Optional[dict], optional): A dictionary mapping file paths to UUIDs. Speeds up ingest
                dramatically as it can skip uploading files or looking up file UUIDs in TDR. Defaults to None.
            schema_info (Optional[dict], optional): Schema information for the tables. Defaults to None.
        """
        self.file_list = file_list
        self.ingest_metadata = ingest_metadata
        self.cloud_type = cloud_type
        self.sas_token_string = sas_token_string
        self.file_prefix = {GCP: "gs://", AZURE: "https://"}[cloud_type]
        self.workspace_storage_container = storage_container
        self.dest_file_path_flat = dest_file_path_flat
        self.file_to_uuid_dict = file_to_uuid_dict
        self.schema_info = schema_info

    def _add_file_ref(self, file_details: dict) -> None:
        """
        Create file ref for ingest.

        Args:
            file_details (dict): The details of the file to be ingested.
        """
        file_details["file_ref"] = {
            "sourcePath": file_details["path"],
            "targetPath": self._format_relative_tdr_path(file_details["path"]),
            "description": f"Ingest of {file_details['path']}",
            "mimeType": file_details["content_type"]
        }

    def _format_relative_tdr_path(self, cloud_path: str) -> str:
        """
        Format cloud path to TDR path.

        Args:
            cloud_path (str): The cloud path to be formatted.

        Returns:
            str: The formatted TDR path.
        """
        if self.cloud_type == GCP:
            relative_path = "/".join(cloud_path.split("/")[3:])
        else:
            split_path = cloud_path.split("/")
            file_path_storage_container = split_path[3]
            if file_path_storage_container != self.workspace_storage_container:
                raise ValueError(
                    f"{cloud_path} storage container {file_path_storage_container} does not match workspace storage "
                    f"container {self.workspace_storage_container}. SAS token will not work"
                )
            relative_path = "/".join(split_path[4:])
        if self.dest_file_path_flat:
            return "/" + relative_path.replace("/", "_").replace("#", "").replace("?", "")
        else:
            return f"/{relative_path}"

    def _check_and_format_file_path(self, column_value: str) -> tuple[Any, bool]:
        """
        Check if column value is a gs:// path and reformat to dict with ingest information. If file_to_uuid_dict is
        provided then it will add existing UUID. If file_to_uuid_dict provided and file not found then will warn and
        return None.

        Args:
            column_value (str): The column value to be checked and formatted.

        Returns:
            tuple[Any, bool]: The formatted column value and a validity flag.
        """
        valid = True
        if isinstance(column_value, str):
            if column_value.startswith(self.file_prefix):
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
                    source_dest_mapping = {
                        "sourcePath": f"{column_value}{self.sas_token_string}" if self.cloud_type == AZURE else column_value,
                        "targetPath": self._format_relative_tdr_path(column_value)
                    }
                    return source_dest_mapping, valid
        return column_value, valid

    def _validate_and_update_column_for_schema(self, column_name: str, column_value: Any) -> tuple[str, bool]:
        """
        Check if column matches what schema expects and attempt to update if not. Changes to string at the end.

        Args:
            column_name (str): The name of the column.
            column_value (Any): The value of the column.

        Returns:
            tuple[str, bool]: The validated and updated column value and a validity flag.
        """
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
                if expected_data_type == "fileref" and not column_value.startswith(self.file_prefix):
                    valid = False
                    logging.warning(f"Column {column_name} with value {column_value} is not a file path")
        return str(column_value), valid

    def _reformat_metric(self, row_dict: dict) -> Optional[dict]:
        """
        Reformat metric for ingest.

        Args:
            row_dict (dict): The row dictionary to be reformatted.

        Returns:
            Optional[dict]: The reformatted row dictionary or None if invalid.
        """
        reformatted_dict = {}
        row_valid = True
        if self.file_list:
            self._add_file_ref(row_dict)
            reformatted_dict = row_dict
        else:
            for key, value in row_dict.items():
                if value or value == 0:
                    if self.schema_info:
                        value, valid = self._validate_and_update_column_for_schema(key, value)
                        if not valid:
                            row_valid = False
                    if isinstance(value, list):
                        updated_value_list = []
                        for item in value:
                            update_value, valid = self._check_and_format_file_path(item)
                            if not valid:
                                row_valid = False
                            updated_value_list.append(update_value)
                        reformatted_dict[key] = updated_value_list
                    else:
                        update_value, valid = self._check_and_format_file_path(value)
                        reformatted_dict[key] = update_value
                        if not valid:
                            row_valid = False
        reformatted_dict["last_modified_date"] = datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S")
        if row_valid:
            return reformatted_dict
        else:
            logging.info(f"Row {json.dumps(row_dict, indent=4)} not valid and will not be included in ingest")
            return None

    def run(self) -> list[dict]:
        """
        Run the reformatting process for all metrics.

        Returns:
            list[dict]: A list of reformatted metrics.
        """
        reformatted_metrics = []
        for row_dict in self.ingest_metadata:
            reformatted_row = self._reformat_metric(row_dict)
            if reformatted_row:
                reformatted_metrics.append(reformatted_row)
        return reformatted_metrics


class SetUpTDRTables:
    """
    A class to set up TDR tables by comparing and updating schemas.

    Attributes:
        tdr (TDR): An instance of the TDR class.
        dataset_id (str): The ID of the dataset.
        table_info_dict (dict): A dictionary containing table information.
    """

    def __init__(self, tdr: TDR, dataset_id: str, table_info_dict: dict):
        """
        Initialize the SetUpTDRTables class.

        Args:
            tdr (TDR): An instance of the TDR class.
            dataset_id (str): The ID of the dataset.
            table_info_dict (dict): A dictionary containing table information.
        """
        self.tdr = tdr
        self.dataset_id = dataset_id
        self.table_info_dict = table_info_dict

    @staticmethod
    def _compare_table(reference_dataset_table: dict, target_dataset_table: list[dict], table_name: str) -> list[dict]:
        """
        Compare tables between two datasets.

        Args:
            reference_dataset_table (dict): The reference dataset table schema.
            target_dataset_table (list[dict]): The target dataset table schema.
            table_name (str): The name of the table being compared.

        Returns:
            list[dict]: A list of columns that need to be updated.
        """
        logging.info(f"Comparing table {reference_dataset_table['name']} to existing target table")
        columns_to_update = []
        # Convert target table to dict for easier comparison
        target_dataset_table_dict = {col["name"]: col for col in target_dataset_table}
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
        """
        Compare dataset relationships between two datasets.

        Args:
            reference_dataset_relationships (dict): The reference dataset relationships.
            target_dataset_relationships (list): The target dataset relationships.

        Returns:
            list[dict]: A list of relationships that need to be modified.
        """
        dataset_relationships_to_modify = []
        for dataset in reference_dataset_relationships:
            if dataset not in target_dataset_relationships:
                dataset_relationships_to_modify.append(dataset)
        return dataset_relationships_to_modify

    def run(self) -> dict:
        """
        Run the setup process to ensure tables are created or updated as needed.

        Returns:
            dict: A dictionary with table names as keys and column information as values.
        """
        data_set_info = self.tdr.get_dataset_info(dataset_id=self.dataset_id, info_to_include=["SCHEMA"])
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
                # Ensure there is columns in table before adding to list
                if expected_tdr_schema_dict['columns']:
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
            data_set_info = self.tdr.get_dataset_info(dataset_id=self.dataset_id, info_to_include=["SCHEMA"])
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
    """
    A class to handle batch ingestion of metadata into TDR (Terra Data Repository).
    """

    def __init__(
            self,
            ingest_metadata: list[dict],
            tdr: TDR,
            target_table_name: str,
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
        """
        Initialize the BatchIngest class.

        Args:
            ingest_metadata (list[dict]): The metadata to be ingested.
            tdr (TDR): An instance of the TDR class.
            target_table_name (str): The name of the target table.
            dataset_id (str): The ID of the dataset.
            batch_size (int): The size of each batch for ingestion.
            file_list_bool (bool): Flag indicating if the input is a list of file paths.
            bulk_mode (bool): Flag indicating if bulk mode should be used.
            cloud_type (str): The type of cloud (GCP or AZURE).
            terra_workspace (Optional[TerraWorkspace], optional): An instance of the TerraWorkspace class. Used for Azure
                ingests so sas token can be created. Defaults to None.
            update_strategy (str, optional): The strategy for updating existing records. Defaults to "replace".
            waiting_time_to_poll (int, optional): The time to wait between polling for job status. Defaults to 60.
            sas_expire_in_secs (int, optional): The expiration time for SAS tokens in seconds. Azure only. Defaults to 3600.
            test_ingest (bool, optional): Flag indicating if only the first batch should be ingested for testing. Defaults to False.
            load_tag (Optional[str], optional): A tag to identify the load. Used so future ingests
                can pick up where left off. Defaults to None.
            dest_file_path_flat (bool, optional): Flag indicating if the destination file path should be flattened. Defaults to False.
            file_to_uuid_dict (Optional[dict], optional): A dictionary mapping file paths to UUIDs. If used
                will make ingest much quicker since no ingest or look up of file needed. Defaults to None.
            schema_info (Optional[dict], optional): Schema information for the tables. Validates ingest data matches up
                with schema info. Defaults to None.
            skip_reformat (bool, optional): Flag indicating if reformatting should be skipped. Defaults to False.
        """
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
        """
        Reformat the metadata for ingestion based on the cloud type.

        Args:
            metrics_batch (list[dict]): A batch of metrics to be reformatted.

        Returns:
            list[dict]: The reformatted batch of metrics.
        """
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
        """
        Run the batch ingestion process.
        """
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
        """
        Initialize the ConvertTerraTableInfoForIngest class.

        Args:
            table_metadata (list[dict]): The metadata of the table to be converted.
            tdr_row_id (str): The row ID to be used in the TDR. Defaults to 'sample_id'.
            columns_to_ignore (list[str]): List of columns to ignore during conversion. Defaults to an empty list.
        """
        self.table_metadata = table_metadata
        self.tdr_row_id = tdr_row_id
        self.columns_to_ignore = columns_to_ignore

    def run(self) -> list[dict]:
        """
        Convert the table metadata into a format suitable for TDR ingestion.

        Returns:
            list[dict]: A list of dictionaries containing the converted table metadata.
        """
        return [
            {
                self.tdr_row_id: row["name"],
                **{k: v for k, v in row["attributes"].items()
                   # if columns_to_ignore is not provided or if the column is not in the columns_to_ignore list
                   if k not in self.columns_to_ignore}
            }
            for row in self.table_metadata
        ]


class InferTDRSchema:
    """
    A class to infer the schema for a table in TDR (Terra Data Repository) based on input metadata.
    """

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
        """
        Initialize the InferTDRSchema class.

        Args:
            input_metadata (list[dict]): The input metadata to infer the schema from.
            table_name (str): The name of the table for which the schema is being inferred.
        """
        self.input_metadata = input_metadata
        self.table_name = table_name

    @staticmethod
    def _check_type_consistency(key_value_type_mappings: dict) -> None:
        """
        Check if all values for each header are of the same type.

        Args:
            key_value_type_mappings (dict): A dictionary where the key is the header, and the value is a list of values for the header.

        Raises:
            Exception: If types do not match for any header.
        """
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
        """
        Convert Python data types to TDR data types.

        Args:
            value_for_header (Any): The value to determine the TDR type for.

        Returns:
            str: The TDR data type.
        """
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

        # Tried to use this to parse datetimes, but it was turning too many
        # regular ints into datetimes. Commenting out for now
        # try:
        #    date_or_time = parser.parse(value_for_header)
        #    return self.PYTHON_TDR_DATA_TYPE_MAPPING[type(date_or_time)]
        #    pass
        # except (TypeError, ParserError):
        #    pass

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
        """
        Generate the metadata for each column's header name, data type, and whether it's an array of values.

        Args:
            key_value_type_mappings (dict): A dictionary where the key is the header, and the value is a list of values for the header.

        Returns:
            list[dict]: A list of dictionaries containing column metadata.
        """
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
        """
        Determine whether each header is required or not.

        Args:
            metadata_df (Any): The original dataframe.
            dataframe_headers (list[str]): A list of dataframe headers.

        Returns:
            list[dict]: A list of dictionaries containing header requirements.
        """
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
        """
        Create a dictionary where the key is the header name, and the value is a list of all values for that header.

        Args:
            cleaned_metadata (list[dict]): The cleaned metadata.

        Returns:
            dict: A dictionary with header names as keys and lists of values as values.
        """
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
        """
        Infer the schema for the table based on the input metadata.

        Returns:
            dict: The inferred schema in TDR format.
        """
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
        """
        Initialize the GetPermissionsForWorkspaceIngest class.

        Args:
            terra_workspace (TerraWorkspace): Instance of the TerraWorkspace class.
            dataset_info (dict): Information about the dataset.
            added_to_auth_domain (bool, optional): Flag indicating if the SA account
                has been added to the auth domain. Defaults to False.
        """
        self.terra_workspace = terra_workspace
        self.dataset_info = dataset_info
        self.added_to_auth_domain = added_to_auth_domain

    def run(self) -> None:
        """
        Ensure the dataset SA account has the necessary permissions on the Terra workspace.

        This method updates the user ACL to make the SA account a reader on the Terra workspace.
        It also checks if the workspace has an authorization domain and logs the
        necessary steps to add the SA account to the auth domain.
        """
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
                    f"Please add TDR SA account {tdr_sa_account} to auth domain group(s) to allow  "
                    "access to workspace and then rerun with added_to_auth_domain=True"
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
        """
        Initialize the FilterAndBatchIngest class.

        Args:
            tdr (TDR): Instance of the TDR class.
            filter_existing_ids (bool): Whether to filter out sample IDs that already exist in the dataset.
            unique_id_field (str): The unique ID field to filter on.
            table_name (str): The name of the table to ingest data into.
            ingest_metadata (list[dict]): The metadata to ingest.
            dataset_id (str): The ID of the dataset.
            file_list_bool (bool): Whether the ingest metadata is a list of files.
            ingest_waiting_time_poll (int): The waiting time to poll for ingest status.
            ingest_batch_size (int): The batch size for ingest.
            bulk_mode (bool): Whether to use bulk mode for ingest.
            cloud_type (str): The type of cloud (e.g., GCP, AZURE).
            update_strategy (str): The update strategy to use.
            load_tag (str): The load tag for the ingest. Used to make future ingests of same files go faster.
            test_ingest (bool, optional): Whether to run a test ingest. Defaults to False.
            dest_file_path_flat (bool, optional): Whether to flatten the destination file path. Defaults to False.
            file_to_uuid_dict (Optional[dict], optional): A dictionary mapping files to UUIDs.
                If supplied makes ingest run faster due to just linking to already ingested file UUID. Defaults to None.
            sas_expire_in_secs (int, optional): The expiration time for SAS tokens in seconds.
                Azure only. Defaults to 3600.
            schema_info (Optional[dict], optional): Schema information for the tables.
                Used to validate ingest metrics match. Defaults to None.
            terra_workspace (Optional[TerraWorkspace], optional): Instance of the TerraWorkspace class.
                Only used for azure ingests to get token. Defaults to None.
        """
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
        self.schema_info = schema_info

    def run(self) -> None:
        """
        Run the filter and batch ingest process.

        This method filters out sample IDs that already exist in the dataset (if specified),
        and then performs a batch ingest of the remaining metadata into the specified table.
        """
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
        """
        Initialize the FilterOutSampleIdsAlreadyInDataset class.

        Args:
            ingest_metrics (list[dict]): The metrics to be ingested.
            dataset_id (str): The ID of the dataset.
            tdr (TDR): Instance of the TDR class.
            target_table_name (str): The name of the target table.
            filter_entity_id (str): The entity ID to filter on.
        """
        self.ingest_metrics = ingest_metrics
        self.tdr = tdr
        self.dataset_id = dataset_id
        self.target_table_name = target_table_name
        self.filter_entity_id = filter_entity_id

    def run(self) -> list[dict]:
        """
        Run the filter process to remove sample IDs that already exist in the dataset.

        Returns:
            list[dict]: The filtered ingest metrics.
        """
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
