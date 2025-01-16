import json
import logging

import requests
import re
from typing import Any, Optional, Union
from urllib.parse import unquote
from pydantic import ValidationError

from ..requests_utils.request_util import GET, POST, DELETE
from ..tdr_api_schema.create_dataset_schema import CreateDatasetSchema
from ..tdr_api_schema.update_dataset_schema import UpdateSchema
from .tdr_job_utils import MonitorTDRJob, SubmitAndMonitorMultipleJobs
from .. import ARG_DEFAULTS


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

    def get_data_set_files(
            self,
            dataset_id: str,
            limit: int = ARG_DEFAULTS['batch_size_to_list_files']  # type: ignore[assignment]
    ) -> list[dict]:
        """
        Get all files in a dataset. Returns json like below

            {
        "fileId": "68ba8bfc-1d84-4ef3-99b8-cf1754d5rrrr",
        "collectionId": "b20b6024-5943-4c23-82e7-9c24f545fuy7",
        "path": "/path/set/in/ingest.csv",
        "size": 1722,
        "checksums": [
            {
                "checksum": "82f7e79v",
                "type": "crc32c"
            },
            {
                "checksum": "fff973507e30b74fa47a3d6830b84a90",
                "type": "md5"
            }
        ],
        "created": "2024-13-11T15:01:00.256Z",
        "description": null,
        "fileType": "file",
        "fileDetail": {
            "datasetId": "b20b6024-5943-4c23-82e7-9c24f5456444",
            "mimeType": null,
            "accessUrl": "gs://datarepo-bucket/path/to/actual/file.csv",
            "loadTag": "RP_3333-RP_3333"
        },
        "directoryDetail": null
    }

        Args:
            dataset_id (str): The ID of the dataset.
            limit (int, optional): The maximum number of records to retrieve per batch. Defaults to 1000.

        Returns:
            list[dict]: A list of dictionaries containing the metadata of the files in the dataset.
        """
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}/files"
        logging.info(f"Getting all files in dataset {dataset_id}")
        return self._get_response_from_batched_endpoint(uri=uri, limit=limit)

    def create_file_dict(
            self,
            dataset_id: str,
            limit: int = ARG_DEFAULTS['batch_size_to_list_files']  # type: ignore[assignment]
    ) -> dict:
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

    def create_file_uuid_dict_for_ingest_for_experimental_self_hosted_dataset(
            self,
            dataset_id: str,
            limit: int = ARG_DEFAULTS['batch_size_to_list_files']  # type: ignore[assignment]
    ) -> dict:
        """
        Create a dictionary of all files in a dataset where the key is the file 'path' and the value is the file UUID.
        This assumes that the tdr 'path' is original path of the file in the cloud storage with gs:// stripped out

        This will ONLY work if dataset was created with experimentalSelfHosted = True

        Args:
            dataset_id (str): The ID of the dataset.
            limit (int, optional): The maximum number of records to retrieve per batch. Defaults to 20000.

        Returns:
            dict: A dictionary where the key is the file UUID and the value is the file path.
        """
        return {
            file_dict['fileDetail']['accessUrl']: file_dict['fileId']
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

    def delete_files(
            self,
            file_ids: list[str],
            dataset_id: str,
            batch_size_to_delete_files: int = 250,
            check_interval: int = 15) -> None:
        """
        Delete multiple files from a dataset in batches and monitor delete jobs until completion for each batch.

        Args:
            file_ids (list[str]): A list of file IDs to be deleted.
            dataset_id (str): The ID of the dataset.
            batch_size_to_delete_files (int, optional): The number of files to delete per batch. Defaults to 100.
            check_interval (int, optional): The interval in seconds to wait between status checks. Defaults to 15.
        """
        SubmitAndMonitorMultipleJobs(
            tdr=self,
            job_function=self.delete_file,
            job_args_list=[(file_id, dataset_id) for file_id in file_ids],
            batch_size=batch_size_to_delete_files,
            check_interval=check_interval
        ).run()

    def add_user_to_dataset(self, dataset_id: str, user: str, policy: str) -> None:
        """
        Add a user to a dataset with a specified policy.

        Args:
            dataset_id (str): The ID of the dataset.
            user (str): The email of the user to be added.
            policy (str): The policy to be assigned to the user.
                Must be one of "steward", "custodian", or "snapshot_creator".

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

    def remove_user_from_dataset(self, dataset_id: str, user: str, policy: str) -> None:
        """
        Remove a user from a dataset.

        Args:
            dataset_id (str): The ID of the dataset.
            user (str): The email of the user to be removed.
            policy (str): The policy to be removed from the user.
                Must be one of "steward", "custodian", or "snapshot_creator".

        Raises:
            ValueError: If the policy is not valid.
        """
        if policy not in ["steward", "custodian", "snapshot_creator"]:
            raise ValueError(f"Policy {policy} is not valid. Must be steward, custodian, or snapshot_creator")
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}/policies/{policy}/members/{user}"
        logging.info(f"Removing user {user} from dataset {dataset_id} with policy {policy}")
        self.request_util.run_request(uri=uri, method=DELETE)

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
        MonitorTDRJob(tdr=self, job_id=job_id, check_interval=30, return_json=False).run()

    def get_snapshot_info(
            self,
            snapshot_id: str,
            continue_not_found: bool = False,
            info_to_include: Optional[list[str]] = None
    ) -> dict:
        """
        Get information about a snapshot.

        Args:
            snapshot_id (str): The ID of the snapshot.
            continue_not_found (bool, optional): Whether to accept a 404 response. Defaults to False.
            info_to_include (Optional[list[str]]): A list of additional information to include. Defaults to None.
                Options are: SOURCES, TABLES, RELATIONSHIPS, ACCESS_INFORMATION, PROFILE, PROPERTIES, DATA_PROJECT,
                CREATION_INFORMATION, DUOS

        Returns:
            dict: A dictionary containing the snapshot information.
        """
        acceptable_return_code = [404, 403] if continue_not_found else []
        acceptable_include_info = [
            "SOURCES", "TABLES", "RELATIONSHIPS",
            "ACCESS_INFORMATION", "PROFILE", "PROPERTIES",
            "DATA_PROJECT", "CREATION_INFORMATION", "DUOS"
        ]
        if info_to_include:
            if not all(info in acceptable_include_info for info in info_to_include):
                raise ValueError(f"info_to_include must be a subset of {acceptable_include_info}")
            include_string = '&include='.join(info_to_include)
        else:
            include_string = ""
        uri = f"{self.TDR_LINK}/snapshots/{snapshot_id}?include={include_string}"
        response = self.request_util.run_request(
            uri=uri,
            method=GET,
            accept_return_codes=acceptable_return_code
        )
        if response.status_code == 404:
            logging.warning(f"Snapshot {snapshot_id} not found")
            return {}
        if response.status_code == 403:
            logging.warning(f"Access denied for snapshot {snapshot_id}")
            return {}
        return json.loads(response.text)

    def delete_snapshots(
            self,
            snapshot_ids: list[str],
            batch_size: int = 25,
            check_interval: int = 10,
            verbose: bool = False) -> None:
        """
        Delete multiple snapshots.

        Args:
            snapshot_ids (list[str]): A list of snapshot IDs to be deleted.
            batch_size (int, optional): The number of snapshots to delete per batch. Defaults to 25.
            check_interval (int, optional): The interval in seconds to wait between status checks. Defaults to 10.
            verbose (bool, optional): Whether to log detailed information about each job. Defaults to False.
        """
        SubmitAndMonitorMultipleJobs(
            tdr=self,
            job_function=self.delete_snapshot,
            job_args_list=[(snapshot_id,) for snapshot_id in snapshot_ids],
            batch_size=batch_size,
            check_interval=check_interval,
            verbose=verbose
        ).run()

    def delete_snapshot(self, snapshot_id: str) -> str:
        """
        Delete a snapshot.

        Args:
            snapshot_id (str): The ID of the snapshot to be deleted.
        """
        uri = f"{self.TDR_LINK}/snapshots/{snapshot_id}"
        logging.info(f"Deleting snapshot {snapshot_id}")
        response = self.request_util.run_request(uri=uri, method=DELETE)
        job_id = response.json()['id']
        return job_id

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
            log_message = f"Searching for datasets with filter {filter} in batches of {batch_size}"
        else:
            filter_str = ""
            log_message = f"Searching for all datasets in batches of {batch_size}"
        logging.info(log_message)
        while True:
            uri = f"{self.TDR_LINK}/datasets?offset={offset}&limit={batch_size}&sort=created_date&direction={direction}{filter_str}"  # noqa: E501
            response = self.request_util.run_request(uri=uri, method=GET)
            datasets = response.json()["items"]
            if not datasets:
                break
            for dataset in datasets:
                yield dataset
            offset += batch_size
            break

    def check_if_dataset_exists(self, dataset_name: str, billing_profile: Optional[str] = None) -> list[dict]:
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
            # Search uses wildcard so could grab more datasets where dataset_name is substring
            if dataset_name == dataset["name"]:
                if billing_profile:
                    if dataset["defaultProfileId"] == billing_profile:
                        logging.info(
                            f"Dataset {dataset['name']} already exists under billing profile {billing_profile}")
                        dataset_id = dataset["id"]
                        logging.info(f"Dataset ID: {dataset_id}")
                        matching_datasets.append(dataset)
                    else:
                        logging.warning(
                            f"Dataset {dataset['name']} exists but is under {dataset['defaultProfileId']} " +
                            f"and not under billing profile {billing_profile}"
                        )
                        # Datasets names need to be unique regardless of billing profile, so raise an error if
                        # a dataset with the same name is found but is not under the requested billing profile
                        raise ValueError(
                            f"Dataset {dataset_name} already exists but is not under billing profile {billing_profile}")
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

    def get_table_schema_info(
            self,
            dataset_id: str,
            table_name: str,
            dataset_info: Optional[dict] = None
    ) -> Union[dict, None]:
        """
        Get schema information for a specific table within a dataset.

        Args:
            dataset_id (str): The ID of the dataset.
            table_name (str): The name of the table.
            dataset_info (dict, optional): The dataset information if already retrieved. Defaults to None.

        Returns:
            Union[dict, None]: A dictionary containing the table schema information, or None if the table is not found.
        """
        if not dataset_info:
            dataset_info = self.get_dataset_info(dataset_id=dataset_id, info_to_include=["SCHEMA"])
        for table in dataset_info["schema"]["tables"]:
            if table["name"] == table_name:
                return table
        return None

    def get_job_result(self, job_id: str, expect_failure: bool = False) -> requests.Response:
        """
        Retrieve the result of a job.

        Args:
            job_id (str): The ID of the job.
            expect_failure (bool, optional): Whether the job is expected to fail. Defaults to False.

        Returns:
            dict: A dictionary containing the job result.
        """
        uri = f"{self.TDR_LINK}/jobs/{job_id}/result"
        # If job is expected to fail, accept any return code
        acceptable_return_code = list(range(100, 600)) if expect_failure else []
        response = self.request_util.run_request(uri=uri, method=GET, accept_return_codes=acceptable_return_code)
        return response

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

    def file_ingest_to_dataset(
            self,
            dataset_id: str,
            profile_id: str,
            file_list: list[dict],
            load_tag: str = "file_ingest_load_tag"
    ) -> dict:
        """
        Load files into a TDR dataset.

        Args:
            dataset_id (str): The ID of the dataset.
            data (dict): list of cloud file paths to be ingested.
            {
                "sourcePath":"gs:{bucket_name}/{file_path}",
                "targetPath":"/{path}/{file_name}"
            }

        Returns:
            dict: A dictionary containing the response from the ingest operation.
        """
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}/files/bulk/array"
        data = {
            "profileId": profile_id,
            "loadTag": f"{load_tag}",
            "maxFailedFileLoads": 0,
            "loadArray": file_list
        }

        response = self.request_util.run_request(
            uri=uri,
            method=POST,
            content_type="application/json",
            data=json.dumps(data)
        )
        job_id = response.json()['id']
        job_results = MonitorTDRJob(tdr=self, job_id=job_id, check_interval=30, return_json=True).run()
        return job_results  # type: ignore[return-value]

    def get_dataset_table_metrics(
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
            for metric in self._yield_dataset_metrics(
                dataset_id=dataset_id,
                target_table_name=target_table_name,
                query_limit=query_limit
            )
        ]

    def _yield_dataset_metrics(self, dataset_id: str, target_table_name: str, query_limit: int = 1000) -> Any:
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
        data_set_metadata = self._yield_dataset_metrics(dataset_id=dataset_id, target_table_name=target_table_name)
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
            data_set_metrics = self.get_dataset_table_metrics(dataset_id=dataset_id, target_table_name=table_name)
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

    def soft_delete_entries(
            self,
            dataset_id: str,
            table_name: str,
            datarepo_row_ids: list[str],
            check_intervals: int = 15
    ) -> None:
        """
        Soft delete specific records from a table.

        Args:
            dataset_id (str): The ID of the dataset.
            table_name (str): The name of the target table.
            datarepo_row_ids (list[str]): A list of row IDs to be deleted.
            check_intervals (int, optional): The interval in seconds to wait between status checks. Defaults to 15.

        Returns:
            None
        """
        if not datarepo_row_ids:
            logging.info(f"No records found to soft delete in table {table_name}")
            return
        logging.info(f"Soft deleting {len(datarepo_row_ids)} records from table {table_name}")
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}/deletes"
        payload = {
            "deleteType": "soft",
            "specType": "jsonArray",
            "tables": [
                {
                    "tableName": table_name,
                    "jsonArraySpec": {
                        "rowIds": datarepo_row_ids
                    }
                }
            ]
        }
        response = self.request_util.run_request(
            method=POST,
            uri=uri,
            data=json.dumps(payload),
            content_type="application/json"
        )
        job_id = response.json()["id"]
        MonitorTDRJob(tdr=self, job_id=job_id, check_interval=check_intervals, return_json=False).run()

    def soft_delete_all_table_entries(
            self,
            dataset_id: str,
            table_name: str,
            query_limit: int = 1000,
            check_intervals: int = 15
    ) -> None:
        """
        Soft deletes all records in a table.

        Args:
            dataset_id (str): The ID of the dataset.
            table_name (str): The name of the target table.
            query_limit (int, optional): The maximum number of records to retrieve per batch. Defaults to 1000.
            check_intervals (int, optional): The interval in seconds to wait between status checks. Defaults to 15.

        Returns:
            None
        """
        data_set_metrics = self.get_dataset_table_metrics(
            dataset_id=dataset_id, target_table_name=table_name, query_limit=query_limit
        )
        row_ids = [metric["datarepo_row_id"] for metric in data_set_metrics]
        self.soft_delete_entries(
            dataset_id=dataset_id,
            table_name=table_name,
            datarepo_row_ids=row_ids,
            check_intervals=check_intervals
        )

    def get_or_create_dataset(
            self,
            dataset_name: str,
            billing_profile: str,
            schema: dict,
            description: str,
            cloud_platform: str,
            delete_existing: bool = False,
            continue_if_exists: bool = False,
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
            additional_properties_dict (Optional[dict], optional): Additional properties
                for the dataset. Defaults to None.
            delete_existing (bool, optional): Whether to delete the existing dataset if found.
                Defaults to False.
            continue_if_exists (bool, optional): Whether to continue if the dataset already exists.
                Defaults to False.

        Returns:
            str: The ID of the dataset.

        Raises:
            ValueError: If multiple datasets with the same name are found under the billing profile.
        """
        existing_data_sets = self.check_if_dataset_exists(dataset_name, billing_profile)
        if existing_data_sets:
            if not continue_if_exists:
                raise ValueError(
                    f"Run with continue_if_exists=True to use the existing dataset {dataset_name}"
                )
            # If delete_existing is True, delete the existing dataset and set existing_data_sets to an empty list
            if delete_existing:
                logging.info(f"Deleting existing dataset {dataset_name}")
                self.delete_dataset(existing_data_sets[0]["id"])
                existing_data_sets = []
            # If not delete_existing and continue_if_exists then grab existing datasets id
            else:
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
            additional_dataset_properties (Optional[dict], optional): Additional
                properties for the dataset. Defaults to None.

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
        job_results = MonitorTDRJob(tdr=self, job_id=job_id, check_interval=30, return_json=True).run()
        dataset_id = job_results["id"]  # type: ignore[index]
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
        job_results = MonitorTDRJob(tdr=self, job_id=job_id, check_interval=30, return_json=True).run()
        dataset_id = job_results["id"]  # type: ignore[index]
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

    def get_dataset_snapshots(self, dataset_id: str) -> list[dict]:
        """
        Returns snapshots belonging to specified datset.

        Args:
            dataset_id: uuid of dataset to query.

        Returns:
            list[dict]: A list of dictionaries containing the metadata of snapshots in the dataset.
        """
        uri = f"{self.TDR_LINK}/snapshots?datasetIds={dataset_id}"
        response = self.request_util.run_request(
            uri=uri,
            method=GET
        )
        return response.json()


class FilterOutSampleIdsAlreadyInDataset:
    def __init__(
            self,
            ingest_metrics: list[dict],
            dataset_id: str,
            tdr: TDR,
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
                f"dataset. There is {len(filtered_ingest_metrics)} rows left to ingest"
            )

            if filtered_ingest_metrics:
                return filtered_ingest_metrics
            else:
                logging.info("All rows filtered out as they all exist in dataset, nothing to ingest")
                return []
        else:
            logging.info("No rows were filtered out as they all do not exist in dataset")
            return filtered_ingest_metrics
