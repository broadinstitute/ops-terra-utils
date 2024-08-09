# If Using azure then
#   pip install azure-identity
#   pip install azure-storage-blob
#   pip install wds-client
# pip install backoff

# If getting azure token use:
#  pip install azure-identity azure-mgmt-resource
#     !az login --identity --allow-no-subscriptions
#     cli_token = !az account get-access-token | jq .accessToken
#     azure_token = cli_token[0].replace('"', '')

# To get gcp token if doing locally run:
#   pip install google-auth google-auth-httplib2 google-auth-oauthlib ?
#    gcloud auth application-default print-access-token
#

import requests
import backoff
import json
import pytz
import os
import logging
import time
import re
import httplib2
import base64
import sys
from schema import Schema, And, Use, Optional, SchemaError

import pandas as pd
import numpy as np

from urllib.parse import urlparse, unquote
from typing import Any, List, Union, Optional
from datetime import datetime, timedelta, date, time
from dateutil import parser
from dateutil.parser import ParserError


GCP = 'gcp'
AZURE = 'azure'

GET = 'get'
POST = 'post'
PUT = 'put'
PATCH = 'patch'
DELETE = 'delete'

# Used when creating a new dataset
FILE_INVENTORY_DEFAULT_SCHEMA = {
    "tables": [
      {
        "name": "file_inventory",
        "columns": [
            {
                "name": "file_name",
                "datatype": "string",
                "array_of": False,
                "required": True
            },
            {
                "name": "file_path",
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
        ],
        "primaryKey": [
          "file_name"
        ],
      }
    ]
}


class Token:
    def __init__(self, cloud: Optional[str] = None, token_file: Optional[str] = None):
        self.cloud = cloud
        self.expiry = None
        self.token_string = None
        # If provided with a file just use the contents of file
        if token_file:
            self.token_file = token_file
            with open(self.token_file) as f:
                self.token_string = f.read().rstrip()
        else:
            self.token_file = None
            # If not provided with a file must authenticate with either google or azure python libraries
            if self.cloud == GCP:
                # Only import libraries if needed
                from oauth2client.client import GoogleCredentials
                self.credentials = GoogleCredentials.get_application_default()
                self.credentials = self.credentials.create_scoped(
                    [
                        "https://www.googleapis.com/auth/userinfo.profile",
                        "https://www.googleapis.com/auth/userinfo.email",
                        "https://www.googleapis.com/auth/devstorage.full_control"
                    ]
                )
            elif self.cloud == AZURE:
                # Only import libraries if needed
                from azure.identity import DefaultAzureCredential
                self.credentials = DefaultAzureCredential()
                self.az_token = self.credentials.get_token(
                    "https://management.azure.com/.default")
            else:
                raise ValueError(f"Cloud {self.cloud} not supported. Must be {GCP} or {AZURE}")

    def _get_gcp_token(self) -> str:
        # Refresh token if it has not been set or if it is expired or close to expiry
        if not self.token_string or not self.expiry or self.expiry < datetime.now(pytz.UTC) + timedelta(minutes=5):
            http = httplib2.Http()
            self.credentials.refresh(http)
            self.token_string = self.credentials.get_access_token().access_token
            # Set expiry to use UTC since google uses that timezone
            self.expiry = self.credentials.token_expiry.replace(tzinfo=pytz.UTC)
            # Convert expiry time to EST for logging
            est_expiry = self.expiry.astimezone(pytz.timezone('US/Eastern'))
            logging.info(f"New token expires at {est_expiry} EST")
        return self.token_string

    def _get_az_token(self) -> str:
        # This is not working... Should also check about timezones once it does work
        if not self.token_string or not self.expiry or self.expiry < datetime.now() - timedelta(minutes=10):
            self.az_token = self.credentials.get_token(
                "https://management.azure.com/.default")
            self.token_string = self.az_token.token
            self.expiry = datetime.fromtimestamp(self.az_token.expires_on)
        return self.token_string

    def get_token(self) -> str:
        # If token file provided then always return contents
        if self.token_file:
            return self.token_string
        elif self.cloud == GCP:
            return self._get_gcp_token()
        else:
            return self._get_az_token()


class RunRequest:
    def __init__(self, token: Any, max_retries: int = 5, max_backoff_time: int = 900):
        self.max_retries = max_retries
        self.max_backoff_time = max_backoff_time
        self.token = token

    @staticmethod
    def _create_backoff_decorator(max_tries: int, factor: int, max_time: int) -> Any:
        """Create backoff decorator so we can pass in max_tries."""
        return backoff.on_exception(
            backoff.expo,
            requests.exceptions.RequestException,
            max_tries=max_tries,
            factor=factor,
            max_time=max_time
        )

    def run_request(self, uri: str, method: str, data: Any = None, params: Optional[dict] = None,
                    factor: int = 15, content_type: Optional[str] = None,) -> requests.Response:
        """Run request."""
        # Create a custom backoff decorator with the provided parameters
        backoff_decorator = self._create_backoff_decorator(
            max_tries=self.max_retries,
            factor=factor,
            max_time=self.max_backoff_time
        )

        # Apply the backoff decorator to the actual request execution
        @backoff_decorator
        def _make_request() -> requests.Response:
            if method == GET:
                response = requests.get(
                    uri,
                    headers=self._create_headers(content_type=content_type),
                    params=params
                )
            elif method == POST:
                response = requests.post(
                    uri,
                    headers=self._create_headers(content_type=content_type),
                    data=data
                )
            elif method == DELETE:
                response = requests.delete(
                    uri,
                    headers=self._create_headers(content_type=content_type)
                )
            elif method == PATCH:
                response = requests.patch(
                    uri,
                    headers=self._create_headers(content_type=content_type),
                    data=data
                )
            elif method == PUT:
                response = requests.put(
                    uri,
                    headers=self._create_headers(content_type=content_type)
                )
            else:
                raise ValueError(f"Method {method} is not supported")
            if 300 <= response.status_code or response.status_code < 200:
                print(response.text)
                response.raise_for_status()  # Raise an exception for non-200 status codes
            return response

        return _make_request()

    def _create_headers(self, content_type: Optional[str] = None) -> dict:
        """Create headers for API calls."""
        self.token.get_token()
        headers = {"Authorization": f"Bearer {self.token.token_string}",
                   "accept": "application/json"}
        if content_type:
            headers["Content-Type"] = content_type
        return headers


class TDR:
    TDR_LINK = "https://data.terra.bio/api/repository/v1"

    def __init__(self, request_util: Any):
        self.request_util = request_util

    def delete_file(self, file_id: str, dataset_id: str) -> None:
        """Delete a file from a dataset."""
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}/files/{file_id}"
        logging.info(f"Deleting file {file_id} from dataset {dataset_id}")
        response = self.request_util.run_request(uri=uri, method=DELETE)
        # Return job id
        return json.loads(response.text)['id']

    def get_data_set_files(self, dataset_id: str, limit: int = 1000) -> list[dict]:
        """Get all files in a dataset. Returns json like below:
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
        "accessUrl": "gs://datarepo-34a4ac45-bucket/0d1c9aea-e944-4d19-83c3-8675f6aa062a/cf198fcc-3564-46ad-b73f-8bbc3711a866/SM-XXXXX.vcf.gz.md5sum",
        "loadTag": "0d1c9aea-e944-4d19-83c3-8675f6aa123a"
    },
    "directoryDetail": null
}
        """
        offset = 0
        batch = 1
        all_files = []

        logging.info(f"Getting all files in dataset {dataset_id} in batches of {limit}")
        """
        while True:
            logging.info(f"Retrieving {(batch -1) * limit} to {batch * limit} files in dataset")
            #uri = f"{self.TDR_LINK}/datasets/{dataset_id}/files?offset={offset}&limit={limit}"
            uri = f"{self.TDR_LINK}/datasets/{dataset_id}/files"
            response = self.request_util.run_request(uri=uri, method=GET)
            files = json.loads(response.text)

            # If no more files, break the loop
            if not files:
                break

            all_files.extend(files)
            # Increment the offset by limit for the next page
            offset += limit
            batch += 1
        return all_files
        """
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}/files"
        response = self.request_util.run_request(uri=uri, method=GET)
        files = json.loads(response.text)
        return files
    
    def get_sas_token(self, snapshot_id: str="", dataset_id: str="") -> dict:
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
            pass

        sas_expiry_time_pattern = re.compile(r"se.+?(?=\&sp)")
        expiry_time_str = sas_expiry_time_pattern.search(sas_token)
        time_str = unquote(expiry_time_str.group()).replace("se=","")
        
        return {"sas_token": sas_token, "expiry_time": time_str}

    def _yield_existing_datasets(self, filter: Optional[str] = None, batch_size: int = 100, direction: str = 'asc') -> Any:
        """Get all datasets in TDR. Filter can be dataset name"""
        offset = 0
        if filter:
            filter_str = f"&filter={filter}"
        else:
            filter_str = ""
        while True:
            logging.info(f"Searching for datasets with filter {filter_str} in batches of {batch_size}")
            uri = f"{self.TDR_LINK}/datasets?offset={offset}&limit={batch_size}&sort=created_date&direction={direction}{filter_str}"
            response = self.request_util.run_request(uri=uri, method=GET)
            datasets = response.json()['items']
            if not datasets:
                break
            for dataset in datasets:
                yield dataset
            offset += batch_size
            break

    def check_if_dataset_exists(self, dataset_name: str) -> list[dict]:
        return [d for d in self._yield_existing_datasets(filter=dataset_name)]

    def get_data_set_info(self, dataset_id: str, info_to_include: list[str] = None) -> dict:
        """Get dataset info"""
        acceptable_include_info = [
            "SCHEMA", "ACCESS_INFORMATION", "PROFILE", "PROPERTIES", "DATA_PROJECT",
            "STORAGE", "SNAPSHOT_BUILDER_SETTING"
        ]
        if info_to_include:
            if not all(info in acceptable_include_info for info in info_to_include):
                raise ValueError(
                    f"info_to_include must be a subset of {acceptable_include_info}")
            include_string = '&include='.join(info_to_include)
        else:
            include_string = ""
        uri = f"{self.TDR_LINK}/datasets/{dataset_id}?include={include_string}"
        response = self.request_util.run_request(uri=uri, method=GET)
        return json.loads(response.text)

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

    def get_data_set_table_metrics(self, dataset_id: str, target_table_name: str, query_limit: int = 1000) -> list[dict]:
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
            batch_number = int((search_request["offset"] / query_limit)) + 1
            response = self.request_util.run_request(
                uri=uri,
                method=POST,
                content_type="application/json",
                data=json.dumps(search_request)
            )
            if not response or not response.json()["result"]:
                break
            logging.info(
                f"Downloading batch {batch_number} of max {query_limit} records from {target_table_name} table in dataset {dataset_id}")
            for record in response.json()["result"]:
                yield record
            search_request["offset"] += query_limit

    def get_data_set_sample_ids(self, dataset_id: str, target_table_name: str, entity_id: str) -> list[str]:
        """Get existing ids from dataset."""
        data_set_metadata = self._yield_data_set_metrics(
            dataset_id=dataset_id, target_table_name=target_table_name)
        return [
            str(sample_dict[entity_id]) for sample_dict in data_set_metadata
        ]

    def get_job_status(self, job_id: str) -> requests.Response:
        """retrieveJobStatus"""
        # first check job status - retrieveJob
        uri = f"{self.TDR_LINK}/jobs/{job_id}"
        response = self.request_util.run_request(uri=uri, method=GET)
        return response
    
    def create_dataset(self, schema: dict, dataset_name: str, description: str, profile_id: str, list_of_additional_dataset_properties: list=None) -> requests.Response:
        from tdr_api_schema.create_dataset_schema import create_dataset_schema
        dataset_properties = {
            "name": dataset_name,
            "description": f"{description}",
            "defaultProfileId": profile_id,
            "region": "us-central1",
            "cloudPlatform": "gcp",
            "schema": schema
        }
        for property in list_of_additional_dataset_properties:
            dataset_properties[property["key"]] = property["value"]
        try:
            create_dataset_schema.validate(dataset_properties)
        except SchemaError as e:
            raise ValueError(f"Schema validation error: {e}")
        uri = f'{self.TDR_LINK}/repository/v1/datasets'        
        response = self.request_util.run_request(method=POST, uri=uri, data=json.dumps(dataset_properties))
        return response

    def get_data_set_file_uuids_from_metadata(self, dataset_id: str) -> list[str]:
        data_set_info = self.get_data_set_info(dataset_id=dataset_id, info_to_include=["SCHEMA"])
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
            file_uuids = list(set(
                [
                    value
                    for metric in data_set_metrics
                    for key, value in metric.items()
                    if key in file_columns
                ]
            ))
            logging.info(f"Got {len(file_uuids)} file uuids from table '{table_name}'")
            all_metadata_file_uuids.extend(file_uuids)
            # Make full list unique
            all_metadata_file_uuids = list(set(all_metadata_file_uuids))
        logging.info(f"Got {len(all_metadata_file_uuids)} file uuids from {tables} total table(s)")
        return all_metadata_file_uuids

    def get_or_create_dataset(
            self, dataset_name: str, billing_profile: str, schema: dict, staging_area_name: str,
            phs_id: str, monitoring_needed: bool, properties_dict: dict, cloud_platform: str) -> str:
        existing_data_sets = self.check_if_dataset_exists(dataset_name)
        dataset_exists = False
        # If exists then get dataset id
        if existing_data_sets:
            for dataset in existing_data_sets:
                if dataset['defaultProfileId'] == billing_profile:
                    logging.info(
                        f"Dataset {dataset['name']} already exists under billing profile {billing_profile}")
                    dataset_id = dataset['id']
                    dataset_exists = True
                else:
                    logging.warning(
                        f"Dataset {dataset['name']} exists but is under {dataset['defaultProfileId']} " +
                        f"and not under billing profile {billing_profile}"
                    )
        if not dataset_exists:
            logging.info(f"Did not find existing dataset")
            # Create dataset
            dataset_id = self.create_dataset(
                schema=schema,
                dataset_name=dataset_name,
                staging_area_name=staging_area_name,
                profile_id=billing_profile,
                phs_id=phs_id,
                monitoring_needed=monitoring_needed,
                properties_dict=properties_dict,
                cloud_platform=cloud_platform
            )
        return dataset_id

    def create_dataset(self, schema: dict, dataset_name: str, cloud_platform: str, staging_area_name: str, profile_id: str, phs_id: str, monitoring_needed: bool, properties_dict: dict) -> requests.Response:
        dataset_properties = {
            "name": dataset_name,
            "description": f"TDR Dataset for {staging_area_name}",
            "defaultProfileId": profile_id,
            "region": "us-central1",
            "cloudPlatform": cloud_platform,
            "phsId": phs_id,
            "experimentalSelfHosted": True,
            "dedicatedIngestServiceAccount": True,
            "experimentalPredictableFileIds": True,
            "enableSecureMonitoring": monitoring_needed,
            "properties": properties_dict,
            "schema": schema
        }
        
        try:
            create_dataset_schema.validate(dataset_properties)
        except SchemaError as e:
            raise ValueError(f"Schema validation error: {e}")
        
        uri = f'{self.TDR_LINK}/datasets'
        logging.info(f"Creating dataset {dataset_name} under billing profile {profile_id}")
        response = self.request_util.run_request(
            method=POST,
            uri=uri,
            data=json.dumps(dataset_properties),
            content_type='application/json'
        )
        job_id = response.json()['id']
        completed = MonitorTDRJob(tdr=self, job_id=job_id, check_interval=30).run()
        if completed:
            dataset_id = self.get_job_result(job_id)['id']
            logging.info(f"Successfully created dataset {dataset_name}: {dataset_id}")
            return dataset_id

    def create_table(self, table_dict: dict, dataset_id: str, relationship_dict: dict = {}) -> requests.Response:
        """input table dict should be {"name": "table_name", "columns": [...]}
        relationship dict should be {"name": "relationship_name", "from": {"table": "table_name", "column": "column_name"}, "to": {"table": "table_name", "column": "column_name"}}"""
        request_dict = {
            "description": "string",
            "changes": {
                "addTables": [
                    table_dict
                ],
                "addRelationships": [
                    relationship_dict
                ]
            }
        }
        uri = f'{self.TDR_LINK}/datasets/{dataset_id}/updateSchema'
        response = self.request_util.run_request(
            method=POST,
            uri=uri,
            content_type='application/json',
            data=json.dumps(request_dict)
        )
        return response
    
    def update_dataset_schema(self, dataset_uuid: str, update_note: str, tables_to_add: Optional[list[dict]]=None, relationships_to_add: Optional[list[dict]]=None, columns_to_add: Optional[list[dict]]=None) -> requests.Response:
        """Update dataset schema."""
        from tdr_api_schema.update_dataset_schema import update_schema
        uri = f"{self.TDR_LINK}/datasets/{dataset_uuid}/updateSchema"
        request_body = {"description": f"{update_note}", "changes": {}}
        if tables_to_add:
            request_body["changes"]["addTables"] = tables_to_add
        if relationships_to_add:
            request_body["changes"]["addRelationships"] = relationships_to_add
        if columns_to_add:
            request_body["changes"]["addColumns"] = columns_to_add
        try:
            update_schema.validate(request_body)
        except SchemaError as e:
            raise ValueError(f"Schema validation error: {e}")
        response = self.request_util.run_request(
            uri=uri,
            method=POST,
            content_type="application/json",
            data=request_body
        )
        return response


class Terra:
    TERRA_LINK = "https://api.firecloud.org/api"

    def __init__(self, request_util: Any):
        self.request_util = request_util

    def add_user_to_group(self, group: str, email: str, role: str = "member") -> None:
        url = f"{self.TERRA_LINK}/groups/{group}/{role}/{email}"
        self.request_util.run_request(
            uri=url,
            method=PUT
        )
        logging.info(f"Added {email} to group {group} as {role}")


class TerraWorkspace:
    TERRA_LINK = "https://api.firecloud.org/api"
    LEONARDO_LINK = "https://leonardo.dsde-prod.broadinstitute.org/api"
    WORKSPACE_LINK = "https://workspace.dsde-prod.broadinstitute.org/api/workspaces/v1"

    def __init__(self, billing_project: str, workspace_name: str, request_util: Any):
        self.billing_project = billing_project
        self.workspace_name = workspace_name
        self.workspace_id = None
        self.resource_id = None
        self.storage_container = None
        self.bucket = None
        self.wds_url = None
        self.account_url = None
        self.request_util = request_util

    def __repr__(self) -> str:
        return f"{self.billing_project}/{self.workspace_name}"

    def _yield_all_entity_metrics(self, entity: str, total_entities_per_page: int = 40000) -> Any:
        """Yield all entity metrics from workspace."""
        url = f"{self.TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/entityQuery/{entity}?pageSize={total_entities_per_page}"
        response = self.request_util.run_request(
            uri=url,
            method=GET,
            content_type='application/json'
        )
        first_page_json = response.json()
        yield first_page_json
        total_pages = first_page_json["resultMetadata"]["filteredPageCount"]
        logging.info(
            f"Looping through {total_pages} pages of data from workspace")

        for page in range(2, total_pages + 1):
            logging.info(f"Getting page {page} of {total_pages}")
            next_page = self.request_util.run_request(
                uri=url,
                method=GET,
                content_type='application/json',
                params={"page": page}
            )
            yield next_page.json()

    def get_workspace_info(self) -> dict:
        """Get workspace info."""
        url = f"{self.TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}"
        logging.info(
            f"Getting workspace ID for {self.billing_project}/{self.workspace_name}")
        response = self.request_util.run_request(uri=url, method=GET)
        return json.loads(response.text)

    def _set_resource_id_and_storage_container(self) -> None:
        """Get resource ID and storage container."""
        url = f"{self.WORKSPACE_LINK}/{self.workspace_id}/resources?offset=0&limit=10&resource=AZURE_STORAGE_CONTAINER"
        logging.info(
            f"Getting resource ID for {self.billing_project}/{self.workspace_name}")
        response = self.request_util.run_request(uri=url, method=GET)
        for resource_entry in response.json()["resources"]:
            storage_container_name = resource_entry["resourceAttributes"][
                "azureStorageContainer"]["storageContainerName"]
            # Check if storage container name is sc- and set resource ID and storage_container_name as bucket
            if storage_container_name.startswith("sc-"):
                self.resource_id = resource_entry["metadata"]["resourceId"]
                self.storage_container = storage_container_name
                return None

        raise ValueError(
            f"No resource ID found for {self.billing_project}/{self.workspace_name} - {self.workspace_id}: {json.dumps(response.json(), indent=4)}"
        )

    def set_azure_terra_variables(self) -> None:
        """Get all needed variables and set it for the class"""
        workspace_info = self.get_workspace_info()
        self.workspace_id = workspace_info["workspace"]["workspaceId"]
        self._set_resource_id_and_storage_container()
        self._set_account_url()
        self._set_wds_url()

    def _set_wds_url(self) -> None:
        """"Get url for wds."""
        uri = f"{self.LEONARDO_LINK}/apps/v2/{self.workspace_id}?includeDeleted=false"
        logging.info(
            f"Getting WDS URL for {self.billing_project}/{self.workspace_name}")
        response = self.request_util.run_request(uri=uri, method=GET)
        for entries in json.loads(response.text):
            if entries['appType'] == 'WDS' and entries['proxyUrls']['wds'] is not None:
                self.wds_url = entries['proxyUrls']['wds']
                return None
        raise ValueError(
            f"No WDS URL found for {self.billing_project}/{self.workspace_name} - {self.workspace_id}")

    def get_gcp_workspace_metrics(self, entity_type: str) -> list[dict]:
        """Get metrics for entity type in workspace."""
        results = []
        logging.info(
            f"Getting metrics for {self.billing_project}/{self.workspace_name}")
        full_entity_generator = self._yield_all_entity_metrics(
            entity=entity_type
        )
        for page in full_entity_generator:
            results.extend(page["results"])
        return results

    def _get_sas_token_json(self, sas_expiration_in_secs: int) -> dict:
        """Get SAS token JSON."""
        url = f"{self.WORKSPACE_LINK}/{self.workspace_id}/resources/controlled/azure/storageContainer/{self.resource_id}/getSasToken?sasExpirationDuration={str(sas_expiration_in_secs)}"
        response = self.request_util.run_request(uri=url, method=POST)
        return json.loads(response.text)

    def _set_account_url(self) -> None:
        """Set account URL for Azure workspace."""
        # Can only get account URL after setting resource ID and from getting a sas token
        sas_token_json = self._get_sas_token_json(sas_expiration_in_secs=1)
        parsed_url = urlparse(sas_token_json["url"])
        # Set url to be https://account_name.blob.core.windows.net
        self.account_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

    def retrieve_sas_token(self, sas_expiration_in_secs: int) -> str:
        """Retrieve SAS token for workspace."""
        sas_response_json = self._get_sas_token_json(
            sas_expiration_in_secs=sas_expiration_in_secs)
        return sas_response_json["token"]

    def set_workspace_id(self, workspace_info: dict) -> None:
        """Set workspace ID."""
        self.workspace_id = workspace_info["workspace"]["workspaceId"]

    def get_workspace_entity_info(self, use_cache: bool = True) -> dict:
        """Get workspace entity info."""
        use_cache = 'true' if use_cache else 'false'
        url = f"{self.TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/entities?useCache={use_cache}"
        response = self.request_util.run_request(uri=url, method=GET)
        return json.loads(response.text)

    def update_user_acl(
        self, email: str, access_level: str, can_share=False, can_compute=False
    ) -> dict:
        url = f"{self.TERRA_LINK}/workspaces/{self.billing_project}/{self.workspace_name}/acl"
        payload = {
            "email": email,
            "accessLevel": access_level,
            "canShare": can_share,
            "canCompute": can_compute,
        }
        logging.info(
            f"Updating user {email} to {access_level} in workspace {self.billing_project}/{self.workspace_name}")
        response = self.request_util.run_request(
            uri=url,
            method=PATCH,
            content_type='application/json',
            data="[" + json.dumps(payload) + "]"
        )
        request_json = response.json()
        if request_json["usersNotFound"]:
            # Will be a list of one user
            user_not_found = request_json["usersNotFound"][0]
            raise Exception(
                f'The user {user_not_found["email"]} was not found and access was not updated'
            )
        return request_json

    def create_workspace_attributes_ingest_dict(self, workspace_attributes: Optional[dict] = None):
        """Create ingest dictionary for workspace attributes."""
        if not workspace_attributes:
            workspace_attributes = self.get_workspace_info()["attributes"]

        ingest_dict = []
        for key, value in workspace_attributes.items():
            # If value is dict just use 'items' as value
            if isinstance(value, dict):
                value = value.get("items")
            # If value is list convert to comma seperated string
            if isinstance(value, list):
                value = ', '.join(value)
            ingest_dict.append({'attribute': key, 'value': value})
        return ingest_dict


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
                logging.info(f"Ingest {self.job_id} is still running")
                # Check every x seconds if ingest is still running
                time.sleep(self.check_interval)
            elif ingest_response.status_code == 200:
                response_json = json.loads(ingest_response.text)
                if response_json["job_status"] == "succeeded":
                    logging.info(f"Ingest {self.job_id} succeeded")
                    return True
                else:
                    logging.error(f"Ingest {self.job_id} failed")
                    job_result = self.tdr.get_job_result(self.job_id)
                    raise ValueError(
                        f"Status code {ingest_response.status_code}: {response_json}\n{job_result}")
            else:
                logging.error(f"Ingest {self.job_id} failed")
                job_result = self.tdr.get_job_result(self.job_id)
                raise ValueError(
                    f"Status code {ingest_response.status_code}: {ingest_response.text}\n{job_result}")


class StartIngest:
    def __init__(self, tdr: TDR, ingest_records: list[dict], target_table_name: str, dataset_id: str, load_tag: str, bulk_mode: bool, update_strategy: str):
        self.tdr = tdr
        self.ingest_records = ingest_records
        self.target_table_name = target_table_name
        self.dataset_id = dataset_id
        self.load_tag = load_tag
        self.bulk_mode = bulk_mode
        self.update_strategy = update_strategy

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

    def run(self) -> str:
        ingest_request = self._create_ingest_dataset_request()
        logging.info(f"Starting ingest to {self.dataset_id}")
        ingest_response = self.tdr.ingest_dataset(
            dataset_id=self.dataset_id, data=ingest_request)
        return ingest_response["id"]


class FilterOutSampleIdsAlreadyInDataset:
    def __init__(self, ingest_metrics: list[dict], dataset_id: str, tdr: TDR, target_table_name: str, filter_entity_id: str):
        self.ingest_metrics = ingest_metrics
        self.tdr = tdr
        self.dataset_id = dataset_id
        self.target_table_name = target_table_name
        self.filter_entity_id = filter_entity_id

    def run(self) -> list[dict]:
        # Get all sample ids that already exist in dataset
        logging.info(
            f"Getting all sample ids that already exist in dataset {self.dataset_id}")
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
                f"Filtered out {len(self.ingest_metrics) - len(filtered_ingest_metrics)} rows that already exist in dataset")
            if filtered_ingest_metrics:
                return filtered_ingest_metrics
            else:
                logging.info(
                    "All rows filtered out as they all exist in dataset, nothing to ingest")
                sys.exit(0)
        else:
            logging.info(
                "No rows were filtered out as they all do not exist in dataset")
            return filtered_ingest_metrics


class AzureBlobDetails:
    def __init__(self, account_url: str, sas_token: str, container_name: str):
        from azure.storage.blob import BlobServiceClient
        self.account_url = account_url
        self.sas_token = sas_token
        self.container_name = container_name
        self.blob_service_client = BlobServiceClient(
            account_url=self.account_url, credential=self.sas_token)

    def get_blob_details(self, max_per_page: int = 500) -> list[dict]:
        container_client = self.blob_service_client.get_container_client(
            self.container_name)
        details = []

        blob_list = container_client.list_blobs(results_per_page=max_per_page)
        page = blob_list.by_page()

        page_count = 0
        for blob_page in page:
            page_count += 1
            logging.info(
                f"Getting page {page_count} of max {max_per_page} blobs")
            for blob in blob_page:
                blob_client = container_client.get_blob_client(blob)
                props = blob_client.get_blob_properties()
                if not blob.name.endswith('/'):
                    md5_hash = base64.b64encode(props.content_settings.content_md5).decode(
                        'utf-8') if props.content_settings.content_md5 else ""
                    full_path = blob_client.url.replace(
                        f'?{self.sas_token}', '')
                    details.append(
                        {
                            'file_name': blob.name,
                            'file_path': full_path,
                            'content_type': props.content_settings.content_type,
                            'file_extension': os.path.splitext(blob.name)[1],
                            'size_in_bytes': props.size,
                            'md5_hash': md5_hash
                        }
                    )
        return details


class GCPCloudFunctions:
    """List contents of a GCS bucket. Does NOT take in a token and auths as current user"""
    def __init__(self, bucket_name: str):
        from google.cloud import storage
        from mimetypes import guess_type
        self.bucket_name = bucket_name
        self.client = storage.Client()

    @staticmethod
    def process_cloud_path(cloud_path):
        platform_prefix, remaining_url = str.split(str(cloud_path), '//')
        bucket_name = str.split(remaining_url, '/')[0]
        blob_name = "/".join(str.split(remaining_url, '/')[1:])

        path_components = {'platform_prefix': platform_prefix, 'bucket': bucket_name, 'blob_url': blob_name}
        return path_components

    def list_bucket_contents(self, file_extensions_to_ignore: list[str] = [], file_strings_to_ignore: list[str] = []) -> list[dict]:
        logging.info(f"Listing contents of bucket gs://{self.bucket_name}/")
        bucket = self.client.get_bucket(self.bucket_name)
        blobs = bucket.list_blobs()

        file_list = []
        for blob in blobs:
            if blob.name.endswith(tuple(file_extensions_to_ignore)):
                logging.info(f"Skipping file {blob.name}")
                continue
            if any(file_string in blob.name for file_string in file_strings_to_ignore):
                logging.info(f"Skipping file {blob.name}")
                continue
            file_info = {
                "file_name": blob.name,
                "file_path": f"gs://{self.bucket_name}/{blob.name}",
                "content_type": blob.content_type or guess_type(blob.name)[0] or "application/octet-stream",
                "file_extension": os.path.splitext(blob.name)[1],
                "size_in_bytes": blob.size,
                "md5_hash": blob.md5_hash
            }
            file_list.append(file_info)
        logging.info(f"Found {len(file_list)} files in bucket")
        return file_list


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
    def __init__(self, ingest_metadata: list[dict], cloud_type: str, storage_container: Optional[str] = None, sas_token_string: Optional[str] = None, file_list: bool = False, dest_file_path_flat: bool = False):
        self.file_list = file_list
        self.ingest_metadata = ingest_metadata
        self.cloud_type = cloud_type
        self.sas_token_string = sas_token_string
        self.file_prefix = {GCP: "gs://", AZURE: "https://"}[cloud_type]
        self.workspace_storage_container = storage_container
        self.dest_file_path_flat = dest_file_path_flat

    def _add_file_ref(self, file_details: dict) -> None:
        """Create file ref for ingest."""
        file_details['file_ref'] = {
            "sourcePath": file_details['file_path'],
            # https://some_url.blob.core.windows.net/container_name/dir/file.txt
            # Remove url and container name with. Result will be /dir/file.txt
            "targetPath": self._format_relative_tdr_path(file_details['file_path']),
            "description": f"Ingest of {file_details['file_path']}",
            "mime_type": file_details['content_type']
        }

    def _format_relative_tdr_path(self, cloud_path: str) -> str:
        """Format cloud path to TDR path."""
        if self.cloud_type == GCP:
            # Cloud path will be gs://bucket/path/to/file convert to /path/to/file
            relative_path = '/'.join(cloud_path.split('/')[3:])
        else:
            # Cloud path will be https://landing_zone/storage_account/path/to/file convert to /path/to/file
            split_path = cloud_path.split('/')
            file_path_storage_container = split_path[3]
            if file_path_storage_container != self.workspace_storage_container:
                raise ValueError(
                    f"{cloud_path} storage container {file_path_storage_container} does not match workspace storage container {self.workspace_storage_container}. SAS token will not work"
                )
            relative_path = '/' + '/'.join(split_path[4:])
        if self.dest_file_path_flat:
            return "/" + relative_path.replace("/", "_").replace("#", "").replace("?", "")

    def _check_and_format_file_path(self, column_value: str) -> Union[str, dict]:
        """Check if column value is a gs:// path and reformat to TDR's dataset relative path"""
        if isinstance(column_value, str):
            if column_value.startswith(self.file_prefix):
                # If azure sas token will be '?{sas_token}', if gcp it just be file path
                return {
                        "sourcePath": f"{column_value}{self.sas_token_string}" if self.cloud_type == AZURE else column_value,
                        "targetPath": self._format_relative_tdr_path(column_value)
                }
        return column_value

    def _reformat_metric(self, row_dict: dict) -> dict:
        """Reformat metric for ingest."""
        reformatted_dict = {}
        #  If a specific file list is provided, then add file ref. Different then all other ingests
        if self.file_list:
            self._add_file_ref(row_dict)
            reformatted_dict = row_dict
        else:
            for key, value in row_dict.items():
                # Ignore where there is no value
                if value:
                    # If it is a list go through each item and recreate list
                    if isinstance(value, list):
                        reformatted_dict[key] = [
                            self._check_and_format_file_path(item) for item in value
                        ]
                    reformatted_dict[key] = self._check_and_format_file_path(value)
        # add in timestamp
        reformatted_dict['last_modified_date'] = datetime.now(
            tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S")
        return reformatted_dict

    def run(self) -> list[dict]:
        return [
            self._reformat_metric(row_dict) for row_dict in self.ingest_metadata
        ]

      
class CompareTDRDataset:

    def compare_table(self, reference_dataset_table: dict, target_dataset_table: dict) -> list[dict]:
        """Compare tables between two datasets."""
        columns_to_update = []
        for column in reference_dataset_table['columns']:
            target_table_col_names = [col['name'] for col in target_dataset_table['columns']]
            if column not in target_dataset_table['columns']:
                column_dict = column 
                if column['name'] not in target_table_col_names:
                    column_dict['action'] = 'add'
                else:
                    column_dict['action'] = 'modify'
                columns_to_update.append(column_dict)
        return columns_to_update

    def compare_dataset_relationships(self, reference_dataset_relationships, target_dataset_relationships) -> list[dict]:
        dataset_relationships_to_modify = []
        for dataset in reference_dataset_relationships:
            if dataset not in target_dataset_relationships:
                dataset_relationships_to_modify.append(dataset)
        return dataset_relationships_to_modify


class BatchIngest:
    def __init__(self, ingest_metadata: list[dict], tdr: TDR, target_table_name: str, dataset_id: str, batch_size: int, file_list_bool: bool,
                 bulk_mode: bool, cloud_type: str, terra_workspace: Optional[TerraWorkspace] = None, update_strategy: str = "replace",
                 waiting_time_to_poll: int = 60, sas_expire_in_secs: int = 3600, test_ingest: bool = False, load_tag: Optional[str] = None,
                 dest_file_path_flat: bool = False):
        self.ingest_metadata = ingest_metadata
        self.tdr = tdr
        self.target_table_name = target_table_name
        self.dataset_id = dataset_id
        self.cloud_type = cloud_type
        self.terra_workspace = terra_workspace  # This is only used if ingesting Azure data where you need to create sas tokens from workspace
        self.batch_size = batch_size
        self.update_strategy = update_strategy
        self.bulk_mode = bulk_mode
        self.waiting_time_to_poll = waiting_time_to_poll
        self.sas_expire_in_secs = sas_expire_in_secs
        self.test_ingest = test_ingest  # Used if you want to run first batch and then exit after success
        self.load_tag = load_tag
        self.file_list_bool = file_list_bool
        self.dest_file_path_flat = dest_file_path_flat

    def _reformat_metadata(self, metrics_batch: list[dict]) -> list[dict]:
        if self.cloud_type == AZURE:
            sas_token = self.terra_workspace.retrieve_sas_token(sas_expiration_in_secs=self.sas_expire_in_secs)
            cloud_container = self.terra_workspace.storage_container
            return ReformatMetricsForIngest(
                ingest_metadata=metrics_batch,
                cloud_type=self.cloud_type,
                storage_container=cloud_container,
                sas_token_string=sas_token,
                file_list=self.file_list_bool,
                dest_file_path_flat=self.dest_file_path_flat
            ).run()
        elif self.cloud_type == GCP:
            return ReformatMetricsForIngest(
                ingest_metadata=metrics_batch,
                cloud_type=self.cloud_type,
                file_list=self.file_list_bool,
                dest_file_path_flat=self.dest_file_path_flat
            ).run()
        else:
            raise ValueError(f"Cloud type {self.cloud_type} not supported. Must be {GCP} or {AZURE}")

    def run(self) -> None:
        logging.info(f"Batching {len(self.ingest_metadata)} total rows into batches of {self.batch_size} for ingest")
        total_batches = len(self.ingest_metadata) // self.batch_size + 1
        for i in range(0, len(self.ingest_metadata), self.batch_size):
            batch_number = i // self.batch_size + 1
            logging.info(f"Starting ingest batch {batch_number} of {total_batches}")
            metrics_batch = self.ingest_metadata[i:i + self.batch_size]

            reformatted_batch = self._reformat_metadata(metrics_batch)

            if self.load_tag:
                load_tag = self.load_tag
            else:
                load_tag = f"{self.dataset_id}.{self.target_table_name}"

            # Start actual ingest
            ingest_id = StartIngest(
                tdr=self.tdr,
                ingest_records=reformatted_batch,
                target_table_name=self.target_table_name,
                dataset_id=self.dataset_id,
                load_tag=load_tag,
                bulk_mode=self.bulk_mode,
                update_strategy=self.update_strategy
            ).run()
            # monitor ingest until completion
            MonitorTDRJob(
                tdr=self.tdr,
                job_id=ingest_id,
                check_interval=self.waiting_time_to_poll
            ).run()
            logging.info(f"Completed batch ingest of {len(reformatted_batch)} rows")
            if self.test_ingest:
                logging.info("First batch completed, exiting since test_ingest was used")
                sys.exit(0)
        logging.info("Ingest complete")


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
                self.tdr_row_id: row['name'],
                **{k: v for k, v in row['attributes'].items() if k not in self.columns_to_ignore}
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
                [v for v in values_for_header if v is not None][0])

            # check if all the values in the list that are non-none match the type of the first entry
            all_values_matching = all(
                type(v) == type_to_match_against for v in values_for_header if v is not None)
            matching.append({header: all_values_matching})

        # Returns true if all headers are determined to be "matching"
        problematic_headers = [list(d.keys())
                               for d in matching if not list(d.values())[0]]
        if problematic_headers:
            raise Exception(
                f"Not all values for the following headers are of the same type: {problematic_headers}")

    def _python_type_to_tdr_type_conversion(self, value_for_header: Any) -> str:
        az_filref_regex = "^https.*sc-.*"
        gcp_fileref_regex = "^gs.*"

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
                "data_type": data_type,
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
                header_requirements.append(
                    {"name": header, "required": False, "data_type": "string"})
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
        # create the dataframe
        metadata_df = pd.DataFrame(self.input_metadata)

        # find all headers that need to be renamed if they have "entity" in them and rename the headers
        headers_to_be_renamed = [{h: h.split(":")[1] for h in list(metadata_df.columns) if h.startswith("entity")}][0]
        metadata_df = metadata_df.rename(columns=headers_to_be_renamed)

        # start by gathering the column metadata by determining which headers are required or not
        column_metadata = self._gather_required_and_non_required_headers(metadata_df, list(metadata_df.columns))

        # drop columns where ALL values are None, but keep rows where some values are None
        # we keep the rows where some values are none because if we happen to have a different column that's none in
        # every row, we could end up with no data at the end
        all_none_columns_dropped_df = metadata_df.dropna(axis=1, how="all")
        cleaned_metadata = all_none_columns_dropped_df.to_dict(
            orient="records")
        key_value_type_mappings = self._reformat_metadata(cleaned_metadata)

        # check to see if all values corresponding to a header are of the same type
        self._check_type_consistency(key_value_type_mappings)

        columns = self._format_column_metadata(key_value_type_mappings)

        # combine the information about required headers with the data types that were collected
        for header_metadata in column_metadata:
            matching_metadata = [
                d for d in columns if d["name"] == header_metadata["name"]]
            if matching_metadata:
                header_metadata.update(matching_metadata[0])

        tdr_tables_json = {
            "tables": [
                {
                    "name": self.table_name,
                    "columns": column_metadata,
                }

            ]
        }
        return tdr_tables_json



class GetPermissionsForWorkspaceIngest:
    def __init__(self, terra_workspace: TerraWorkspace, terra: Terra, dataset_info: dict):
        self.terra_workspace = terra_workspace
        self.dataset_info = dataset_info
        self.terra = terra

    def run(self):
        # Ensure dataset SA account is reader on Terra workspace.
        tdr_sa_account = self.dataset_info['ingestServiceAccount']
        self.terra_workspace.update_user_acl(email=tdr_sa_account, access_level='READER')
        print("here")

        # Check if workspace has auth domain
        workspace_info = self.terra_workspace.get_workspace_info()
        auth_domain = workspace_info['workspace']['authorizationDomain']
        # Attempt to add tdr_sa_account to auth domain
        if auth_domain:
            # List of 1 dict with key 'membersGroupName' and value of auth domain group name
            auth_domain_str = auth_domain[0]['membersGroupName']
            try:
                self.terra.add_user_to_group(email=tdr_sa_account, group=auth_domain_str)
            except Exception as e:
                logging.warning(
                    f"Could not add {tdr_sa_account} to auth domain {auth_domain_str}. Assuming it is already added.")