import logging

from ops_utils.terra_util import TerraWorkspace
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token
from ops_utils.tdr_utils.tdr_api_utils import TDR
from argparse import ArgumentParser, Namespace
from datetime import datetime
from ops_utils.tdr_utils.tdr_table_utils import MatchSchemas
from ops_utils.tdr_utils.tdr_bq_utils import TdrBq, GetTdrAssetInfo
from ops_utils.tdr_utils.tdr_ingest_utils import BatchIngest
from utils.copy_dataset_or_snapshot_files import CopyDatasetOrSnapshotFiles
from ops_utils.gcp_utils import GCPCloudFunctions
from typing import Optional


logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

# Must be part of monster-dev@dev.test.firecloud.org group to use billing profile
DEV_BILLING_PROFILE = "390e7a85-d47f-4531-b612-165fc977d3bd"

# Column data types
FILE_REF_TYPE = "fileref"
TIMESTAMP_TYPE = "timestamp"

# Default download type for files
DOWNLOAD_TYPE = "structured"

# Default values for batch ingest
INGEST_BATCH_SIZE = 1000
INGEST_UPDATE_STRATEGY = "replace"
INGEST_WAITING_TIME_TO_POLL = 30  # seconds

# Snapshot type for creating a snapshot
SNAPSHOT_TYPE = "byFullView"


class CreateAndSetUpDataset:
    def __init__(
            self,
            orig_dataset_info: dict,
            new_tdr: TDR,
            snapshot_info: dict,
            continue_if_exists: bool
    ):
        """
        Initialize the CreateAndSetUpDataset class.

        Args:
            orig_dataset_info (dict): The original dataset information to be copied.
            new_tdr (TDR): The new TDR instance where the dataset will be created.
            snapshot_info (dict): Information about the snapshot to be used for creating the new dataset.
            continue_if_exists (bool): If True, will continue without error if the dataset already exists.
        """
        self.orig_dataset_info = orig_dataset_info
        self.new_tdr = new_tdr
        self.continue_if_exists = continue_if_exists
        self.snapshot_info = snapshot_info

    def _create_additional_properties(self) -> dict:
        """
        Create additional properties for the new dataset based on the original dataset and snapshot information.

        Returns:
            dict: A dictionary containing additional properties for the new dataset.
        """
        additional_properties = {
            "experimentalSelfHosted": self.snapshot_info["selfHosted"],
            "dedicatedIngestServiceAccount": True,
            "experimentalPredictableFileIds": False,
            "enableSecureMonitoring": self.snapshot_info["secureMonitoringEnabled"],
        }
        if self.orig_dataset_info.get('phsId'):
            additional_properties['phsId'] = self.orig_dataset_info['phsId']
        if self.orig_dataset_info.get('tags'):
            additional_properties['tags'] = self.orig_dataset_info['tags']
        if self.orig_dataset_info.get('properties'):
            additional_properties['properties'] = self.orig_dataset_info['properties']
        return additional_properties

    def run(self) -> str:
        additional_properties = self._create_additional_properties()

        dest_dataset_id = self.new_tdr.get_or_create_dataset(
            dataset_name=self.orig_dataset_info['name'],
            billing_profile=new_billing_profile,
            schema=self.snapshot_info['schema'],
            description=self.orig_dataset_info['description'],
            additional_properties_dict=additional_properties,
            continue_if_exists=self.continue_if_exists
        )
        dest_dataset_info = self.new_tdr.get_dataset_info(dest_dataset_id).json()

        # Check if schema matches and update if needed. Only will be possibly updated if dataset already exists
        MatchSchemas(
            # Dict set up the same as dataset info
            orig_dataset_info=self.snapshot_info,
            dest_dataset_info=dest_dataset_info,
            dest_dataset_id=dest_dataset_id,
            tdr=self.new_tdr
        ).run()
        return dest_dataset_id


class GetLatestSnapsShotInfoAndUpdatePaths:
    def __init__(self, tdr: TDR, dataset_id: str, workspace_bucket: str):
        """
        Initialize the GetSnapsShotInfo class.

        Args:
            tdr (TDR): The TDR instance to interact with the Terra Data Repository.
            dataset_id (str): The ID of the dataset to retrieve snapshot information from.
            workspace_bucket (str): The GCS bucket where files will be copied. Will not include gs:// prefix.
        """
        self.tdr = tdr
        self.dataset_id = dataset_id
        self.workspace_bucket = workspace_bucket

    def _get_latest_snapshot(self) -> dict:
        """ Retrieve the latest snapshot for the given dataset.

        Returns:
            dict: The latest snapshot information.
        """
        snapshots = self.tdr.get_dataset_snapshots(dataset_id=dataset_id)
        latest_snapshot = None
        latest_date = None
        for snapshot in snapshots.json()['items']:
            created_date = snapshot.get('createdDate')
            if created_date:
                created_dt = datetime.fromisoformat(created_date.replace('Z', '+00:00'))
                if latest_date is None or created_dt > latest_date:
                    latest_date = created_dt
                    latest_snapshot = snapshot
        if latest_snapshot:
            return latest_snapshot
        raise ValueError(f"No snapshots found for dataset {self.dataset_id}")

    def _create_snapshot_file_dict(self, snapshot_id: str) -> dict:
        """
        Create a dictionary of files in the latest snapshot.

        Returns:
            dict: A dictionary where keys are file IDs and values are file metadata.
        """
        return {
            file_dict['fileId']: file_dict
            for file_dict in self.tdr.get_files_from_snapshot(snapshot_id=snapshot_id)
        }

    def _create_updated_paths(self, drs_id: str, snapshot_file_dict: dict) -> Optional[str]:
        """
        Given a DRS ID, returns the updated file path in the workspace bucket.

        Args:
            drs_id (str): The DRS ID of the file.
            snapshot_file_dict (dict): A dictionary of files in the snapshot, where keys are file IDs.
        Returns:
            Optional[str]: The updated file path in the workspace bucket, or None if the file ID is not found.
        """
        # drs id expected format like below where the last part is the file id
        # drs://data.terra.bio/v1_0533186c-f440-454b-b9cc-7fca2e2bdbf2_87239d82-45e7-4d59-8014-a5304f8d4e34
        file_id = drs_id.split('_')[-1] if drs_id else None
        if file_id in snapshot_file_dict:
            file_metadata = snapshot_file_dict[file_id]
            return f"gs://{self.workspace_bucket}/{file_metadata['path'].lstrip('/')}"
        logging.info(f"File {file_id} not found in snapshot files list")
        return None

    def _get_table_contents_with_file_refs(self, table: dict, tdr_bq: TdrBq, snapshot_file_dict: dict) -> list[dict]:
        """
        Given a table dict, snapshot_info, and snapshot_file_dict, returns the table contents with file reference columns updated.
        """
        table_name = table['name']
        file_ref_columns = [
            column['name']
            for column in table['columns']
            if column['datatype'] == FILE_REF_TYPE
        ]
        timestamp_columns = [
            column['name']
            for column in table['columns']
            if column['datatype'] == TIMESTAMP_TYPE
        ]
        table_contents = tdr_bq.get_tdr_table_contents(
            table_name=table_name,
            exclude_datarepo_id=True,
            to_dataframe=False
        )
        for row in table_contents:
            for column, value in row.items():
                if column in file_ref_columns:
                    drs_id = row[column]
                    # Update file reference paths to point to the temp workspace bucket
                    row[column] = self._create_updated_paths(drs_id=drs_id, snapshot_file_dict=snapshot_file_dict)
                elif column in timestamp_columns:
                    # Convert timestamp to ISO format if it's a datetime object
                    row[column] = value.isoformat() if isinstance(value, datetime) else value
        return table_contents

    @staticmethod
    def _remove_extra_columns_from_table(table_dict: dict) -> dict:
        """
        Remove some filled in columns that are not needed.
        """
        table_dict['columns'] = [
            col
            for col in table_dict['columns']
            if col['name'] not in ['partitionMode', 'datePartitionOptions', 'intPartitionOptions', 'rowCount']
        ]
        return table_dict

    def run(self) -> tuple[dict, dict]:
        latest_snapshot = self._get_latest_snapshot()
        snapshot_id = latest_snapshot['id']
        snapshot_file_dict = self._create_snapshot_file_dict(snapshot_id)

        # Get full snapshot info including schema and relationships
        snapshot_full_info = GetTdrAssetInfo(tdr=self.tdr, snapshot_id=snapshot_id).run()

        # Have snapshot info schema mimic what dataset schema dict looks like
        latest_snapshot['schema'] = {
            'tables': [
                self._remove_extra_columns_from_table(table_dict=table_dict)
                for table_dict in snapshot_full_info['tables']
            ]
        }
        latest_snapshot['schema']['relationships'] = snapshot_full_info['relationships']

        # Get the BigQuery schema for the snapshot
        tdr_bq = TdrBq(
            project_id=snapshot_full_info['bq_project'],
            bq_schema=snapshot_full_info['bq_schema']
        )
        all_table_contents = {}
        # Get the contents of each table in the snapshot from big query
        # and updating file references to point to where they will live in temp workspace bucket
        for table in snapshot_full_info['tables']:
            all_table_contents[table['name']] = self._get_table_contents_with_file_refs(
                table=table,
                tdr_bq=tdr_bq,
                snapshot_file_dict=snapshot_file_dict
            )
        return latest_snapshot, all_table_contents


def get_args() -> Namespace:
    parser = ArgumentParser(description="Get files that are not in the dataset metadata")
    parser.add_argument("--billing_project", "-b", required=True)
    parser.add_argument("--workspace_name", "-w", required=True)
    parser.add_argument("--dataset_id", "-d", required=True)
    parser.add_argument("--orig_env", "-oe", required=True,
                        choices=['prod', 'dev'], help="Environment of the original dataset. Will copy to the other environment")
    parser.add_argument("--new_billing_profile", "-nb",
                        help="Only needed if going dev -> prod")
    parser.add_argument("--continue_if_exists", "-c", action="store_true",
                        help="If the dataset already exists, continue without error")
    parser.add_argument("--delete_temp_workspace", "-dtw", action="store_true",)
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="If set, will print additional information during the copy process")
    parser.add_argument("--service_account_json", "-saj", type=str,
                        help="Path to the service account JSON file. If not provided, will use the default credentials.")
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()
    billing_project = args.billing_project
    workspace_name = args.workspace_name
    dataset_id = args.dataset_id
    orig_env = args.orig_env
    new_billing_profile = args.new_billing_profile
    continue_if_exists = args.continue_if_exists
    delete_temp_workspace = args.delete_temp_workspace
    service_account_json = args.service_account_json
    verbose = args.verbose

    # Set src and dest environment
    new_env = 'dev' if orig_env == 'prod' else 'prod'
    if new_env == 'dev':
        new_billing_profile = DEV_BILLING_PROFILE
    if new_env == 'prod' and not new_billing_profile:
        raise ValueError("Must provide new_billing_profile when copying from dev to prod")

    token = Token(
        service_account_json=service_account_json
    )
    request_util = RunRequest(token=token)

    orig_tdr = TDR(request_util=request_util, env=orig_env)
    new_tdr = TDR(request_util=request_util, env=new_env)

    terra_workspace = TerraWorkspace(
        billing_project=billing_project,
        workspace_name=workspace_name,
        request_util=request_util,
        env=new_env,
    )

    # Create temp workspace to copy files to
    terra_workspace.create_workspace(
        continue_if_exists=continue_if_exists
    )
    workspace_bucket = terra_workspace.get_workspace_bucket()
    logging.info(f"Temp workspace bucket: {workspace_bucket}")

    # Get original dataset info
    orig_dataset_info = orig_tdr.get_dataset_info(dataset_id=dataset_id).json()
    if not orig_dataset_info:
        raise ValueError(f"Dataset with ID {dataset_id} not found in the original environment ({orig_env})")
    # Get the latest snapshot info from the original dataset
    # Will return updated table contents where files are pointing to the temp bucket
    snapshot_info, all_table_contents = GetLatestSnapsShotInfoAndUpdatePaths(
        tdr=orig_tdr,
        dataset_id=dataset_id,
        workspace_bucket=workspace_bucket
    ).run()

    # Copy files from the original dataset snapshot to the new workspace bucket
    CopyDatasetOrSnapshotFiles(
        tdr=orig_tdr,
        snapshot_id=snapshot_info['id'],
        output_bucket=workspace_bucket,
        download_type=DOWNLOAD_TYPE,
        gcp_functions=GCPCloudFunctions(service_account_json=service_account_json),
        verbose=verbose
    ).run()

    # Create a new dataset in the new environment
    dest_tdr_id = CreateAndSetUpDataset(
        orig_dataset_info=orig_dataset_info,
        new_tdr=new_tdr,
        snapshot_info=snapshot_info,
        continue_if_exists=continue_if_exists
    ).run()

    # Add the ingest service account to the workspace
    dest_dataset_info = new_tdr.get_dataset_info(dest_tdr_id).json()
    ingest_account = dest_dataset_info.get('ingestServiceAccount')
    terra_workspace.update_user_acl(
        email=ingest_account,
        access_level="READER",
    )

    # Ingest the updated table contents into the dataset in new environment
    for table_name, table_contents in all_table_contents.items():
        BatchIngest(
            tdr=new_tdr,
            ingest_metadata=table_contents,
            target_table_name=table_name,
            dataset_id=dest_tdr_id,
            batch_size=INGEST_BATCH_SIZE,
            bulk_mode=False,
            update_strategy=INGEST_UPDATE_STRATEGY,
            waiting_time_to_poll=INGEST_WAITING_TIME_TO_POLL
        ).run()

    # Create a snapshot in the new environment
    new_tdr.create_snapshot(
        snapshot_name=snapshot_info['name'],
        description=snapshot_info['description'],
        consent_code=snapshot_info['consentCode'],
        duos_id=snapshot_info.get('duosId'),
        dataset_name=orig_dataset_info['name'],
        snapshot_mode=SNAPSHOT_TYPE,
        profile_id=new_billing_profile,
    )

    # If delete_temp_workspace is set, delete the temp workspace
    if delete_temp_workspace:
        logging.info("Deleting temp workspace")
        terra_workspace.delete_workspace()
