import pytest
import json
import pathlib
from typing import Any


from python.utils.tdr_utils.tdr_api_utils import TDR
from python.utils.tdr_utils.tdr_table_utils import SetUpTDRTables
from python.utils.tdr_utils.tdr_schema_utils import InferTDRSchema
from python.utils.tdr_utils.tdr_ingest_utils import BatchIngest
from python.utils.token_util import Token
from python.utils.request_util import RunRequest

def tdr_test_resource_json() -> dict:
    resource_json = pathlib.Path(__file__).parent.joinpath("tdr_resources.json")
    json_data = json.loads(resource_json.read_text())
    return json_data

@pytest.fixture()
def tdr_client() -> Any:
    token = Token(cloud='gcp') 
    requestclient = RunRequest(token)
    return TDR(request_util=requestclient)



class TestGetUtils:
    @pytest.fixture(autouse=True)
    def _get_tdr_client(self, tdr_client):
        self.tdr_client = tdr_client

    def test_get_data_set_files(self):
        test_data = tdr_test_resource_json()['tests']['get_data_set_files']
        file_list = self.tdr_client.get_data_set_files(dataset_id=test_data['function_input'])
        assert len(file_list) == 20


    def test_create_file_dict(self):
        cmd = self.tdr_client.create_file_dict(dataset_id='str')

    def test_get_snapshot_info(self):
        cmd = self.tdr_client.get_snapshot_info(snapshot_id=str)

    
    def test_check_if_dataset_exists(self):
        cmd = self.tdr_client.check_if_dataset_exists(dataset_name=str, billing_profile=str)

    def test_get_dataset_info(self):
        cmd = self.tdr_client.get_dataset_info(dataset_id=str)

    def test_get_table_schema_info(self):
        cmd = self.tdr_client.get_table_schema_info(dataset_id=str, table_name=str)

    def test_get_data_set_table_metrics(self):
        cmd = self.tdr_client.get_data_set_table_metrics(dataset_id=str, target_table_name=str)

    def test_get_data_set_sample_ids(self):
        cmd = self.tdr_client.get_data_set_sample_ids(dataset_id=str, target_table_name=str, entity_id=str)

    def test_get_data_set_file_uuids_from_metadata(self):
        cmd = self.tdr_client.get_data_set_file_uuids_from_metadata(dataset_id=str)

    def test_get_files_from_snapshot(self):
        cmd = self.tdr_client.get_files_from_snapshot(snapshot_id=str)

    def test_InferTDRSchema(self):
        cmd = InferTDRSchema().infer_schema()


class TestCreateUtils:

    @pytest.fixture(autouse=True)
    def _get_tdr_client(self, tdr_client):
        self.tdr_client = tdr_client

    def test_add_user_to_dataset(self):
        cmd = self.tdr_client.add_user_to_dataset(dataset_id=str, user=str, policy=str)

    def test_ingest_to_dataset(self):
        cmd = self.tdr_client.ingest_to_dataset(dataset_id=str, data=dict)

    def test_get_or_create_dataset(self):
        cmd = self.tdr_client.get_or_create_dataset(
            dataset_name=str,
            billing_profile=str,
            schema=dict,
            description=str,
            cloud_platform=str
        )

    def test_update_dataset_schema(self):
        cmd = self.tdr_client.update_dataset_schema(  # type: ignore[return]
            dataset_id=str,
            update_note=str,
            tables_to_add="Optional[list[dict]]",
            relationships_to_add='Optional[list[dict]]',
            columns_to_add='Optional[list[dict]]'
        )

    def test_SetUpTDRTables(self):
        cmd = SetUpTDRTables().run()



class TestDeleteUtils:

    @pytest.fixture(autouse=True)
    def _get_tdr_client(self, tdr_client):
        self.tdr_client = tdr_client

    def test_delete_file(self):
        cmd = self.tdr_client.delete_file(file_id='str', dataset_id='str')

    def test_delete_files(self):
        cmd = self.tdr_client.delete_files(file_ids=list[str], dataset_id=str)

    def test_delete_dataset(self):
        cmd = self.tdr_client.delete_dataset(dataset_id=str)

    def test_delete_snapshots(self):
        cmd = self.tdr_client.delete_snapshots(snapshot_ids=list[str])

    def test_delete_snapshot(self):
        cmd = self.tdr_client.delete_snapshot(snapshot_id=str)




