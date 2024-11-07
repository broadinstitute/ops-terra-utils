import pytest
import json
import pathlib
from typing import Any


from python.utils.tdr_utils.tdr_api_utils import TDR
from python.utils.tdr_utils.tdr_schema_utils import InferTDRSchema
from python.utils.tdr_utils.tdr_ingest_utils import BatchIngest
from python.utils.tdr_utils.tdr_job_utils import MonitorTDRJob
from python.utils.token_util import Token
from python.utils.request_util import RunRequest


@pytest.fixture()
def tdr_test_resource_json() -> dict:
    resource_json = pathlib.Path(__file__).parent.joinpath("tdr_resources.json")
    json_data = json.loads(resource_json.read_text())
    return json_data


@pytest.fixture()
def tdr_client() -> Any:
    token = Token(cloud='gcp')
    requestclient = RunRequest(token)
    return TDR(request_util=requestclient)


@pytest.fixture(scope="session", autouse=True)
def ensure_tmp_dataset_deleted(tdr_client, tdr_test_resource_json) -> None:
    tdr = tdr_client
    test_info = tdr_test_resource_json
    dataset_name = "tmp_ops_integration_test_dataset_to_delete"
    profile_id = test_info['tests']['test_delete_dataset']['function_input']['billing_profile']
    dataset_info = tdr.check_if_dataset_exists(dataset_name=dataset_name, billing_profile=profile_id)
    if dataset_info:
        print(f"dataset info found: {dataset_info}")
        dataset_id = dataset_info[0]['id']
        tdr.delete_dataset(dataset_id=dataset_id)


class TestGetUtils:

    @pytest.fixture(autouse=True)
    def _get_tdr_client(self, tdr_client: Any, tdr_test_resource_json: Any) -> None:
        self.tdr_client = tdr_client
        self.test_info = tdr_test_resource_json

    def test_get_data_set_files(self) -> None:
        test_data = self.test_info['tests']['get_data_set_files']
        file_list = self.tdr_client.get_data_set_files(dataset_id=test_data['function_input'])
        assert len(file_list) == 20

    def test_create_file_dict(self) -> None:
        test_data = self.test_info['tests']['create_file_dict']
        file_dict = self.tdr_client.create_file_dict(dataset_id=test_data['function_input'])
        assert len(file_dict.keys()) == 20

    def test_get_snapshot_info(self) -> None:
        test_data = self.test_info['tests']['get_snapshot_info']
        cmd = self.tdr_client.get_snapshot_info(snapshot_id=test_data['function_input'])
        assert cmd['name'] == 'integration_test_snapshot'

    def test_check_if_dataset_exists(self) -> None:
        test_data = self.test_info['tests']['check_if_dataset_exists']
        dataset_exists = self.tdr_client.check_if_dataset_exists(
            dataset_name=test_data['function_input']['dataset_name'],
            billing_profile=test_data['function_input']['billing_profile'])
        dataset_dose_not_exist = self.tdr_client.check_if_dataset_exists(
            dataset_name="fake_dataset", billing_profile='not_real_billing_profile')
        assert dataset_exists and not dataset_dose_not_exist

    def test_get_dataset_info(self) -> None:
        test_data = self.test_info['tests']['get_dataset_info']
        cmd = self.tdr_client.get_dataset_info(dataset_id=test_data['function_input'])
        assert cmd['name'] == 'ops_integration_test_dataset'

    def test_get_table_schema_info(self) -> None:
        test_data = self.test_info['tests']['get_table_schema_info']
        cmd = self.tdr_client.get_table_schema_info(
            dataset_id=test_data['function_input']['dataset_id'], table_name=test_data['function_input']['table_name'])
        assert cmd['name'] == 'samples' and len(cmd['columns']) == 12

    def test_get_data_set_table_metrics(self) -> None:
        test_data = self.test_info['tests']['get_data_set_table_metrics']
        cmd = self.tdr_client.get_data_set_table_metrics(
            dataset_id=test_data['function_input']['dataset_id'],
            target_table_name=test_data['function_input']['table_name'])
        assert len(cmd) == 10

    def test_get_data_set_sample_ids(self) -> None:
        test_data = self.test_info['tests']['get_data_set_sample_ids']
        cmd = self.tdr_client.get_data_set_sample_ids(
            dataset_id=test_data['function_input']['dataset_id'],
            target_table_name=test_data['function_input']['table_name'],
            entity_id=test_data['function_input']['entity_id'])
        assert len(cmd) == 10

    def test_get_data_set_file_uuids_from_metadata(self) -> None:
        test_data = self.test_info['tests']['get_data_set_file_uuids_from_metadata']
        self.tdr_client.get_data_set_file_uuids_from_metadata(dataset_id=test_data['function_input'])

    def test_get_files_from_snapshot(self) -> None:
        test_data = self.test_info['tests']['get_files_from_snapshot']
        self.tdr_client.get_files_from_snapshot(snapshot_id=test_data['function_input'])

    def test_InferTDRSchema(self) -> None:
        # input_metadata: list[dict], table_name
        test_data = self.test_info['tests']['InferTDRSchema']
        cmd = InferTDRSchema(input_metadata=test_data['function_input']['input_metadata'],
                             table_name=test_data['function_input']['table_name']).infer_schema()
        assert cmd['name'] == 'samples' and len(cmd['columns']) == 13


class TestCreateUtils:

    @pytest.fixture(autouse=True)
    def _get_tdr_client(self, tdr_client: Any, tdr_test_resource_json: Any) -> None:
        self.tdr_client = tdr_client
        self.test_info = tdr_test_resource_json

    def test_get_or_create_dataset(self) -> None:
        test_data = self.test_info['tests']['test_get_or_create_dataset']

        def get_existing_dataset() -> None:
            cmd = self.tdr_client.get_or_create_dataset(
                dataset_name=test_data['existing_dataset']['function_input']['dataset_name'],
                billing_profile=test_data['existing_dataset']['function_input']['billing_profile'],
                schema=test_data['existing_dataset']['function_input']['schema'],
                description=test_data['existing_dataset']['function_input']['description'],
                cloud_platform=test_data['existing_dataset']['function_input']['cloud_platform']
            )
            assert cmd == '0981274b-61e3-4efb-99f2-eaea57075612'

        def create_new_dataset() -> None:
            self.tdr_client.get_or_create_dataset(
                dataset_name=test_data['new_dataset']['function_input']['dataset_name'],
                billing_profile=test_data['new_dataset']['function_input']['billing_profile'],
                schema=test_data['new_dataset']['function_input']['schema'],
                description=test_data['new_dataset']['function_input']['description'],
                cloud_platform=test_data['new_dataset']['function_input']['cloud_platform']
            )

        get_existing_dataset()
        create_new_dataset()

    def test_add_user_to_dataset(self) -> None:
        test_data = self.test_info['tests']['test_add_user_to_dataset']
        self.tdr_client.add_user_to_dataset(
            dataset_id=test_data['function_input']['dataset_id'],
            user=test_data['function_input']['user'],
            policy=test_data['function_input']['policy'])

    def test_update_dataset_schema(self) -> None:
        test_data = self.test_info['tests']['test_update_dataset_schema']
        dataset_info = self.tdr_client.check_if_dataset_exists(
            dataset_name=test_data['function_input']['dataset_name'],
            billing_profile=test_data['function_input']['billing_profile'])
        dataset_id = dataset_info[0]['id'] if dataset_info else None
        if dataset_id:
            self.tdr_client.update_dataset_schema(  # type: ignore[return]
                dataset_id=dataset_id,
                update_note=test_data['function_input']['update_note'],
                tables_to_add=test_data['function_input']['tables_to_add']
            )

    def test_batch_ingest_to_dataset(self) -> None:
        test_data = self.test_info['tests']['test_batch_ingest']['metadata_ingest']
        dataset_info = self.tdr_client.check_if_dataset_exists(
            dataset_name=test_data['function_input']['dataset_name'],
            billing_profile=test_data['function_input']['billing_profile'])
        dataset_id = dataset_info[0]['id'] if dataset_info else None
        if dataset_id:
            BatchIngest(
                ingest_metadata=test_data['function_input']['ingest_metadata'],
                tdr=self.tdr_client,
                target_table_name=test_data['function_input']['target_table_name'],
                dataset_id=dataset_id,
                batch_size=test_data['function_input']['batch_size'],
                bulk_mode=test_data['function_input']['bulk_mode'],
                cloud_type=test_data['function_input']['cloud_type']
            ).run()

    def test_ingest_files(self) -> None:
        test_data = self.test_info['tests']['test_batch_ingest']['file_ingest']
        self.tdr_client.file_ingest_to_dataset(dataset_id=test_data['function_input']['dataset_id'],
                                               profile_id=test_data['function_input']['profileId'],
                                               file_list=test_data['function_input']['ingest_files'],
                                               load_tag=test_data['function_input']['load_tag'])


class TestDeleteUtils:

    @pytest.fixture(autouse=True)
    def _get_tdr_client(self, tdr_client: Any, tdr_test_resource_json: Any) -> None:
        self.tdr_client = tdr_client
        self.test_info = tdr_test_resource_json

    def test_delete_files(self) -> None:
        test_data = self.test_info['tests']['test_delete_files']
        file_list = self.tdr_client.get_data_set_files(dataset_id=test_data['function_input']['dataset_id'])
        deletion_list = [file['fileId']
                         for file in file_list if file['fileDetail']['loadTag'] == 'integration_test_file_load']
        self.tdr_client.delete_files(file_ids=deletion_list, dataset_id=test_data['function_input']['dataset_id'])

    def test_delete_snapshot(self) -> None:
        def create_test_snapshot(dataset_id: str) -> Any:
            test_data = self.test_info['tests']['test_delete_snapshot']['create_snapshot']
            # adding dataset id to snapshot name due to TDR constraints on snapshot name uniueness
            data = {
                "name": f"{test_data['function_input']['name']}_{dataset_id}",
                "description": test_data['function_input']['description'],
                "contents": test_data['function_input']['contents'],
                "profileId": test_data['function_input']['profileId']
            }
            response = self.tdr_client.request_util.run_request(
                uri="https://data.terra.bio/api/repository/v1/snapshots",
                method='POST',
                data=json.dumps(data),
                content_type="application/json"
            )
            if response.ok:
                job_results = MonitorTDRJob(tdr=self.tdr_client, job_id=response.json()[
                                            'id'], check_interval=15, return_json=True).run()
                return job_results

        def delete_test_snapshot(snapshot_id: str) -> None:

            self.tdr_client.delete_snapshot(snapshot_id=snapshot_id)

        test_data = self.test_info['tests']['test_delete_snapshot']
        dataset_info = self.tdr_client.check_if_dataset_exists(
            dataset_name=test_data['delete_snapshot']['function_input']['dataset_name'],
            billing_profile=test_data['delete_snapshot']['function_input']['profileId'])
        if dataset_info:
            dataset_id = dataset_info[0]['id']
            dataset_snapshots = self.tdr_client.get_dataset_snapshots(dataset_id=dataset_id)
            if dataset_snapshots['total'] > 0:
                snapshot_id = dataset_snapshots['items'][0]['id']
            else:
                formatted_id = dataset_id.replace('-', '_')
                new_snapshot = create_test_snapshot(dataset_id=formatted_id)
                snapshot_id = new_snapshot['id']

            delete_test_snapshot(snapshot_id)

    def test_delete_dataset(self) -> None:
        test_data = self.test_info['tests']['test_delete_dataset']
        dataset_info = self.tdr_client.check_if_dataset_exists(
            dataset_name=test_data['function_input']['dataset_name'],
            billing_profile=test_data['function_input']['billing_profile'])
        if dataset_info:
            dataset_id = dataset_info[0]['id']
            self.tdr_client.delete_dataset(dataset_id=dataset_id)
        else:
            print("Dataset does not exist")
