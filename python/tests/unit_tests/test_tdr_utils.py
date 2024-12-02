import pytest
import json
import pathlib
import responses
from responses import matchers

from python.utils.tdr_utils.tdr_api_utils import TDR
from python.utils.tdr_utils.tdr_schema_utils import InferTDRSchema
from python.utils.tdr_utils.tdr_ingest_utils import BatchIngest
from python.utils.token_util import Token
from python.utils.request_util import RunRequest


def mock_api_response(test_json):
    match test_json['method']:
        case 'GET':
            responses.get(
                url=test_json['url'],
                body=json.dumps(test_json['response']),
                status=test_json['status'],
                content_type='application/json',
                match=[matchers.query_param_matcher(test_json['params'], strict_match=False)]
            )

        case 'POST':
            responses.post(
                url=test_json['url'],
                body=json.dumps(test_json['response']),
                status=test_json['status'],
                content_type='application/json'
            )

        case 'DELETE':
            responses.delete(
                url=test_json['url'],
                body=json.dumps(test_json['response']),
                status=test_json['status'],
                content_type='application/json'
            )


@pytest.fixture()
def tdr_test_resource_json():
    resource_json = pathlib.Path(__file__).parent.joinpath("tdr_resources.json")
    json_data = json.loads(resource_json.read_text())
    return json_data


@pytest.fixture()
def tdr_client():
    token = Token(cloud='gcp')
    requestclient = RunRequest(token, max_retries=1, max_backoff_time=1)
    return TDR(request_util=requestclient)


class TestGetUtils:

    @pytest.fixture(autouse=True)
    def _get_tdr_client(self, tdr_client, tdr_test_resource_json):
        self.tdr_client = tdr_client
        self.test_info = tdr_test_resource_json

    @responses.activate
    def test_get_data_set_files(self):
        test_data = self.test_info['tests']['get_files_endpoint']
        mock_api_response(test_json=test_data['mock_response']['page_one'])
        mock_api_response(test_json=test_data['mock_response']['page_two'])
        file_list = self.tdr_client.get_data_set_files(dataset_id=test_data['function_input'])
        assert len(file_list) == 3

    @responses.activate
    def test_create_file_dict(self):
        test_data = self.test_info['tests']['get_files_endpoint']
        mock_api_response(test_json=test_data['mock_response']['page_one'])
        mock_api_response(test_json=test_data['mock_response']['page_two'])
        file_dict = self.tdr_client.create_file_dict(dataset_id=test_data['function_input'])
        assert len(file_dict.keys()) == 3

    @responses.activate
    def test_get_snapshot_info(self):
        test_data = self.test_info['tests']['get_snapshot_endpoint']
        mock_api_response(test_json=test_data['mock_response'])
        cmd = self.tdr_client.get_snapshot_info(snapshot_id=test_data['function_input'])
        assert cmd['name'] == 'snapshot name'

    @responses.activate
    def test_check_if_dataset_exists(self):
        test_data = self.test_info['tests']['list_datasets_endpoint']
        mock_api_response(test_json=test_data['mock_response']['page_one'])
        mock_api_response(test_json=test_data['mock_response']['page_two'])
        dataset_exists = self.tdr_client.check_if_dataset_exists(
            dataset_name=test_data['function_input']['dataset_name'],
            billing_profile=test_data['function_input']['billing_profile'])
        assert dataset_exists

    @responses.activate
    def test_get_dataset_info(self):
        test_data = self.test_info['tests']['get_dataset_endpoint']
        mock_api_response(test_json=test_data['mock_response'])
        found_dataset = self.tdr_client.get_dataset_info(dataset_id=test_data['function_input']['dataset_id'])
        assert found_dataset

    @responses.activate
    def test_get_table_schema_info(self):
        test_data = self.test_info['tests']['get_dataset_endpoint']
        mock_api_response(test_json=test_data['mock_response'])
        schema_info = self.tdr_client.get_table_schema_info(
            dataset_id=test_data['function_input']['dataset_id'], table_name=test_data['function_input']['table_name'])
        assert schema_info

    @responses.activate
    def test_get_data_set_table_metrics(self):
        test_data = self.test_info['tests']['get_dataset_table']
        mock_api_response(test_json=test_data['mock_response']['page_one'])
        mock_api_response(test_json=test_data['mock_response']['page_two'])
        table_metrics = self.tdr_client.get_dataset_table_metrics(
            dataset_id=test_data['function_input']['dataset_id'],
            target_table_name=test_data['function_input']['table_name'])
        assert table_metrics

    @responses.activate
    def test_get_data_set_sample_ids(self):
        test_data = self.test_info['tests']['get_dataset_table']
        mock_api_response(test_json=test_data['mock_response']['page_one'])
        mock_api_response(test_json=test_data['mock_response']['page_two'])
        sample_ids = self.tdr_client.get_data_set_sample_ids(
            dataset_id=test_data['function_input']['dataset_id'],
            target_table_name=test_data['function_input']['table_name'],
            entity_id=test_data['function_input']['entity_id'])

        assert sample_ids

    @responses.activate
    def test_get_files_from_snapshot(self):
        test_data = self.test_info['tests']['get_snapshot_files']
        mock_api_response(test_json=test_data['mock_response']['page_one'])
        mock_api_response(test_json=test_data['mock_response']['page_two'])
        snapshot_files = self.tdr_client.get_files_from_snapshot(snapshot_id=test_data['function_input'])
        assert snapshot_files

    @responses.activate
    def test_InferTDRSchema(self):
        # input_metadata: list[dict], table_name
        test_data = self.test_info['tests']['InferTDRSchema']
        cmd = InferTDRSchema(input_metadata=test_data['function_input']['input_metadata'],
                             table_name=test_data['function_input']['table_name']).infer_schema()
        assert cmd['name'] == 'samples' and len(cmd['columns']) == 13


class TestCreateUtils:

    @pytest.fixture(autouse=True)
    def _get_tdr_client(self, tdr_client, tdr_test_resource_json):
        self.tdr_client = tdr_client
        self.test_info = tdr_test_resource_json

    @responses.activate
    def test_get_or_create_dataset(self):
        list_dataset_endpoint = self.test_info['tests']['list_datasets_endpoint']
        create_dataset_endpoint = self.test_info['tests']['create_dataset_endpoint']
        get_job_status = self.test_info['tests']['get_job_status']
        get_job_results = self.test_info['tests']['create_dataset_job_results']

        # list datasets for existing dataset - page 1
        mock_api_response(test_json=list_dataset_endpoint['mock_response']['page_one'])
        # ^ - page 2
        mock_api_response(test_json=list_dataset_endpoint['mock_response']['page_two'])
        # ^ - list datasets - filter for dataset that does not exist
        mock_api_response(test_json=list_dataset_endpoint['mock_response']['non_existing_dataset'])
        # Create dataset request
        mock_api_response(test_json=create_dataset_endpoint['mock_response']['create_dataset'])
        # list datasets endpoint for new dataset
        mock_api_response(test_json=create_dataset_endpoint['mock_response']['get_datasets'])
        # Get job status
        mock_api_response(test_json=get_job_status['mock_response'])
        # Get job results
        mock_api_response(test_json=get_job_results['mock_response'])

        def get_existing_dataset():
            cmd = self.tdr_client.get_or_create_dataset(
                dataset_name=create_dataset_endpoint['function_input']['dataset_name'],
                billing_profile=create_dataset_endpoint['function_input']['billing_profile'],
                schema=create_dataset_endpoint['function_input']['schema'],
                description=create_dataset_endpoint['function_input']['description'],
                cloud_platform=create_dataset_endpoint['function_input']['cloud_platform'],
                continue_if_exists=True
            )
            assert cmd == 'ex_dataset_name_guid'

        def create_new_dataset():
            new_dataset = self.tdr_client.get_or_create_dataset(
                dataset_name=create_dataset_endpoint['function_input']['new_dataset_name'],
                billing_profile=create_dataset_endpoint['function_input']['billing_profile'],
                schema=create_dataset_endpoint['function_input']['schema'],
                description=create_dataset_endpoint['function_input']['description'],
                cloud_platform=create_dataset_endpoint['function_input']['cloud_platform']
            )
            assert new_dataset == 'new_dataset_guid'

        get_existing_dataset()
        create_new_dataset()

    @responses.activate
    def test_add_user_to_dataset(self):
        test_data = self.test_info['tests']['test_add_user_to_dataset']
        mock_api_response(test_json=test_data['mock_response'])
        self.tdr_client.add_user_to_dataset(
            dataset_id=test_data['function_input']['dataset_id'],
            user=test_data['function_input']['user'],
            policy=test_data['function_input']['policy'])

    @responses.activate
    def test_update_dataset_schema(self):
        test_data = self.test_info['tests']['test_update_dataset_schema']
        mock_api_response(test_json=test_data['mock_response']['update_schema'])
        mock_api_response(self.test_info['tests']['get_job_status']['mock_response'])
        mock_api_response(test_json=test_data['mock_response']['job_results'])

        self.tdr_client.update_dataset_schema(  # type: ignore[return]
            dataset_id=test_data['function_input']['dataset_guid'],
            update_note=test_data['function_input']['update_note'],
            tables_to_add=test_data['function_input']['tables_to_add']
        )

    @responses.activate
    def test_batch_ingest_to_dataset(self):
        test_data = self.test_info['tests']['test_batch_ingest']['metadata_ingest']
        mock_api_response(test_json=test_data['mock_response']['dataset_ingest'])
        mock_api_response(self.test_info['tests']['get_job_status']['mock_response'])
        mock_api_response(test_json=test_data['mock_response']['job_results'])
        BatchIngest(
            ingest_metadata=test_data['function_input']['ingest_metadata'],
            tdr=self.tdr_client,
            target_table_name=test_data['function_input']['target_table_name'],
            dataset_id=test_data['function_input']['dataset_id'],
            batch_size=test_data['function_input']['batch_size'],
            bulk_mode=test_data['function_input']['bulk_mode'],
            cloud_type=test_data['function_input']['cloud_type']
        ).run()

    @responses.activate
    def test_ingest_files(self):
        test_data = self.test_info['tests']['test_batch_ingest']['file_ingest']
        mock_api_response(test_json=test_data['mock_response']['file_ingest'])
        mock_api_response(self.test_info['tests']['get_job_status']['mock_response'])
        mock_api_response(test_json=test_data['mock_response']['job_results'])
        self.tdr_client.file_ingest_to_dataset(dataset_id=test_data['function_input']['dataset_id'],
                                               profile_id=test_data['function_input']['profileId'],
                                               file_list=test_data['function_input']['ingest_files'],
                                               load_tag=test_data['function_input']['load_tag'])


class TestDeleteUtils:

    @pytest.fixture(autouse=True)
    def _get_tdr_client(self, tdr_client, tdr_test_resource_json):
        self.tdr_client = tdr_client
        self.test_info = tdr_test_resource_json

    @responses.activate
    def test_delete_files(self):
        test_data = self.test_info['tests']['delete_files_endpoint']
        mock_api_response(test_json=test_data['mock_response']['delete_file'])
        mock_api_response(self.test_info['tests']['get_job_status']['mock_response'])
        mock_api_response(test_json=test_data['mock_response']['job_results'])
        self.tdr_client.delete_files(file_ids=test_data['function_input']['file_ids'],
                                     dataset_id=test_data['function_input']['dataset_id'])

    @responses.activate
    def test_delete_snapshot(self):
        test_data = self.test_info['tests']['test_delete_snapshot']
        mock_api_response(test_data['mock_response']['delete_snapshot'])
        mock_api_response(self.test_info['tests']['get_job_status']['mock_response'])
        mock_api_response(test_data['mock_response']['job_results'])
        self.tdr_client.delete_snapshot(snapshot_id=test_data['function_input']['snapshot_guid'])

    @responses.activate
    def test_delete_dataset(self):
        test_data = self.test_info['tests']['test_delete_dataset']
        mock_api_response(test_data['mock_response']['delete_dataset'])
        mock_api_response(self.test_info['tests']['get_job_status']['mock_response'])
        mock_api_response(test_data['mock_response']['job_results'])

        self.tdr_client.delete_dataset(dataset_id=test_data['function_input']['dataset_guid'])
