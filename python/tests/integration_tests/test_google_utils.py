import pytest
import json
import pathlib

from python.utils.gcp_utils import GCPCloudFunctions
from google.cloud import storage
from google.auth import default


def gcp_test_resource_json():
    resource_json = pathlib.Path(__file__).parent.joinpath("gcp_resources.json")
    json_data = json.loads(resource_json.read_text())
    return json_data


def check_cloud_paths(path_dicts):
    def gcs_client():
        credentials, project = default()
        return storage.Client(credentials=credentials, project=project)

    client = gcs_client()
    for validation in path_dicts:
        bucket = client.bucket(validation["path"]["bucket"])
        blob = bucket.blob(validation["path"]["file_path"])
        check_passed = blob.exists() == validation["should_exist"]
        validation["check_passed"] = check_passed


@pytest.fixture(scope='session', autouse=True)
def setup_test_gcs_resources():

    def gcp_test_resource_json():
        resource_json = pathlib.Path(__file__).parent.joinpath("gcp_resources.json")
        json_data = json.loads(resource_json.read_text())
        return json_data

    def gcs_client():
        credentials, project = default()
        return storage.Client(credentials=credentials, project=project)

    def del_bucket_objs(obj_list):
        for item in obj_list:
            item.delete()

    def create_cloud_files():

        bucket = client.bucket(json_data["bucket"])
        for test, test_info in json_data["tests"].items():
            for item in test_info['resources']:
                blob = bucket.blob(item['path'])
                blob.upload_from_string(item['data'])

    # Setup resources
    client = gcs_client()
    json_data = gcp_test_resource_json()
    test_bucket = client.bucket(json_data["bucket"])
    # cleanup bucket if any left-over objects are present before creating new ones
    blob_list = test_bucket.list_blobs()
    if blob_list.num_results > 0:
        del_bucket_objs(obj_list=blob_list)

    # create test objects
    create_cloud_files()

    yield

    # teardown resources
    blob_list = test_bucket.list_blobs()
    del_bucket_objs(blob_list)


def test_list_bucket_contents():
    resources = gcp_test_resource_json()['tests']
    expected_files = []
    gcp_blob_count = 0
    for key, value in resources.items():
        for item in value['resources']:
            expected_files.append(item['path'])
            gcp_blob_count += 1

    result = GCPCloudFunctions().list_bucket_contents(bucket_name=gcp_test_resource_json()["bucket"])
    found_files = [
        # Remove bucket. gs://bucket_name/path/to/file -> path/to/file
        '/'.join(item['path'].split('/')[3:])
        for item in result
    ]
    if len(found_files) > gcp_blob_count:
        extra_files = set(found_files) - set(expected_files)
        message = f"Found more files than expected. Found extra files: {extra_files}"
    elif gcp_blob_count > len(found_files):
        missing_files = set(expected_files) - set(found_files)
        message = f"Found fewer files than expected. Missing files: {missing_files}"
    else:
        message = ""
    assert len(found_files) == gcp_blob_count, f"Expected {gcp_blob_count} files, got {len(found_files)}. {message}"


def test_get_blob_details():
    test_data = gcp_test_resource_json()['tests']['get_blob_details']['test_data']
    result = GCPCloudFunctions().load_blob_from_full_path(full_path=test_data['function_input']['blob_path'])
    assert result.path == "/b/ops_dev_bucket/o/list_bucket_test%2Fex_file_1.txt"


def test_copy_cloud_file():
    test_data = gcp_test_resource_json()['tests']['copy_file']['test_data']
    validations = test_data['validation']

    GCPCloudFunctions().copy_cloud_file(
        src_cloud_path=test_data['function_input']['source_path'],
        full_destination_path=test_data['function_input']['destination_path'])
    check_cloud_paths(validations)
    for item in validations:
        assert item["check_passed"], "Files were not in expected end state"


def test_delete_cloud_file():
    test_data = gcp_test_resource_json()['tests']['delete_file']['test_data']
    validations = test_data['validation']

    GCPCloudFunctions().delete_cloud_file(full_cloud_path=test_data['function_input']['deletion_path'])
    check_cloud_paths(validations)
    for item in validations:
        assert item["check_passed"], "Files were not in expected end state"


def test_move_cloud_file():
    test_data = gcp_test_resource_json()['tests']['move_file']['test_data']
    validations = test_data['validation']

    GCPCloudFunctions().move_cloud_file(
        src_cloud_path=test_data['function_input']['source_path'],
        full_destination_path=test_data['function_input']['destination_path'])
    check_cloud_paths(validations)
    for item in validations:
        assert item["check_passed"], "Files were not in expected end state"


def test_get_filesize():
    test_data = gcp_test_resource_json()['tests']['get_filesize']['test_data']

    filesize = GCPCloudFunctions().get_filesize(target_path=test_data['function_input']['source_path'])
    assert filesize == 30, "Filesize was not as expected"


def test_validate_files_are_same():
    test_data = gcp_test_resource_json()['tests']['validate_files_are_same']['test_data']

    files_match = GCPCloudFunctions().validate_files_are_same(
        src_cloud_path=test_data['function_input']['file_1'], dest_cloud_path=test_data['function_input']['file_1'])
    files_do_not_match = GCPCloudFunctions().validate_files_are_same(
        src_cloud_path=test_data['function_input']['file_1'], dest_cloud_path=test_data['function_input']['file_2'])
    assert files_match and not files_do_not_match, "File validations did not return expected results"


def test_delete_multiple_files():
    test_data = gcp_test_resource_json()['tests']['delete_multiple_files']['test_data']
    validations = test_data['validation']

    GCPCloudFunctions().delete_multiple_files(files_to_delete=test_data['function_input']['files_to_delete'])
    check_cloud_paths(validations)
    for item in validations:
        assert item["check_passed"], "Files were not in expected end state"


def test_validate_file_pair():
    test_data = gcp_test_resource_json()['tests']['validate_file_pair']['test_data']

    files_match = GCPCloudFunctions().validate_file_pair(
        source_file=test_data['function_input']['file_1'], full_destination_path=test_data['function_input']['file_1'])

    files_do_not_match = GCPCloudFunctions().validate_file_pair(
        source_file=test_data['function_input']['file_1'], full_destination_path=test_data['function_input']['file_2'])

    assert files_match['identical'] and not files_do_not_match['identical'], \
        "File validations did not return expected results"


def test_loop_and_log_validation_files_multithreaded():
    test_data = gcp_test_resource_json()['tests']['loop_and_log_validation_files_multithreaded']['test_data']

    result = GCPCloudFunctions().loop_and_log_validation_files_multithreaded(
        files_to_validate=test_data['function_input']['input_list'], log_difference=True)

    assert len(result) == 1, "Expected one file to be different, got more or less"


def test_multithread_copy_of_files_with_validation():
    test_data = gcp_test_resource_json()['tests']['multithread_copy_of_files_with_validation']['test_data']
    validation = test_data['validation']

    GCPCloudFunctions().multithread_copy_of_files_with_validation(
        files_to_copy=test_data['function_input'], workers=2, max_retries=1)
    check_cloud_paths(validation)
    for item in validation:
        assert item["check_passed"], "Files were not in expected end state"


def test_move_or_copy_multiple_files():
    test_data = gcp_test_resource_json()['tests']['move_or_copy_multiple_files']['test_data']
    validation = test_data['validation']

    def run_copy_test():

        GCPCloudFunctions().move_or_copy_multiple_files(
            files_to_move=test_data['function_input']['copy_test_input'], action="copy", workers=2, max_retries=1)
        check_cloud_paths(validation['copy_test'])
        for item in validation['copy_test']:
            assert item["check_passed"], "Files were not in expected end state"

    def run_mv_test():

        GCPCloudFunctions().move_or_copy_multiple_files(
            files_to_move=test_data['function_input']['move_test_input'], action="move", workers=2, max_retries=1)
        check_cloud_paths(validation['move_test'])
        for item in validation['move_test']:
            assert item["check_passed"], "Files were not in expected end state"

    run_copy_test()
    run_mv_test()
