import pytest
import json
import pathlib
from typing import Any
import random
import string


from python.utils.gcp_utils import GCPCloudFunctions
from google.cloud import storage
from google.auth import default


def gcp_test_resource_json() -> dict:
    resource_json = pathlib.Path(__file__).parent.joinpath("gcp_resources.json")
    json_data = json.loads(resource_json.read_text())
    return json_data


def transform_components_to_full_cloud_path(bucket: str, file_path: str) -> str:
    return f"gs://{bucket}/{file_path}"


def check_cloud_paths(path_dicts: list[dict]) -> None:
    def gcs_client() -> storage.Client:
        credentials, project = default()
        return storage.Client(credentials=credentials, project=project)

    client = gcs_client()
    for validation in path_dicts:
        bucket = client.bucket(validation["path"]["bucket"])
        blob = bucket.blob(validation["path"]["file_path"])
        check_passed = blob.exists() == validation["should_exist"]
        validation["check_passed"] = check_passed


@pytest.fixture(scope='session', autouse=True)
def setup_test_gcs_resources() -> Any:

    def gcp_test_resource_json() -> dict:
        resource_json = pathlib.Path(__file__).parent.joinpath("gcp_resources.json")
        json_data = json.loads(resource_json.read_text())
        return json_data

    def gcs_client() -> storage.Client:
        credentials, project = default()
        return storage.Client(credentials=credentials, project=project)

    def del_bucket_objs(obj_list: list) -> None:
        for item in obj_list:
            item.delete()

    def create_cloud_files() -> None:
        def gen_rand_str() -> str:
            return ''.join(random.choices(string.ascii_lowercase + string.digits, k=30))

        bucket = client.bucket(json_data["bucket"])
        for test, files in json_data["test_resources"].items():
            rand_string = gen_rand_str()
            if type(files) == list:
                for file in files:
                    blob = bucket.blob(file)
                    blob.upload_from_string(rand_string)
            else:
                blob = bucket.blob(files)
                blob.upload_from_string(rand_string)

    # Setup resources
    client = gcs_client()
    json_data = gcp_test_resource_json()
    test_bucket = client.bucket(json_data["bucket"])
    #cleanup bucket if any left over objects are present before creating new ones
    blob_list = test_bucket.list_blobs()
    if blob_list.num_results > 0:
        del_bucket_objs(obj_list=blob_list)

    # create test objects
    create_cloud_files()

    yield

    #teardown resources
    blob_list = test_bucket.list_blobs()
    del_bucket_objs(blob_list)


def test_list_bucket_contents() -> None:
    result = GCPCloudFunctions().list_bucket_contents(bucket_name=gcp_test_resource_json()["bucket"])
    print(result)


def test_copy_cloud_file() -> None:
    cloud_resources = gcp_test_resource_json()
    src_path = f"gs://{cloud_resources['bucket']}/{cloud_resources['test_resources']['copy_file']}"
    dest_path = f"gs://{cloud_resources['bucket']}/tmp/file_copied.txt"
    expected_files = [
        {"path": {
            "bucket": cloud_resources['bucket'],
            "file_path": cloud_resources['test_resources']['copy_file']
        },
            "should_exist": True},
        {"path": {
            "bucket": cloud_resources['bucket'],
            "file_path": "tmp/file_copied.txt"
        },
            "should_exist": True}
    ]
    GCPCloudFunctions().copy_cloud_file(src_cloud_path=src_path, full_destination_path=dest_path)
    check_cloud_paths(expected_files)
    for item in expected_files:
        assert item["check_passed"] == True, "Files were not in expected end state"


def test_delete_cloud_file() -> None:
    cloud_resources = gcp_test_resource_json()
    file_to_del = f"gs://{cloud_resources['bucket']}/{cloud_resources['test_resources']['delete_file']}"
    expected_files = [
        {"Path": {
            "bucket": cloud_resources['bucket'],
            "file_path": cloud_resources['test_resources']['delete_file']
        },
            "should_exist": False
        }
    ]
    GCPCloudFunctions().delete_cloud_file(full_cloud_path=file_to_del)

    for item in expected_files:
        assert item["check_passed"] == True, "Files were not in expected end state"


def test_move_cloud_file() -> None:
    cloud_resources = gcp_test_resource_json()
    src_blob_path = f"gs://{cloud_resources['bucket']}/{cloud_resources['test_resources']['move_file']}"
    dest_blob_path = f"gs://{cloud_resources['bucket']}/tmp/file_moved.txt"

    expected_files = [
        {"Path": {
            "bucket": cloud_resources['bucket'],
            "file_path": cloud_resources['test_resources']['move_file']
        },
            "should_exist": False
        },
        {"Path": {
            "bucket": cloud_resources['bucket'],
            "file_path": "tmp/file_moved.txt"
        },
            "should_exist": True
        }
    ]

    GCPCloudFunctions().move_cloud_file(src_cloud_path=src_blob_path, full_destination_path=dest_blob_path)
    for item in expected_files:
        assert item["check_passed"] == True, "Files were not in expected end state"


def test_get_filesize() -> None:
    cloud_resources = gcp_test_resource_json()
    target_path = f"gs://{cloud_resources['bucket']}/{cloud_resources['test_resources']['get_filesize']}"

    filesize = GCPCloudFunctions().get_filesize(target_path=target_path)


def test_validate_files_are_same() -> None:
    cloud_resources = gcp_test_resource_json()
    validation_files = [transform_components_to_full_cloud_path(
        bucket=cloud_resources['bucket'], file_path=file) for file in cloud_resources['test_resources']['validate_files_are_same']]
    result = GCPCloudFunctions().validate_files_are_same(
        src_cloud_path=validation_files[0], dest_cloud_path=validation_files[1])


def test_delete_multiple_files() -> None:
    cloud_resources = gcp_test_resource_json()
    deletion_files = [transform_components_to_full_cloud_path(
        bucket=cloud_resources['bucket'], file_path=file) for file in cloud_resources['test_resources']['delete_multiple_files']]

    expected_files = [
        {"Path": {
            "bucket": cloud_resources['bucket'],
            "file_path": cloud_resources['test_resources']['delete_multiple_files'][0]
        },
            "should_exist": False
        },
        {"Path": {
            "bucket": cloud_resources['bucket'],
            "file_path": cloud_resources['test_resources']['delete_multiple_files'][1]
        },
            "should_exist": False
        }
    ]

    GCPCloudFunctions().delete_multiple_files(files_to_delete=deletion_files)

    for item in expected_files:
        assert item["check_passed"] == True, "Files were not in expected end state"


def validate_file_pair() -> None:
    cloud_resources = gcp_test_resource_json()
    validation_file_pair = [transform_components_to_full_cloud_path(
        bucket=cloud_resources['bucket'], file_path=file) for file in cloud_resources['test_resources']['validate_file_pair']]

    result = GCPCloudFunctions().validate_file_pair(
        source_file=validation_file_pair[0], full_destination_path=validation_file_pair[1])


def test_loop_and_log_validation_files_multithreaded() -> None:
    cloud_resources = gcp_test_resource_json()
    test_files = [transform_components_to_full_cloud_path(bucket=cloud_resources['bucket'], file_path=file)
                  for file in cloud_resources['test_resources']['loop_and_log_validation_files_multithreaded']]
    input_list = [
        {"source_file": test_files[0], "full_destination_path": test_files[1]},
        {"source_file": test_files[2], "full_destination_path": test_files[3]}]

    result = GCPCloudFunctions().loop_and_log_validation_files_multithreaded(files_to_validate=input_list, log_difference=True)


def test_multithread_copy_of_files_with_validation() -> None:
    cloud_resources = gcp_test_resource_json()
    test_files = [transform_components_to_full_cloud_path(bucket=cloud_resources['bucket'], file_path=file)
                  for file in cloud_resources['test_resources']['multithread_copy_of_files_with_validation']]
    input_dict_list = [
        {"source_file": test_files[0], "full_destination_path": cloud_resources['bucket'] +
            "/tmp/multi_copy/test_1.txt"},
        {"source_file": test_files[1], "full_destination_path": cloud_resources['bucket'] +
            "/tmp/multi_copy/test_2.txt"}
    ]

    expected_files = [
        {"Path": {
            "bucket": cloud_resources['bucket'],
            "file_path": cloud_resources['test_resources']['multithread_copy_of_files_with_validation'][0]
        },
            "should_exist": True
        },
        {"Path": {
            "bucket": cloud_resources['bucket'],
            "file_path": cloud_resources['test_resources']['multithread_copy_of_files_with_validation'][1]
        },
            "should_exist": True
        },
        {"Path": {
            "bucket": cloud_resources['bucket'],
            "file_path": "/tmp/multi_copy/test_1.txt"
        },
            "should_exist": True
        },
        {"Path": {
            "bucket": cloud_resources['bucket'],
            "file_path": "/tmp/multi_copy/test_2.txt"
        },
            "should_exist": True
        }
    ]

    GCPCloudFunctions().multithread_copy_of_files_with_validation(
        files_to_move=input_dict_list, workers=2, max_retries=1)

    for item in expected_files:
        assert item["check_passed"] == True, "Files were not in expected end state"


def test_move_or_copy_multiple_files() -> None:
    cloud_resources = gcp_test_resource_json()
    test_files = [transform_components_to_full_cloud_path(
        bucket=cloud_resources['bucket'], file_path=file) for file in cloud_resources['test_resources']['move_or_copy_multiple_files']]
    ouput_copy_path = cloud_resources['bucket'] + "/tmp/copy/"
    output_mv_path = cloud_resources['bucket'] + "/tmp/mv/"

    def run_copy_test() -> None:
        input_dict_list = [
            {"source_file": test_files[0], "full_destination_path": cloud_resources['bucket'] + "/tmp/copy/file_1.txt"},
            {"source_file": test_files[1], "full_destination_path": cloud_resources['bucket'] + "/tmp/copy/file_2.txt"}
        ]
        expected_files = [
            {"Path": {
                "bucket": cloud_resources['bucket'],
                "file_path": cloud_resources['test_resources']['move_or_copy_multiple_files'][0]
            },
                "should_exist": True
            },
            {"Path": {
                "bucket": cloud_resources['bucket'],
                "file_path": cloud_resources['test_resources']['move_or_copy_multiple_files'][1]
            },
                "should_exist": True
            },
            {"Path": {
                "bucket": cloud_resources['bucket'],
                "file_path": cloud_resources['bucket'] + "/tmp/copy/file_1.txt"
            },
                "should_exist": True
            },
            {"Path": {
                "bucket": cloud_resources['bucket'],
                "file_path": cloud_resources['bucket'] + "/tmp/copy/file_2.txt"
            },
                "should_exist": True
            }
        ]

        GCPCloudFunctions().move_or_copy_multiple_files(files_to_move=input_dict_list, action="copy", workers=2, max_retries=1)
        for item in expected_files:
            assert item["check_passed"] == True, "Files were not in expected end state"

    def run_mv_test() -> None:
        input_dict_list = [
            {"source_file": test_files[0], "full_destination_path": cloud_resources['bucket'] + "/tmp/mv/file_1.txt"},
            {"source_file": test_files[1], "full_destination_path": cloud_resources['bucket'] + "/tmp/mv/file_2.txt"}
        ]

        expected_files = [
            {"Path": {
                "bucket": cloud_resources['bucket'],
                "file_path": test_files[0]
            },
                "should_exist": False
            },
            {"Path": {
                "bucket": cloud_resources['bucket'],
                "file_path": test_files[1]
            },
                "should_exist": False
            },
            {"Path": {
                "bucket": cloud_resources['bucket'],
                "file_path": cloud_resources['bucket'] + "/tmp/mv/file_1.txt"
            },
                "should_exist": True
            },
            {"Path": {
                "bucket": cloud_resources['bucket'],
                "file_path": cloud_resources['bucket'] + "/tmp/mv/file_1.txt"
            },
                "should_exist": True
            }
        ]

        GCPCloudFunctions().move_or_copy_multiple_files(files_to_move=input_dict_list, action="move", workers=2, max_retries=1)
        for item in expected_files:
            assert item["check_passed"] == True, "Files were not in expected end state"

    run_copy_test()
    run_mv_test()
