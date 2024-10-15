import pytest
import requests
import os

from python.utils.request_util import RunRequest
from python.utils.terra_utils.terra_util import TerraWorkspace
from python.utils.terra_utils.terra_workflow_configs import WorkflowConfigs
from python.utils.token_util import Token

INTEGRATION_TEST_TERRA_BILLING_PROJECT = "ops-integration-billing"
INTEGRATION_TEST_TERRA_WORKSPACE_NAME = "ops-integration-test-workspace"

auth_token = Token(cloud="gcp")
request_util = RunRequest(token=auth_token)
terra_workspace = TerraWorkspace(
    billing_project=INTEGRATION_TEST_TERRA_BILLING_PROJECT,
    workspace_name=INTEGRATION_TEST_TERRA_WORKSPACE_NAME,
    request_util=request_util
)


@pytest.fixture(scope="session", autouse=True)
def setup_test_terra_resources():
    # Attempt to delete the workspace before starting any tests
    try:
        terra_workspace.delete_workspace()
    except requests.exceptions.HTTPError:
        pass

    # Create the test workspace
    terra_workspace.create_workspace(continue_if_exists=False)

    # Run tests
    yield


def test_get_workspace_acl():
    res = terra_workspace.get_workspace_acl()
    for _, perms in res["acl"].items():
        assert perms["accessLevel"] == "OWNER"
        assert perms["canCompute"] is True
        assert perms["canShare"] is True


def test_get_workspace_info():
    bucket = terra_workspace.get_workspace_bucket()
    res = terra_workspace.get_workspace_info()
    assert res["workspace"]["attributes"] == {}
    assert res["workspace"]["authorizationDomain"] == []
    assert res["workspace"]["billingAccount"] == "billingAccounts/01E530-84B082-ED5441"
    assert res["workspace"]["bucketName"] == bucket
    assert res["workspace"]["cloudPlatform"] == "Gcp"
    assert res["workspace"]["name"] == INTEGRATION_TEST_TERRA_WORKSPACE_NAME
    assert res["workspace"]["namespace"] == INTEGRATION_TEST_TERRA_BILLING_PROJECT
    assert res["canShare"] is True
    assert res["canCompute"] is True


def test_update_user_acl():
    access_level = "READER"
    email = "test@broadinstitute.org"
    res = terra_workspace.update_user_acl(
        email=email, access_level=access_level, invite_users_not_found=True
    )
    assert res["invitesSent"] == []
    assert res["usersNotFound"] == []
    assert res["usersUpdated"][0]["accessLevel"] == access_level
    assert res["usersUpdated"][0]["canCompute"] is False
    assert res["usersUpdated"][0]["canShare"] is False
    assert res["usersUpdated"][0]["email"] == email


def test_put_metadata_for_library_dataset():
    bucket = terra_workspace.get_workspace_bucket()
    library_metadata = {"library:dulvn": 1}
    res = terra_workspace.put_metadata_for_library_dataset(library_metadata=library_metadata)
    assert res["namespace"] == INTEGRATION_TEST_TERRA_BILLING_PROJECT
    assert res["name"] == INTEGRATION_TEST_TERRA_WORKSPACE_NAME
    assert res["bucketName"] == bucket
    assert res["attributes"] == library_metadata
    assert res["name"] == INTEGRATION_TEST_TERRA_WORKSPACE_NAME
    assert res["namespace"] == INTEGRATION_TEST_TERRA_BILLING_PROJECT


def test_update_multiple_users_acl():
    acl_list = [
        {
            "email": "test2@broadinstitute.org",
            "accessLevel": "READER",
            "canShare": False,
            "canCompute": False,

        },
        {
            "email": "test3@broadinstitute.org",
            "accessLevel": "WRITER",
            "canShare": True,
            "canCompute": True,
        }
    ]
    res = terra_workspace.update_multiple_users_acl(acl_list=acl_list, invite_users_not_found=True)
    invites = res["usersUpdated"]
    for invite in invites:
        if invite["email"] == "test2@broadinstitute.org":
            assert invite["accessLevel"] == "READER"
            assert invite["canCompute"] is False
            assert invite["canShare"] is False
        if invite["email"] == "test3@broadinstitute.org":
            assert invite["accessLevel"] == "WRITER"
            assert invite["canCompute"] is True
            assert invite["canShare"] is True


def test_create_workspace_attributes_ingest_dict():
    res = terra_workspace.create_workspace_attributes_ingest_dict()
    assert res == [{"attribute": "library:dulvn", "value": "1"}]


def test_upload_metadata_to_workspace_table():
    current_dir = os.path.dirname(__file__)
    file_path = os.path.join(current_dir, "sample.tsv")
    res = terra_workspace.upload_metadata_to_workspace_table(entities_tsv=file_path)
    assert res == "sample"


def test_get_workspace_workflows():
    res = terra_workspace.get_workspace_workflows()
    assert res == []


def test_import_workflow():
    workflow = "ExportDataFromSnapshotToBucket"
    workflow_config = getattr(WorkflowConfigs(), workflow)(billing_project=INTEGRATION_TEST_TERRA_BILLING_PROJECT)
    status_code = terra_workspace.import_workflow(workflow_dict=workflow_config)
    assert status_code == 201


def test_get_gcp_workspace_metrics():
    res = terra_workspace.get_gcp_workspace_metrics(entity_type="sample")
    expected_res = [{"attributes": {"sample_alias": "ABC"}, "entityType": "sample", "name": "RP-123_ABC"}]
    assert res == expected_res


def test_get_workspace_entity_info():
    res = terra_workspace.get_workspace_entity_info()
    expected_res = {"sample": {"attributeNames": ["sample_alias"], "count": 1, "idName": "sample_id"}}
    assert res == expected_res
