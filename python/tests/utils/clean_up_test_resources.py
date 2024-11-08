from python.utils.tdr_utils.tdr_api_utils import TDR
from python.utils.token_util import Token
from python.utils.request_util import RunRequest


def tdr_resource_cleanup() -> None:
    token = Token(cloud='gcp')
    requestclient = RunRequest(token=token)
    tdr_client = TDR(request_util=requestclient)
    dataset_name = "tmp_ops_integration_test_dataset_to_delete"
    dataset_info = tdr_client.check_if_dataset_exists(dataset_name=dataset_name,
                                                      billing_profile="ce149ca7-608b-4d5d-9612-2a43a7378885")
    if dataset_info:
        print(f"dataset info found: {dataset_info}")
        dataset_id = dataset_info[0]['id']
        tdr_client.add_user_to_dataset(dataset_id=dataset_id, user='jscira@broadinstitute.org', policy='steward')
        if dataset_info[0]['resourceLocks']['exclusive']:
            lock_id = dataset_info[0]['resourceLocks']['exclusive']
            tdr_client.unlock_dataset(dataset_id=dataset_id, lock_id=lock_id)

        tdr_client.delete_dataset(dataset_id=dataset_id)


if __name__ == "__main__":
    tdr_resource_cleanup()
