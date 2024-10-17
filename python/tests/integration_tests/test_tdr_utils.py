import pytest
import json
import pathlib
from typing import Any


from python.utils.tdr_utils.tdr_api_utils import TDR
from python.utils.tdr_utils.tdr_table_utils import SetUpTDRTables
from python.utils.tdr_utils.tdr_schema_utils import InferTDRSchema


@pytest.fixture(scope='session', autouse=True)
def setup_tdr_resources() -> Any:
    pass



def test_get_data_set_files():
    cmd = TDR().get_data_set_files(dataset_id=' str')

def test_create_file_dict():
    cmd = TDR().create_file_dict(dataset_id='str')

def test_get_sas_token():
    cmd = TDR().get_sas_token(snapshot_id=' str', dataset_id='str')

def test_delete_file():
    cmd = TDR().delete_file(file_id='str', dataset_id='str')

def test_delete_files():
    cmd = TDR().delete_files(file_ids=list[str], dataset_id=str)

def test_add_user_to_dataset():
    cmd = TDR().add_user_to_dataset(dataset_id=str, user=str, policy=str)

def test_delete_dataset():
    cmd = TDR().delete_dataset(dataset_id=str)

def test_get_snapshot_info():
    cmd = TDR().get_snapshot_info(snapshot_id=str)

def test_delete_snapshots():
    cmd = TDR().delete_snapshots(snapshot_ids=list[str])

def test_delete_snapshot():
    cmd = TDR().delete_snapshot(snapshot_id=str)

def test_check_if_dataset_exists():
    cmd = TDR().check_if_dataset_exists(dataset_name=str, billing_profile=str)

def test_get_dataset_info():
    cmd = TDR().get_dataset_info(dataset_id=str)

def test_get_table_schema_info():
    cmd = TDR().get_table_schema_info(dataset_id=str, table_name=str)

def test_ingest_to_dataset():
    cmd = TDR().ingest_to_dataset(dataset_id=str, data=dict)

def test_get_data_set_table_metrics():
    cmd = TDR().get_data_set_table_metrics(dataset_id=str, target_table_name=str)

def test_get_data_set_sample_ids():
    cmd = TDR().get_data_set_sample_ids(dataset_id=str, target_table_name=str, entity_id=str)

def test_get_data_set_file_uuids_from_metadata():
    cmd = TDR().get_data_set_file_uuids_from_metadata(dataset_id=str)

def test_get_or_create_dataset():
    cmd = TDR().get_or_create_dataset(
        dataset_name=str,
        billing_profile=str,
        schema=dict,
        description=str,
        cloud_platform=str
    )

def test_update_dataset_schema():
    cmd = TDR().update_dataset_schema(  # type: ignore[return]
        dataset_id=str,
        update_note=str,
        tables_to_add="Optional[list[dict]]",
        relationships_to_add='Optional[list[dict]]',
        columns_to_add='Optional[list[dict]]'
    )

def test_get_files_from_snapshot():
    cmd = TDR().get_files_from_snapshot(snapshot_id=str)




def test_ingest():
    pass


def test_SetUpTDRTables():
    cmd = SetUpTDRTables().run()
    pass

def test_InferTDRSchema():
    cmd = InferTDRSchema().infer_schema()
    pass