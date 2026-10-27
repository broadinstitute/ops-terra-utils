"""
Set up steps:

1. gcloud config set project broad-duos-dev
2. Get Tessera credentials: gcloud container clusters get-credentials tessera --region us-central1
3. Port forward: kubectl port-forward svc/tessera-api 8080:8080 -n tessera
4. When creating datasets, we have two options:
    a. Can either use the Tessera managed bucket: broad-duos-dev-tessera-datasets
    b. Can try to use a non Tessera managed GCP bucket, by giving read and write access to tessera-sa@broad-duos-dev.iam.gserviceaccount.com
"""

import argparse
import json
import logging
from typing import Optional

from ops_utils.gcp_utils import GCPCloudFunctions
from ops_utils.ingest_util import DataIngest

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)

def list_datasets(ingest_util: DataIngest) -> dict:
    return ingest_util.list_datasets().json()

def create_dataset(
        ingest_util: DataIngest,
        name: str,
        description: Optional[str] = None,
        tags: Optional[list[str]] = None,
        ddl: Optional[str] = None,
        storage: Optional[dict] = None
) -> dict:
    return ingest_util.create_dataset(
        name=name,
        description=description,
        tags=tags,
        ddl=ddl,
        storage=storage
    ).json()

def get_dataset(ingest_util: DataIngest, dataset_id: str) -> dict:
    return ingest_util.get_dataset(dataset_id=dataset_id).json()

def register_files(
        ingest_util: DataIngest,
        dataset_id: str,
        files: list[dict]
) -> dict:
    """files (list[dict]): Files to register. Each entry looks like
            `{"uri": "s3://my-bucket/data/samples/HG00096.vcf.gz", "path": "data/samples/HG00096.vcf.gz"}`.
            `path` is optional and defaults to the URI's object key when omitted."""
    return ingest_util.register_files(dataset_id=dataset_id, files=files).json()

def list_files(ingest_util: DataIngest, dataset_id: str) -> dict:
    return ingest_util.list_files(dataset_id=dataset_id).json()

if __name__ == '__main__':
    data_ingest_util = DataIngest()
    gcp_util = GCPCloudFunctions()
