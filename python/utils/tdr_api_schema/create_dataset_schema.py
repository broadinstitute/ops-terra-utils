from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field

from python.utils.tdr_api_schema import Relationship, Table


class AssetTable(BaseModel):
    name: Field(str, min_length=1, max_length=63)
    columns: list[Field(str, min_length=1, max_length=63)]


class Asset(BaseModel):
    name: Field(str, min_length=1)
    tables: [AssetTable]
    rootTable: Field(str, min_length=1, max_length=63)
    rootColumn: Field(str, min_length=1, max_length=63)
    follow: list[Field(str, min_length=1, max_length=63)]


class Schema(BaseModel):
    tables: list[Table]
    relationships: Optional[list[Relationship]]
    assets: Optional[list[Asset]]


class Policy(BaseModel):
    stewards: str
    custodians: str
    snapshotCreators: str


class GcpEnum(str, Enum):
    gcp = "gcp"
    azure = "azure"


class CreateDatasetSchema(BaseModel):
    name: Field(str, max_length=511, min_length=1)
    description: Optional[str]
    defaultProfileId: str
    schema: Schema
    region: Optional[str]
    cloudPlatform: Optional[GcpEnum] = GcpEnum.gcp
    enableSecureMonitoring: Optional[bool]
    phsId: Optional[str]
    experimentalSelfHosted: Optional[bool]
    properties: Optional[dict]
    dedicatedIngestServiceAccount: Optional[bool]
    experimentalPredictableFileIds: Optional[bool]
    policies: Optional[Policy]
    tags: Optional[str]
