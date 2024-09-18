from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field

from . import Relationship, Table


class AssetTable(BaseModel):
    name: str = Field(min_length=1, max_length=63)
    columns: list[str]


class Asset(BaseModel):
    name: str = Field(min_length=1)
    tables: list[AssetTable]
    rootTable: str = Field(min_length=1, max_length=63)
    rootColumn: str = Field(min_length=1, max_length=63)
    follow: list[str]


class Schema(BaseModel):
    tables: list[Table]
    relationships: Optional[list[Relationship]] = None
    assets: Optional[list[Asset]] = None


class Policy(BaseModel):
    stewards: str
    custodians: str
    snapshotCreators: str


class CloudPlatformEnum(str, Enum):
    gcp = "gcp"
    azure = "azure"


class CreateDatasetSchema(BaseModel):
    name: str = Field(max_length=511, min_length=1)
    description: Optional[str] = None
    defaultProfileId: str
    tdr_schema: Schema = Field(alias="schema")
    region: Optional[str] = None
    cloudPlatform: Optional[CloudPlatformEnum] = CloudPlatformEnum.gcp
    enableSecureMonitoring: Optional[bool] = None
    phsId: Optional[str] = None
    experimentalSelfHosted: Optional[bool] = None
    properties: Optional[dict] = None
    dedicatedIngestServiceAccount: Optional[bool] = None
    experimentalPredictableFileIds: Optional[bool] = None
    policies: Optional[Policy] = None
    tags: Optional[list[str]] = None
