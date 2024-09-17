from enum import Enum
from pydantic import BaseModel, Field
from typing import Optional


class DataTypeEnum(str, Enum):
    string = "string"
    boolean = "boolean"
    bytes = "bytes"
    date = "date"
    datetime = "datetime"
    dirref = "dirref"
    fileref = "fileref"
    float = "float"
    float64 = "float64"
    integer = "integer"
    int64 = "int64"
    numeric = "numeric"
    record = "record"
    text = "text"
    time = "time"
    timestamp = "timestamp"


class Column(BaseModel):
    name: str = Field(min_length=1, max_length=63)
    datatype: DataTypeEnum
    array_of: Optional[bool] = None
    required: Optional[bool] = None


class RelationshipTerm(BaseModel):
    table: str = Field(min_length=1, max_length=63)
    column: str = Field(min_length=1, max_length=63)


class Relationship(BaseModel):
    name: str = Field(min_length=1)
    from_table: RelationshipTerm = Field(alias="from")
    to: RelationshipTerm


class PartitionModeEnum(str, Enum):
    none = "none"
    date = "date"
    int = "int"


class DatePartition(BaseModel):
    column: str = Field(min_length=1, max_length=63)


class IntPartition(BaseModel):
    column: str = Field(min_length=1, max_length=63)
    min: int
    max: int
    interval: int


class Table(BaseModel):
    name: str = Field(max_length=63, min_length=1)
    columns: list[Column]
    primaryKey: Optional[list[str]] = None
    partitionMode: Optional[PartitionModeEnum] = None
    datePartitionOptions: Optional[DatePartition] = None
    intPartitionOptions: Optional[IntPartition] = None
    row_count: Optional[int] = None
