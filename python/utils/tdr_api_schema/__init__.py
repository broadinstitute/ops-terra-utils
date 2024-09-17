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
    name: Field(str, min_length=1, max_length=63)
    datatype: DataTypeEnum
    array_of: Optional[bool]
    required: Optional[bool]


class RelationshipTerm(BaseModel):
    table: Field(str, min_length=1, max_length=63)
    column: Field(str, min_length=1, max_length=63)


class Relationship(BaseModel):
    name: Field(str, min_length=1)
    from_table: RelationshipTerm = Field(alias="from")
    to: RelationshipTerm


class PartitionModeEnum(str, Enum):
    none = "none"
    date = "date"
    int = "int"


class DatePartition(BaseModel):
    column: Field(str, min_length=1, max_length=63)


class IntPartition(BaseModel):
    column: Field(str, min_length=1, max_length=63)
    min: int
    max: int
    interval: int


class Table(BaseModel):
    name: Field(str, max_length=63, min_length=1)
    columns: list[Column]
    primaryKey: Optional[list[Field(str, min_length=1, max_length=63)]]
    partitionMode: Optional[PartitionModeEnum]
    datePartitionOptions: Optional[DatePartition]
    intPartitionOptions: Optional[IntPartition]
    row_count: Optional[int]
