from pydantic import BaseModel, Field
from typing import Optional


class IntPartitionOptions(BaseModel):
    column: str
    min: int
    max: int
    interval: int


class DatePartitionOptions(BaseModel):
    column: str


class Column(BaseModel):
    name: str
    datatype: str
    array_of: bool
    required: bool


class NewTable(BaseModel):
    name: str
    columns: list[Column]
    primary_key: list[str]
    partitionMode: Optional[str]
    datePartitionOptions: Optional[DatePartitionOptions]
    intPartitionOptions: Optional[IntPartitionOptions]
    rowCount: Optional[int]


class NewColumn(BaseModel):
    tableName: str
    columns: list[Column]


class Table(BaseModel):
    table: str
    column: str


class NewRelationship(BaseModel):
    name: str
    from_table: Table = Field(alias="from")
    to: Table


class Changes(BaseModel):
    addTables: Optional[list[NewTable]]
    addColumns: Optional[list[NewColumn]]
    addRelationships: Optional[list[NewRelationship]]


class UpdateSchema(BaseModel):
    description: str
    changes: Changes
