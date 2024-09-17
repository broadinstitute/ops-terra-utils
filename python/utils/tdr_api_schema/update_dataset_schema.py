from pydantic import BaseModel
from typing import Optional

from python.utils.tdr_api_schema import Column, Relationship, Table


class NewColumn(BaseModel):
    tableName: str
    columns: list[Column]


class Changes(BaseModel):
    addTables: Optional[list[Table]]
    addColumns: Optional[list[NewColumn]]
    addRelationships: Optional[list[Relationship]]


class UpdateSchema(BaseModel):
    description: str
    changes: Changes
