from pydantic import BaseModel
from typing import Optional

from . import Column, Relationship, Table


class NewColumn(BaseModel):
    tableName: str
    columns: list[Column]


class Changes(BaseModel):
    addTables: Optional[list[Table]] = None
    addColumns: Optional[list[NewColumn]] = None
    addRelationships: Optional[list[Relationship]] = None


class UpdateSchema(BaseModel):
    description: str
    changes: Changes
