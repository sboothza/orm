from sb_serializer import Name

from src.sb_orm import TableBase
from src.sb_orm.database_objects import Table, Field, FieldType, Key, KeyType
from src.sb_orm.table_base import M


class TestTable(TableBase):
    __table_definition__ = Table(Name("test_table"),
                                 fields=[
                                     Field(Name("id"), FieldType.Integer, 4, auto_increment=True, required=True),
                                     Field(Name("name"), FieldType.String, 50, required=False)
                                 ], pk=Key(Name("pk_id"), KeyType.PrimaryKey, ["id"], primary_table="test_table",
                                           primary_fields=["id"]))

    id: int
    name: str

    def __init__(self, id: int = 0, name: str = ""):
        self.id = id
        self.name = name

    def __str__(self):
        return f"id:{self.id} name:{self.name}"

    def map_row(self, row) -> M:
        self.id = row[0]
        self.name = row[1]
        return self

    def get_update_params(self) -> dict:
        return {"id": self.id,
                "name": self.name}

    def get_insert_params(self) -> dict:
        return {"name": self.name}
