from typing import TypeVar

from src.sb_orm.adaptor import Adaptor
from src.sb_orm.database_objects import Table


class Mappable:
    pass


M = TypeVar("M", bound=Mappable)


class TableBase(Mappable):
    __table_name__ = "table_name"
    __table_definition__: Table = None
    __table_exists_script__ = "table_exists"
    __table_count_script__ = "table_count"
    __create_script__ = "table_create"
    __insert_script__ = "table_insert"
    __update_script__ = "table_update"
    __delete_script__ = "table_delete"
    __drop_script__ = "table_drop"
    __fetch_by_id_script__ = "table_fetch"
    __item_exists_script__ = "table_exists"

    @classmethod
    def build_queries(cls, adaptor: Adaptor, db_name: str):
        cls.__table_name__ = cls.__table_definition__.name.raw()
        cls.__table_exists_script__ = adaptor.generate_table_exists_script(cls.__table_definition__, db_name)
        cls.__table_count_script__ = adaptor.generate_count_script(cls.__table_definition__)
        cls.__create_script__ = adaptor.generate_create_script(cls.__table_definition__)
        cls.__insert_script__ = adaptor.generate_insert_script(cls.__table_definition__)
        cls.__update_script__ = adaptor.generate_update_script(cls.__table_definition__)
        cls.__delete_script__ = adaptor.generate_delete_script(cls.__table_definition__)
        cls.__drop_script__ = adaptor.generate_drop_script(cls.__table_definition__)
        cls.__fetch_by_id_script__ = adaptor.generate_fetch_by_id_script(cls.__table_definition__)
        cls.__item_exists_script__ = adaptor.generate_item_exists_script(cls.__table_definition__)

    def map_row(self, row) -> M:
        pass

    def get_update_params(self) -> dict:
        return {}

    def get_insert_params(self) -> dict:
        return {}
