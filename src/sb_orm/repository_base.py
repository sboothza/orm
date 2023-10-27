from typing import Any, TypeVar

from sb_db_common import Session, DataException

from src.sb_orm import TableBase

# T = TypeVar("T", bound=TableBase)


class RepositoryBase:
    __table__ : type

    @classmethod
    def drop_schema(cls, session: Session) -> None:
        cls._execute(session, cls.__table__.__drop_script__)

    def schema_exists(self, session: Session) -> bool:
        script = self.__table__.__table_exists_script__.format(database=session.connection.database)
        result = self._fetch_scalar(session, script)
        return result != 0

    def create_schema(self, session: Session) -> None:
        script = list(filter(None, self.__table__.__create_script__.split(";")))
        for line in [s for s in script if s.strip() != ""]:
            self._execute(session, line + ";")

    @staticmethod
    def _fetch_scalar(session: Session, query: str, parameters: None | dict = None) -> Any | None:
        if parameters is None:
            parameters = {}
        return session.fetch_scalar(query, parameters)

    @staticmethod
    def _fetch_one_row(session: Session, query: str, parameters: None | dict = None) -> Any | None:
        if parameters is None:
            parameters = {}
        return session.fetch_one(query, parameters)

    @staticmethod
    def _execute(session: Session, query: str, parameters: None | dict = None) -> None:
        try:
            if parameters is None:
                parameters = {}
            session.execute(query, parameters)
        except Exception as ex:
            print(ex)

    @staticmethod
    def _execute_lastrowid(session: Session, query: str, parameters: None | dict = None) -> Any:
        try:
            if parameters is None:
                parameters = {}
            value = session.execute_lastrowid(query, parameters)
            return value
        except Exception as ex:
            print(ex)

    def fetch_by_id(self, session: Session, id: None | dict) -> TableBase | None:
        if id is None:
            raise DataException("id cannot be null")
        row = self._fetch_one_row(session, self.__table__.__fetch_by_id_script__, id)
        if row:
            return self.__table__().map_row(row)
        return None

    def _item_exists(self, session: Session, id: None | dict):
        if id is None:
            raise DataException("id cannot be null")
        cnt = self._fetch_scalar(session, self.__table__.__item_exists_script__, id)
        return cnt > 0

    def fetch_one(self, session: Session, query: str, parameters: None | dict = None) -> TableBase | None:
        try:
            if parameters is None:
                parameters = {}
            row = self._fetch_one_row(session, query, parameters)
            if row:
                return self.__table__().map_row(row)
            return None
        except Exception as ex:
            print(ex)

    def fetch_multiple(self, session: Session, query: str, parameters: None | dict = None) -> None | list[TableBase]:
        try:
            if parameters is None:
                parameters = {}
            with session.fetch(query, parameters) as cursor:
                return [self.__table__().map_row(row) for row in cursor]
        except Exception as ex:
            print(ex)

    def count_all(self, session: Session):
        script = self.__table__.__table_count_script__
        return self._fetch_scalar(session, script)

    def add(self, session: Session, item: TableBase):
        pass

    def update(self, session: Session, item: TableBase):
        self._execute(session, item.__update_script__, item.get_update_params())

    def _delete_row(self, session: Session, id: None | dict):
        if id is None:
            raise DataException("id cannot be null")
        self._execute(session, self.__table__.__delete_script__, id)
