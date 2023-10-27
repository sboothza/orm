from sb_db_common import Session

from src.sb_orm import RepositoryBase
from tests.test_table import TestTable


class TestRepository(RepositoryBase):
    __table__ = TestTable

    def add(self, session: Session, item: TestTable):
        id = self._execute_lastrowid(session, item.__insert_script__, item.get_insert_params())
        item.id = id
