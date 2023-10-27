import unittest

from sb_db_common import SessionFactory
from sb_serializer import Naming

from src.sb_orm.adaptor_factory import AdaptorFactory
from tests.test_repo import TestRepository
from tests.test_table import TestTable


class BasicRepoTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.naming: Naming = Naming("S:\\src_repo\\python\\serializer\\dictionary.txt",
                                    "S:\\src_repo\\python\\serializer\\bigworddictionary.txt")

    def init_common(self, connection_string: str):
        with SessionFactory.connect(connection_string=connection_string) as session:
            adaptor = AdaptorFactory.get_adaptor_for_connection_string(connection_string, self.naming)
            TestTable.build_queries(adaptor, session.connection.database)
            repo: TestRepository = TestRepository()
            if not repo.schema_exists(session):
                repo.create_schema(session)

            self.assertTrue(repo.schema_exists(session))

    def crud(self, connection_string: str):
        with SessionFactory.connect(connection_string=connection_string) as session:
            adaptor = AdaptorFactory.get_adaptor_for_connection_string(connection_string, self.naming)
            TestTable.build_queries(adaptor, session.connection.database)
            repo: TestRepository = TestRepository()
            # trash table
            repo.drop_schema(session)
            repo.create_schema(session)
            session.commit()

            # insert
            item1 = TestTable(name="item1")
            item2 = TestTable(name="item2")
            repo.add(session, item1)
            repo.add(session, item2)
            session.commit()
            self.assertEqual(item1.id, 1)
            self.assertEqual(item2.id, 2)

            # select
            new_item1: TestTable = repo.fetch_by_id(session, {"id": 1})
            new_item2: TestTable = repo.fetch_by_id(session, {"id": 2})
            self.assertEqual(new_item1.name, item1.name)
            self.assertEqual(new_item2.name, item2.name)

            # update
            item1.name = "item1_update"
            repo.update(session, item1)
            session.commit()
            new_item1: TestTable = repo.fetch_by_id(session, {"id": item1.id})
            self.assertEqual(new_item1.name, item1.name)

            # delete
            repo._delete_row(session, {"id": 1})
            repo._delete_row(session, {"id": 2})
            session.commit()
            self.assertFalse(repo._item_exists(session, {"id": item1.id}))
            self.assertFalse(repo._item_exists(session, {"id": item2.id}))

    def test_init_sqlite(self):
        self.init_common("sqlite://testdb.db")

    def test_init_mysql(self):
        self.init_common("mysql://root:or9asm1c@milleniumfalcon/test")

    def test_init_pgsql(self):
        self.init_common("pgsql://postgres:or9asm1c@milleniumfalcon/test")

    def test_sqlite_crud(self):
        self.crud("sqlite://testdb.db")

    def test_mysql_crud(self):
        self.crud("mysql://root:or9asm1c@milleniumfalcon/test")

    def test_pgsql_crud(self):
        self.crud("pgsql://postgres:or9asm1c@milleniumfalcon/test")
