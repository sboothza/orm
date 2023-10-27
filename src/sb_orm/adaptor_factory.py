import re

from sb_serializer import Naming

from src.sb_orm.adaptor import Adaptor
from src.sb_orm.mysql_adaptor import MySqlAdaptor
from src.sb_orm.pgsql_adaptor import PgSqlAdaptor
from src.sb_orm.sqlite_adaptor import SqliteAdaptor


class AdaptorFactory(object):
    @classmethod
    def get_adaptor_for_connection_string(cls, connection_string: str, naming: Naming) -> Adaptor:
        match = re.search(r"(\w+):\/\/(.+)", connection_string)
        if match:
            db_type = match.group(1)
            connection = match.group(2)

            if db_type == "sqlite":
                return SqliteAdaptor(connection_string, naming)
            elif db_type == "mysql":
                return MySqlAdaptor(connection_string, naming)
            elif db_type == "pgsql":
                return PgSqlAdaptor(connection_string, naming)

    @classmethod
    def get_adaptor_for_dbtype(cls, dbtype: str, naming: Naming) -> Adaptor:
        if dbtype.lower() == "sqlite":
            return SqliteAdaptor("memory", naming)
        elif dbtype.lower() == "mysql":
            return MySqlAdaptor(MySqlAdaptor.__blank_connection__, naming)
        elif dbtype.lower() == "pgsql":
            return PgSqlAdaptor(PgSqlAdaptor.__blank_connection__, naming)
