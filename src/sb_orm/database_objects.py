from enum import Enum
from typing import Any, Union, List

from sb_serializer import Name


class DataException(Exception):
    pass


class DatatypeException(DataException):
    pass


class FieldType:
    pass


class FieldType(Enum):
    Undefined = 0
    Integer = 1
    String = 2
    Float = 3
    Decimal = 4
    Datetime = 5
    Boolean = 6
    Item = 7
    ListOfItem = 8

    @classmethod
    def get_fieldtype(cls, value: str) -> FieldType:
        value = value.lower()
        if value == "integer" or value == "int" or value == "bigint" or value == "tinyint":
            return FieldType.Integer
        elif value == "string" or value == "varchar" or value == "char" or value == "text":
            return FieldType.String
        elif value == "float" or value == "real":
            return FieldType.Float
        elif value == "datetime" or value == "date":
            return FieldType.Datetime
        elif value == "boolean" or value == "bool":
            return FieldType.Boolean
        elif value == "decimal" or value == "money":
            return FieldType.Decimal
        elif value == "__item__":
            return FieldType.Item
        elif value == "[__item__]":
            return FieldType.ListOfItem
        elif value == "none":
            return FieldType.Undefined
        else:
            raise DatatypeException(f"Unknown field type {value}")

    def __str__(self):
        if self == FieldType.Integer:
            return "Integer"
        elif self == FieldType.String:
            return "String"
        elif self == FieldType.Float:
            return "Float"
        elif self == FieldType.Datetime:
            return "Datetime"
        elif self == FieldType.Boolean:
            return "Boolean"
        elif self == FieldType.Decimal:
            return "Decimal"
        elif self == FieldType.Item:
            return "__Item__"
        elif self == FieldType.ListOfItem:
            return "[__Item__]"
        elif self == FieldType.Undefined:
            return "None"
        else:
            raise DatatypeException("Unknown field type ")


class Field(object):
    name: Name
    type: FieldType
    size: int
    scale: int
    auto_increment: bool
    default: Any
    required: bool

    def __init__(self, name: Name = None, field_type: FieldType = FieldType.Undefined, size: int = 0, scale: int = 0,
                 auto_increment: bool = False, default=None, required: bool = False):
        self.name = name
        self.type = field_type
        self.size = size
        self.scale = scale
        self.auto_increment = auto_increment
        self.default = default
        self.required = required

    def __str__(self):
        return f"{str(self.name)} {self.type} ({self.size},{self.scale}) {'AUTOINC ' if self.auto_increment else ''}" \
               f"{'DEFAULT ' + self.default + ' ' if self.default else ''}{'NOT NULL' if self.required else 'NULL'}"


class KeyType:
    pass


class KeyType(Enum):
    Undefined = 0
    PrimaryKey = 1
    Index = 2
    Unique = 3
    ForeignKey = 4
    Lookup = 5

    @classmethod
    def get_keytype(cls, value: str) -> KeyType:
        value = value.lower()
        if value == "undefined":
            return KeyType.Undefined
        elif value == "primarykey" or value == "primary key":
            return KeyType.PrimaryKey
        elif value == "index":
            return KeyType.Index
        elif value == "unique":
            return KeyType.Unique
        elif value == "foreignkey" or value == "foreign key":
            return KeyType.ForeignKey
        elif value == "lookup":
            return KeyType.Lookup
        else:
            raise DatatypeException(f"Unknown key type {value}")

    def __str__(self):
        if self == KeyType.Undefined:
            return "Undefined"
        elif self == KeyType.PrimaryKey:
            return "PrimaryKey"
        elif self == KeyType.Index:
            return "Index"
        elif self == KeyType.Unique:
            return "Unique"
        elif self == KeyType.ForeignKey:
            return "ForeignKey"
        elif self == KeyType.Lookup:
            return "Lookup"
        else:
            raise DatatypeException("Unknown key type ")


class Key(object):
    name: Name
    fields: List[str]
    primary_table: str
    primary_fields: List[str]
    referenced_table: str
    key_type: KeyType

    def __init__(self, name: Name = None, key_type: KeyType = KeyType.Undefined, fields:list[str]= [], primary_table: str = "", primary_fields:list[str]=[]):
        self.name = name
        self.fields: List[str] = fields
        self.primary_table = primary_table
        self.primary_fields: List[str] = primary_fields
        self.key_type = key_type
        self.referenced_table = ""

    def __str__(self):
        return f"{self.name} {self.key_type} {','.join(self.fields)}{self.primary_table}" \
               f"{'' if len(self.primary_fields) == 0 else ','.join(self.primary_fields)}"


class TransformType:
    pass


class TransformType(Enum):
    Undefined = 0
    Map = 1
    IntToBool = 2

    @classmethod
    def get_transformtype(cls, value: str) -> TransformType:
        value = value.lower()
        if value == "undefined" or value == "none":
            return TransformType.Undefined
        elif value == "map":
            return TransformType.Map
        elif value == "inttobool":
            return TransformType.IntToBool
        else:
            raise DatatypeException(f"Unknown transform type {value}")


class QueryType:
    pass


class QueryType(Enum):
    Undefined = 0
    FetchScalar = 1
    FetchOne = 2
    FetchAll = 3
    Execute = 4

    @classmethod
    def get_querytype(cls, value: str) -> QueryType:
        value = value.lower()
        if value == "undefined":
            return QueryType.Undefined
        elif value == "fetchscalar":
            return QueryType.FetchScalar
        elif value == "fetchone":
            return QueryType.FetchOne
        elif value == "fetchall":
            return QueryType.FetchAll
        elif value == "execute":
            return QueryType.Execute
        else:
            raise DatatypeException("Unknown query type {}".format(value))


class Parameter(object):
    name: Name
    type: FieldType

    def __init__(self, name: Name = None, type: FieldType = FieldType.Undefined):
        self.name = name
        self.type = type


class CustomQuery(object):
    name: Name
    parameters: List[Parameter]
    return_type: FieldType
    transform: TransformType
    query_type: QueryType
    query: str

    def __init__(self, name: Name = None):
        self.name = name
        self.parameters: List[Parameter] = list()
        self.return_type = FieldType.Undefined
        self.transform = TransformType.Undefined
        self.query_type = QueryType.Undefined
        self.query = ""

    def __str__(self):
        return str(self.name)


class Table(object):
    name: Name
    fields: list[Field]
    pk: Key | None
    keys: list[Key]
    foreign_keys: list[Key]
    custom_queries: list[CustomQuery]

    def __init__(self, name: Name = None, fields: list[Field] = [], pk: Key = None, keys: list[Key] = [],
                 foreign_keys: list[Key] = [], custom_queries: list[CustomQuery] = []):
        self.name = name
        self.fields: list[Field] = fields
        self.pk: Key | None = pk
        self.keys: list[Key] = keys
        self.foreign_keys: list[Key] = foreign_keys
        self.custom_queries: list[CustomQuery] = custom_queries

    def find_field(self, name: str) -> Field:
        found_fields = [f for f in self.fields if f.name.raw().lower() == name.lower()]
        if len(found_fields) > 0:
            return found_fields[0]
        else:
            raise DataException("Could not find field")

    def __str__(self):
        return str(self.name)


class Database(object):
    name: Name
    tables: List[Table]

    def __init__(self, name: Name = None):
        self.name = name
        self.tables: List[Table] = []

    def get_table(self, table_name: str):
        result = [table for table in self.tables if table.name.raw() == table_name]
        if len(result) > 0:
            return result[0]
        return None
