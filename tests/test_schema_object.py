import pytest
from pg2avro import get_avro_schema, ColumnMapping
from sqlalchemy import Column, BOOLEAN, SMALLINT, VARCHAR
from sqlalchemy.dialects.postgresql import ARRAY


def test_get_avro_schema_sqlalchemy():
    """
    Test sqlalchemy integration.
    """
    columns = [
        Column(SMALLINT, name="smallint", nullable=False),
        Column(BOOLEAN, name="bool", nullable=False),
        Column(ARRAY(VARCHAR), name="array", nullable=False),
    ]

    table_name = "test_table"
    namespace = "test_namespace"

    expected = {
        "name": table_name,
        "namespace": namespace,
        "type": "record",
        "fields": [
            {"name": "smallint", "type": "int"},
            {"name": "bool", "type": "boolean"},
            {"name": "array", "type": {"items": "string", "type": "array"}},
        ],
    }

    actual = get_avro_schema(table_name, namespace, columns)

    assert expected == actual


def test_get_avro_schema_custom_mapping():
    """
    Test custom integration using mapping class.

    TODO: Cover all sql/postgres types.
    """

    class Col:
        def __init__(self, n: str, un: str, nul: bool):
            self.n = n
            self.un = un
            self.nul = nul

    columns = [
        Col(n="smallint", un="int2", nul=False),
        Col(n="bool", un="bool", nul=False),
        Col(n="array", un="_varchar", nul=False),
    ]

    table_name = "test_table"
    namespace = "test_namespace"

    expected = {
        "name": table_name,
        "namespace": namespace,
        "type": "record",
        "fields": [
            {"name": "smallint", "type": "int"},
            {"name": "bool", "type": "boolean"},
            {"name": "array", "type": {"items": "string", "type": "array"}},
        ],
    }

    actual = get_avro_schema(
        table_name,
        namespace,
        columns,
        ColumnMapping(name="n", type="un", nullable="nul"),
    )

    assert expected == actual


def test_get_avro_schema_assumed_column_interface():
    """
    Test using compatible column object without any integration mapping.
    """

    class Column:
        def __init__(self, name: str, type: str, nullable: bool):
            self.name = name
            self.type = type
            self.nullable = nullable

    columns = [Column(name="smallint", type="smallint", nullable=False)]

    table_name = "test_table"
    namespace = "test_namespace"

    expected = {
        "name": table_name,
        "namespace": namespace,
        "type": "record",
        "fields": [{"name": "smallint", "type": "int"}],
    }

    actual = get_avro_schema(table_name, namespace, columns)

    assert expected == actual


def test_get_avro_schema_invalid_column_interface():
    """
    Test incompatible custom class with no mapping, this shall result in exception.
    """

    class Column:
        def __init__(self, name: str, data_type: str, udt_name: str, is_nullable: bool):
            self.name = name
            self.data_type = data_type
            self.udt_name = udt_name
            self.is_nullable = is_nullable

    columns = [
        Column(
            name="smallint", data_type="smallint", udt_name="int2", is_nullable=False
        )
    ]

    table_name = "test_table"
    namespace = "test_namespace"

    # Not passing column mapping, this should raise an exception.
    with pytest.raises(Exception, match="Assuming pg2avro compatible column interface"):
        get_avro_schema(table_name, namespace, columns)
