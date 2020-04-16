from pg2avro import get_avro_schema, ColumnMapping
from sqlalchemy import (
    Column,
    Numeric,
    BIGINT,
    BOOLEAN,
    CHAR,
    DATE,
    INTEGER,
    NUMERIC,
    SMALLINT,
    TEXT,
    VARCHAR,
    TIME,
)
from sqlalchemy.dialects.postgresql import (
    ARRAY,
    INTERVAL,
    TIMESTAMP,
    ENUM,
    UUID,
    JSONB,
    JSON,
    DOUBLE_PRECISION,
)

from typing import Optional


def test_get_avro_schema_sqlalchemy():
    """
    Test sqlalchemy integration.

    TODO: Cover all sql/postgres types.
    """

    custom_enum_type = ("value_1", "value_2")

    columns = [
        Column(SMALLINT, name="smallint", nullable=False),
        Column(BIGINT, name="bigint", nullable=False),
        Column(INTEGER, name="integer", nullable=False),
        Column(NUMERIC(10, 2), name="numeric", nullable=False),
        Column(DOUBLE_PRECISION, name="double_precision", nullable=False),
        Column(BOOLEAN, name="bool", nullable=False),
        Column(DATE, name="date", nullable=False),
        Column(TIME, name="time", nullable=False),
        Column(TIMESTAMP, name="timestamp", nullable=False),
        Column(CHAR, name="char", nullable=False),
        Column(TEXT, name="text", nullable=True),
        Column(VARCHAR(255), primary_key=True, name="varchar", nullable=False),
        Column(ARRAY(VARCHAR), name="array", nullable=False),
        Column(INTERVAL, name="interval", nullable=False),
        Column(ENUM(name="some_enum", *custom_enum_type), name="enum", nullable=False),
        Column(UUID, name="uuid", nullable=False),
        Column(JSONB, name="jsonb", nullable=False),
        Column(JSON, name="json", nullable=False),
    ]

    table_name = "test_table"
    namespace = "test_namespace"

    expected = {
        "name": table_name,
        "namespace": namespace,
        "type": "record",
        "fields": [
            {"name": "smallint", "type": "int"},
            {"name": "bigint", "type": "long"},
            {"name": "integer", "type": "int"},
            {
                "name": "numeric",
                "type": {
                    "logicalType": "decimal",
                    "type": "bytes",
                    "precision": 10,
                    "scale": 2,
                },
            },
            {"name": "double_precision", "type": "double"},
            {"name": "bool", "type": "boolean"},
            {"name": "date", "type": {"logicalType": "date", "type": "int"}},
            {
                "name": "time",
                "type": {"logicalType": "timestamp-millis", "type": "int"},
            },
            {
                "name": "timestamp",
                "type": {"logicalType": "timestamp-millis", "type": "long"},
            },
            {"name": "char", "type": "string"},
            {"name": "text", "type": ["null", "string"]},
            {"name": "varchar", "type": "string"},
            {"name": "array", "type": {"items": "string", "type": "array"}},
            {"name": "interval", "type": "string"},
            {"name": "enum", "type": "string"},
            {"name": "uuid", "type": "string"},
            {"name": "jsonb", "type": "string"},
            {"name": "json", "type": "string"},
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
        def __init__(
            self,
            n: str,
            un: str,
            nul: bool,
            np: Optional[int] = None,
            ns: Optional[int] = None,
        ):
            self.n = n
            self.un = un
            self.nul = nul
            self.np = np
            self.ns = ns

    columns = [
        Col(n="smallint", un="int2", nul=False),
        Col(n="bigint", un="int8", nul=False),
        Col(n="integer", un="int4", nul=False),
        Col(n="numeric", un="numeric", nul=False, np=3, ns=7),
        Col(n="double_precision", un="float8", nul=False),
        Col(n="real", un="float4", nul=False),
        Col(n="bool", un="bool", nul=False),
        Col(n="char", un="char", nul=False),
        Col(n="bpchar", un="bpchar", nul=False),
        Col(n="varchar", un="varchar", nul=False),
        Col(n="array", un="_varchar", nul=False),
        Col(n="array_n", un="_varchar", nul=True),
        Col(n="date", un="date", nul=False),
        Col(n="time", un="time", nul=False),
        Col(n="timestamp", un="timestamp", nul=False),
        Col(n="enum", un="custom_type", nul=False),
        Col(n="uuid", un="uuid", nul=False),
        Col(n="json", un="json", nul=False),
        Col(n="jsonb", un="jsonb", nul=False),
    ]

    table_name = "test_table"
    namespace = "test_namespace"

    expected = {
        "name": table_name,
        "namespace": namespace,
        "type": "record",
        "fields": [
            {"name": "smallint", "type": "int"},
            {"name": "bigint", "type": "long"},
            {"name": "integer", "type": "int"},
            {
                "name": "numeric",
                "type": {
                    "logicalType": "decimal",
                    "type": "bytes",
                    "precision": 3,
                    "scale": 7,
                },
            },
            {"name": "double_precision", "type": "double"},
            {"name": "real", "type": "float"},
            {"name": "bool", "type": "boolean"},
            {"name": "char", "type": "string"},
            {"name": "bpchar", "type": "string"},
            {"name": "varchar", "type": "string"},
            {"name": "array", "type": {"items": "string", "type": "array"}},
            {"name": "array_n", "type": ["null", {"items": "string", "type": "array"}]},
            {"name": "date", "type": {"logicalType": "date", "type": "int"}},
            {
                "name": "time",
                "type": {"logicalType": "timestamp-millis", "type": "int"},
            },
            {
                "name": "timestamp",
                "type": {"logicalType": "timestamp-millis", "type": "long"},
            },
            {"name": "enum", "type": "string"},
            {"name": "uuid", "type": "string"},
            {"name": "json", "type": "string"},
            {"name": "jsonb", "type": "string"},
        ],
    }

    actual = get_avro_schema(
        table_name,
        namespace,
        columns,
        ColumnMapping(
            name="n",
            type="un",
            nullable="nul",
            numeric_precision="np",
            numeric_scale="ns",
        ),
    )

    assert expected == actual
