from pg2avro import get_avro_schema, ColumnMapping, get_avro_row_dict
from sqlalchemy import (
    Column,
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
        Column(NUMERIC(10, 10), name="numeric_to_double", nullable=False),
        Column(NUMERIC, name="numeric_defaults", nullable=False),
        Column(NUMERIC, name="numeric_nullable", nullable=True),
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
            {"name": "numeric_to_double", "type": "double"},
            {
                "name": "numeric_defaults",
                "type": {
                    "logicalType": "decimal",
                    "type": "bytes",
                    "precision": 38,
                    "scale": 9,
                },
            },
            {
                "name": "numeric_nullable",
                "type": [
                    "null",
                    {
                        "logicalType": "decimal",
                        "type": "bytes",
                        "precision": 38,
                        "scale": 9,
                    },
                ],
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
        Col(n="numeric_to_double", un="numeric", nul=False, np=10, ns=10),
        Col(n="numeric_defaults", un="numeric", nul=False),
        Col(n="numeric_nullable", un="numeric", nul=True),
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
            {"name": "numeric_to_double", "type": "double"},
            {
                "name": "numeric_defaults",
                "type": {
                    "logicalType": "decimal",
                    "type": "bytes",
                    "precision": 38,
                    "scale": 9,
                },
            },
            {
                "name": "numeric_nullable",
                "type": [
                    "null",
                    {
                        "logicalType": "decimal",
                        "type": "bytes",
                        "precision": 38,
                        "scale": 9,
                    },
                ],
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


def test_mapping_overrides():
    """
    Test mapping overrides
    """

    from pg2avro.pg2avro import Column

    table_name = "test_table"
    namespace = "test_namespace"

    columns = [
        Column(name="int_to_string", type="int"),
        Column(name="string_to_numeric", type="string"),
        Column(name="not_overriden", type="int"),
        Column(name="numeric_to_float", type="numeric"),
        Column(name="array_to_string", type="_varchar"),
        Column(name="string_to_array", type="varchar"),
    ]
    overrides = {
        "int_to_string": {"pg_type": "string", "python_type": str},
        "string_to_numeric": {"pg_type": "numeric", "python_type": float},
        "not_matching_override_name": {"pg_type": "int", "python_type": int},
        "numeric_to_float": {"pg_type": "float8", "python_type": float},
        "array_to_string": {"pg_type": "string", "python_type": str},
        "string_to_array": {"pg_type": "_string", "python_type": list},
    }

    expected_schema = {
        "name": table_name,
        "namespace": namespace,
        "type": "record",
        "fields": [
            {"name": "int_to_string", "type": ["null", "string"]},
            {
                "name": "string_to_numeric",
                "type": [
                    "null",
                    {
                        "type": "bytes",
                        "logicalType": "decimal",
                        "precision": 38,
                        "scale": 9,
                    },
                ],
            },
            {"name": "not_overriden", "type": ["null", "int"]},
            {"name": "numeric_to_float", "type": ["null", "double"]},
            {"name": "array_to_string", "type": ["null", "string"]},
            {
                "name": "string_to_array",
                "type": ["null", {"type": "array", "items": "string"}],
            },
        ],
    }

    schema = get_avro_schema(
        table_name, namespace, columns, mapping_overrides=overrides
    )

    assert expected_schema == schema

    # Now data
    rows_data = [
        {
            "int_to_string": 1,
            "string_to_numeric": "2.0",
            "not_overriden": 3,
            "numeric_to_float": 0.12345678910,
            "array_to_string": [1, 2, "a", "b"],
            "string_to_array": "asd",
        },
        {
            "int_to_string": None,
            "string_to_numeric": None,
            "not_overriden": None,
            "numeric_to_float": None,
            "array_to_string": None,
            "string_to_array": None,
        },
    ]
    expected = [
        {
            "int_to_string": "1",
            "string_to_numeric": 2.0,
            "not_overriden": 3,
            "numeric_to_float": 0.12345678910,
            "array_to_string": "[1, 2, 'a', 'b']",
            "string_to_array": ["a", "s", "d"],
        },
        {
            "int_to_string": None,
            "string_to_numeric": None,
            "not_overriden": None,
            "numeric_to_float": None,
            "array_to_string": None,
            "string_to_array": None,
        },
    ]

    actual = [get_avro_row_dict(r, schema, overrides) for r in rows_data]

    assert expected == actual
