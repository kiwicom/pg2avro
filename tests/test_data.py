from collections import OrderedDict
from typing import List
from pg2avro import get_avro_schema, get_avro_row_dict
import json


def test_get_avro_row_row_types():
    """
    Test generating Avro rows from different source row data.

    TODO: Cover more than the simplest golden path.
    """
    columns = [
        {"name": "name", "type": "varchar", "nullable": False},
        {"name": "number", "type": "float4", "nullable": False},
        {"name": "list", "type": "_varchar", "nullable": False},
        {"name": "is_working", "type": "bool", "nullable": False},
    ]

    table_name = "test_table"
    namespace = "test_namespace"

    schema = get_avro_schema(table_name, namespace, columns)

    expected = [
        {
            "name": "example-01",
            "number": 1.0,
            "list": ["list", "of", "strings"],
            "is_working": True,
        },
        {
            "name": "example-02",
            "number": 2.5,
            "list": ["another", "list", "of", "strings"],
            "is_working": False,
        },
    ]

    class Row:
        def __init__(self, name: str, number: float, list: List[str], is_working: bool):
            self.name = name
            self.number = number
            self.list = list
            self.is_working = is_working

    rows_data = [
        # Compatible Row objects.
        [
            Row("example-01", 1.0, "list of strings".split(), True),
            Row("example-02", 2.5, "another list of strings".split(), False),
        ],
        # Compatible Dicts.
        [
            {
                "name": "example-01",
                "number": 1.0,
                "list": "list of strings".split(),
                "is_working": True,
            },
            {
                "name": "example-02",
                "number": 2.5,
                "list": "another list of strings".split(),
                "is_working": False,
            },
        ],
        # Compatible Dicts, but extended class.
        [
            OrderedDict(
                {
                    "name": "example-01",
                    "number": 1.0,
                    "list": "list of strings".split(),
                    "is_working": True,
                }
            ),
            OrderedDict(
                {
                    "name": "example-02",
                    "number": 2.5,
                    "list": "another list of strings".split(),
                    "is_working": False,
                }
            ),
        ],
        # Compatible Tuples.
        [
            ("example-01", 1.0, "list of strings".split(), True),
            ("example-02", 2.5, "another list of strings".split(), False),
        ],
    ]

    for row_data in rows_data:
        actual = [get_avro_row_dict(r, schema) for r in row_data]

        assert expected == actual


def test_get_avro_row_dict_special_data_types():
    """
    Test generating Avro rows from data, using special types.
    """
    columns = [
        {"name": "json_col", "type": "json"},
        {"name": "jsonb_col", "type": "jsonb"},
        {"name": "empty_list", "type": "_varchar"},
    ]

    table_name = "test_table"
    namespace = "test_namespace"
    schema = get_avro_schema(table_name, namespace, columns)

    json_1 = {"key1": "val1"}
    json_2 = {"key2": "val2", "key3": [1, 2], "key4": {"key5": "val5"}}

    expected = [
        {
            "json_col": json.dumps(json_1),
            "jsonb_col": json.dumps(json_2),
            "empty_list": [],
        },
        {
            "json_col": json.dumps(json_2),
            "jsonb_col": json.dumps(json_1),
            "empty_list": None,
        },
    ]

    actual = [
        get_avro_row_dict(r, schema)
        for r in [(json_1, json_2, []), (json_2, json_1, None)]
    ]

    assert expected == actual
