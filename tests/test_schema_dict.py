import pytest
from pg2avro import get_avro_schema, ColumnMapping


def test_get_avro_schema_custom_mapping():
    """
    Test using dictionary with custom mapping.
    """

    columns = [{"c1": "smallint", "c2": "smallint", "c3": "int2", "c4": False}]

    table_name = "test_table"
    namespace = "test_namespace"

    expected = {
        "name": table_name,
        "namespace": namespace,
        "type": "record",
        "fields": [{"name": "smallint", "type": "int"}],
    }

    actual = get_avro_schema(
        table_name,
        namespace,
        columns,
        ColumnMapping(name="c1", type="c2", nullable="c4"),
    )

    assert expected == actual


def test_get_avro_schema_assumed_column_interface():
    """
    Test using dictionary with custom mapping.
    """
    columns = [
        {
            "name": "smallint",
            "type": "smallint",
            "secondary_type": "int2",
            "nullable": False,
        }
    ]

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
    Test incompatible dict with no mapping, this shall result in exception.
    """
    columns = [{"incompatible": "smallint", "type": "smallint", "nullable": False}]

    table_name = "test_table"
    namespace = "test_namespace"

    # Not passing column mapping, this should raise an exception.
    with pytest.raises(Exception, match="Assuming pg2avro compatible column interface"):
        get_avro_schema(table_name, namespace, columns)
