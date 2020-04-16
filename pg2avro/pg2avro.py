from datetime import datetime, date, timedelta
import re
from textwrap import dedent
from typing import Iterable, Dict, Union, List, Optional
import json
from sqlalchemy.sql.schema import Column as SqlAlchemyColumn
from psycopg2.extras import NumericRange, DateRange

AVRO_POSTGRES_MAP = {
    "boolean": ("bool", "boolean"),
    "string": (
        "char",
        "character",
        "bpchar",
        "enum",  # TODO: AVRO supports proper enums, make that work.
        "json",
        "jsonb",
        "inet",
        "text",
        "uuid",
        "varchar",
        "character varying",
        "interval",
    ),
    "int": ("smallint", "integer", "int", "int2", "int4", "date", "time"),
    "long": (
        "bigint",
        "int8",
        "timestamp",
        "timestamptz",
        "timestamp without time zone",
        "timestamp with time zone",
    ),
    "float": ("real", "float4"),
    "double": ("float8", "double precision", "double_precision"),
    "array": ("array", "daterange", "int4range", "int2vector"),
    "bytes": ("numeric",),
}
ARRAY_TYPES = ("array", "daterange", "int4range")
LOGICAL_TYPES_AVRO_MAP = {
    "timestamp-millis": (
        "time",
        "timestamp",
        "timestamptz",
        "timestamp without time zone",
        "timestamp with time zone",
    ),
    "date": ("date",),
    "decimal": ("numeric",),
}
REQUIRED_COLUMN_ATTRIBUTES = ["name", "type"]
COLUMN_ATTRIBUTES = REQUIRED_COLUMN_ATTRIBUTES + [
    "nullable",
    "numeric_precision",
    "numeric_scale",
]

BUILTIN_TYPES = [tuple, list, set, int, float, str, dict]

# Generate flipped maps for easier lookup.
TYPE_MAP = {
    postgres_type: avro_type
    for avro_type, postgres_types in AVRO_POSTGRES_MAP.items()
    for postgres_type in postgres_types
}
LOGICAL_TYPES_MAP = {
    postgres_type: avro_type
    for avro_type, postgres_types in LOGICAL_TYPES_AVRO_MAP.items()
    for postgres_type in postgres_types
}


class ColumnMapping:
    def __init__(
        self,
        name: str,
        type: str,
        nullable: str,
        numeric_precision: str,
        numeric_scale: str,
    ):
        self.name = name
        self.type = type
        self.nullable = nullable
        self.numeric_precision = numeric_precision
        self.numeric_scale = numeric_scale


class ColumnAdapter(object):
    def __init__(self, obj, **adapted_methods):
        self.obj = obj
        self.__dict__.update(adapted_methods)

    def __getattr__(self, attr):
        return getattr(self.obj, attr)


class Column:
    def __init__(
        self,
        name: str,
        type: str,
        nullable: bool = True,
        numeric_precision: Optional[int] = None,
        numeric_scale: Optional[int] = None,
    ):
        self.name = name
        self.type = type
        self.nullable = nullable
        self.numeric_scale = numeric_scale
        self.numeric_precision = numeric_precision


def get_avro_schema(
    table_name: str,
    namespace: str,
    columns: Iterable,
    column_mapping: ColumnMapping = None,
) -> Dict:
    """
    Generates AVRO Schema for given postgres schema.

    Function works in 2 basic modes:
    - Object mode - supports:
        - Seamless sqlalchemy integration - sqlalchemy Column objects passed as columns definition:
            - pg2avro picks up the sqlalchemy column objects and uses them to generate schema,
            no extra steps required.
        - Manual object integration using ColumnMapping:
            - User provides arbitrary objects but provides column mapping so that pg2avro knows how to use the provided
            objects.
        - Assumed compatibility, i.e no known column objects and no mapping provided:
            - pg2avro assumes compatible row data is passed, tries to use the passed columns to generate schema.
    - Dictionary mode - supports:
        - Manual dictionary integration using ColumnMapping:
            - User provides arbitrary dicts but provides column mapping so that pg2avro knows how to use the provided
            dicts.
        - Assumed compatibility, i.e no known dicts and no mapping provided:
            - pg2avro assumes compatible row data is passed, tries to use the passed columns to generate schema.

    :param table_name: The name of the table.
    :param namespace: The namespace of the schema.
    :param columns: Columns to generate schema for.
    :param column_mapping:
    :return: Avro schema dictionary.
    """

    avro_schema = {
        "namespace": namespace,
        "name": table_name,
        "type": "record",
        "fields": [],
    }

    for column in columns:
        # Generate fields schema for each column definition.
        if isinstance(column, Dict):
            column = _dict_to_column(column, column_mapping)
        elif isinstance(column, object) and type(column) not in BUILTIN_TYPES:
            column = _object_to_column(column, column_mapping)
        else:
            raise Exception(f"Unsupported column type {type(column)}.")

        # Ensure default values.
        if not hasattr(column, "nullable"):
            column.nullable = True

        # Work with lowercase types only.
        if column.type is not None:
            column.type = column.type.lower()

        field = {"name": column.name, "type": _get_avro_type(column)}

        avro_schema["fields"].append(field)

    return avro_schema


def get_avro_row_dict(row, schema: Dict) -> Dict:
    """
    Generates Avro row dictionary for given row using given avro schema.

    TODO: Handle situations where row object and schema definition do not match.

    :param row: Object to generate Avro row for.
    :type row: Object with compatible attributes, tuple, list or dict.
    :param schema: Schema to generate the Avro row with.
    :return: Row dict.
    """
    avro_dict = {}

    for schema_row in schema["fields"]:
        k = schema_row["name"]
        # TODO: check if row is compatible with schema.
        v = _get_row_attr(row, k, schema["fields"])
        column_type = schema_row["type"]

        if v is None and column_type == "array":
            avro_dict[k] = []
        elif isinstance(v, dict):
            avro_dict[k] = json.dumps(v)
        elif isinstance(v, datetime):
            avro_dict[k] = int(v.timestamp() * 1000)
        elif isinstance(v, date):
            avro_dict[k] = (v - date(1970, 1, 1)).days
        elif isinstance(v, timedelta):
            avro_dict[k] = str(v)
        # Map specific types from supported libraries.
        # TODO: Cover all types that require special handling.
        elif isinstance(v, DateRange):
            lower = (v.lower - date(1970, 1, 1)).days
            upper = (v.upper - date(1970, 1, 1)).days
            avro_dict[k] = [lower, upper]
        elif isinstance(v, NumericRange):
            avro_dict[k] = [v.lower, v.upper]
        elif isinstance(v, list):
            avro_dict[k] = [i for i in v if i is not None]
        else:
            avro_dict[k] = v

    return avro_dict


def _get_row_attr(row, attribute: str, fields_schema: Dict):
    if isinstance(row, Dict):
        return row.get(attribute)
    elif isinstance(row, tuple) or isinstance(row, list):
        index = next(
            (
                index
                for (index, d) in enumerate(fields_schema)
                if d["name"] == attribute
            ),
            None,
        )
        return row[index]
    elif isinstance(row, object) and type(row) not in BUILTIN_TYPES:
        return getattr(row, attribute)
    raise Exception("Unsupported row type.")


def _dict_to_column(column: Dict, column_mapping: ColumnMapping) -> Column:
    """
    Convert dictionary into internally recognized Column.
    :param column: Column dictionary
    :param column_mapping:
    :return: Column
    """
    # User passed column passing, use it to generate column interface adapter.
    if column_mapping:
        column = Column(
            name=column.get(column_mapping.name),
            type=column.get(column_mapping.type),
            nullable=column.get(column_mapping.nullable, True),
            numeric_precision=column.get(column_mapping.numeric_precision, None),
            numeric_scale=column.get(column_mapping.numeric_scale, None),
        )
    else:
        # No column mapping, assume user provided compatible column data.
        _check_required_attributes(list(column.keys()))

        column = Column(
            name=column.get("name"),
            type=column.get("type"),
            nullable=column.get("nullable", True),
            numeric_precision=column.get("numeric_precision", None),
            numeric_scale=column.get("numeric_scale", None),
        )
    return column


def _object_to_column(column, column_mapping: ColumnMapping) -> Column:
    """
    Convert an object into internally recognized Column.
    :param column: Object representing column
    :param column_mapping: Column mapping to use/
    :return: Column
    """
    # User passed column passing, use it to generate column interface adapter.
    if column_mapping:
        column = ColumnAdapter(
            column,
            name=getattr(column, column_mapping.name),
            type=getattr(column, column_mapping.type),
            nullable=getattr(column, column_mapping.nullable, True),
            numeric_precision=getattr(column, column_mapping.numeric_precision, None),
            numeric_scale=getattr(column, column_mapping.numeric_scale, None),
        )
    else:
        # No column mapping, detect passed column type and try to match it with our internal mappings.
        if isinstance(column, SqlAlchemyColumn):
            type_str = str(column.type)
            numeric_precision = None
            numeric_scale = None

            # Special handling for types that have more constraints
            if "numeric" in type_str.lower():
                num_def = re.findall(r"\d+", type_str)
                numeric_precision = int(num_def[0]) if len(num_def) > 0 else None
                numeric_scale = int(num_def[1]) if len(num_def) > 1 else None

            column = ColumnAdapter(
                column,
                name=column.name,
                type=f"_{column.type.item_type.__visit_name__}"
                if column.type.__visit_name__ == "ARRAY"
                else column.type.__visit_name__,
                nullable=column.nullable,
                numeric_precision=numeric_precision,
                numeric_scale=numeric_scale,
            )
        else:
            # No matching internal mapping found, assume user provided compatible column data.
            _check_required_attributes(column.__dict__)
    return column


def _check_required_attributes(attributes: List):
    """
    Check if all required column attributes are present in given attributes list.
    :param attributes: The attributes to check.
    """
    # Todo: cover this case with tests.
    if not set(REQUIRED_COLUMN_ATTRIBUTES) <= set(attributes):
        raise Exception(
            dedent(
                f"""
                Assuming pg2avro compatible column interface, "{attributes}" attributes provided.
                Required column attributes: {REQUIRED_COLUMN_ATTRIBUTES}.
                """
            )
        )


def _get_avro_type(column) -> Union[Dict, str]:
    """
    Determine Avro type for specified column.

    :param column: Column object to determine type for. Compatibility assumed at this point.
    :return: Column Avro type definition.
    """
    is_array_type = False

    if column.type.startswith("_"):
        is_array_type = True
        column.type = column.type[1:]

    if column.type in TYPE_MAP:
        column_type = column.type
    else:
        # Cover all custom and unidentified types as text.
        column_type = "text"

    avro_type = TYPE_MAP.get(column_type, None)
    logical_type = LOGICAL_TYPES_MAP.get(column_type, None)

    if avro_type:
        if logical_type:
            avro_type = {"type": avro_type, "logicalType": logical_type}

        if is_array_type:
            avro_type = {"type": "array", "items": avro_type}

        # Special cases handling.
        if logical_type == "decimal":
            avro_type["precision"] = column.numeric_precision or 24
            avro_type["scale"] = column.numeric_scale or 2

        # Nullable types handling.
        if column.nullable:
            avro_type = ["null", avro_type]

        return avro_type

    # Todo: cover this case with tests.
    raise Exception(f'Type "{column_type}" type conversion to AVRO failed.')
