# pg2avro

Postgres to Avro generator.

## Features

- Generate Avro schema from column definition.
- Generate  data format consumable for Avro serialization.

# Usage

## Generating schema

Method: `pg2avro.get_avro_schema`

```
get_avro_schema(
    "mytable", 
    "public", 
    [
        # Dictionary mode
        {
            "name": "column_name_1",
            "type": "int2",
            "nullable": False,
        },
        # SqlAlchemy mode
        SqlAlchemyColumn(ARRAY(TEXT), name="column_name_2"),
        ...
    ]
)

```

Schema generator needs the following information:
- table name
- namespace (`schema` in SQL, `dataset` in Big Query etc.)
- columns - iterable of columns, each element with:
    - name
    - type - `_` prefix is used to indicate array types
    - nullable (optional, `True` assumed if not provided)
- column mapping - optional `ColumnMapping` object with column mappings (see below for more info).

Column data can be passed in multiple formats.

### Supported column formats

- Dictionary with required keys and data
- SqlAlchemy Column object
- Any object with compatible attributes and required data
- Dictionary or object with required data, but without compatible attributes/keys, supplied with ColumnMapping.

Note: this mode supports **generating schema from raw postgres data** - `udt_name` can be used to generate the schema.
```
columns = [
    CustomColumn(name="column_name", udt_name="int2", is_nullable=False),
]

get_avro_schema(
    table_name,
    namespace,
    columns,
    ColumnMapping(name="name", type="udt_name", nullable="is_nullable"),
)
```

## Generating rows data

Method: `pg2avro.get_avro_row_dict`

This method requires rows data and schema to generate the rows with.

### Supported row formats

- Dictionary with keys corresponding to schema field names
- Object with keys corresponding to schema field names (works the same as dictionary with corresponding fields)
- Tuple with data in the same order as fields specified in schema

```
columns = [
    {"name": "name", "type": "varchar", "nullable": False},
    {"name": "number", "type": "float4", "float4", "nullable": False},
]
schema = get_avro_schema(table_name, namespace, columns)
rows = [
    {"name": "John", "number": 1.0},
    RowObject(name="Jack", number=2.0),
    ("Jim", 3.0),
]
data = [get_avro_row_dict(row, schema) for row in rows]

```

### Overriding mappings

Some cases might require overriding standard mapping. An example of such scenario is moving pg data into google bigquery
where numeric types are handled differently and cannot accept arbitrary scale, so we may want to override that into float. 

To do so, simply pass your mapping overrides as a column name keyed dict to the `get_avro_schema` function like so:

```
columns = [
    {"name": "some_special_field", "type": "int"},
    {"name": "numeric_with_high_scale", "type": "numeric(20, 15)"},
]
overrides = {
    "some_special_field": {"pg_type": "string", "python_type": str},
    "numeric_with_high_scale": {"pg_type": "float8", "python_type": float},
}

schema = get_avro_schema(table_name, namespace, columns, mapping_overrides=overrides)
```

- `pg_type` - the type you want the column to look like for pg2avro instead of what was retrieved from pg/sqlalchemy etc.
- `python_type` - built in python type to use for typecasting. Use `str`, `float`, `int`, `tuple`, `list`, `set` and `dict` here.

And your `some_special_field` will be mapped into a string instead of int accordingly.