# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import typing
from pathlib import Path

from jinja2 import BaseLoader, Environment

from kiara_modules.core.defaults import TEMPLATES_FOLDER

if typing.TYPE_CHECKING:
    import pyarrow as pa


def convert_arraw_type_to_sqlite(data_type: str) -> str:

    if data_type.startswith("int") or data_type.startswith("uint"):
        return "INTEGER"

    if (
        data_type.startswith("float")
        or data_type.startswith("decimal")
        or data_type.startswith("double")
    ):
        return "REAL"

    if data_type.startswith("time") or data_type.startswith("date"):
        return "TEXT"

    if data_type == "bool":
        return "INTEGER"

    if data_type in ["string", "utf8", "large_string", "large_utf8"]:
        return "TEXT"

    if data_type in ["binary", "large_binary"]:
        return "BLOB"

    return "ANY"


def convert_arrow_column_types_to_sqlite(
    table: "pa.Table",
) -> typing.Dict[str, typing.Mapping[str, typing.Any]]:

    result: typing.Dict[str, typing.Mapping[str, typing.Any]] = {}
    for column_name in table.column_names:
        field = table.field(column_name)
        sqlite_type = convert_arraw_type_to_sqlite(str(field.type))
        result[column_name] = {"type": sqlite_type}

    return result


def create_sqlite_schema_from_arrow_table(
    table: "pa.Table",
    table_name: str,
    column_map: typing.Optional[typing.Mapping[str, str]] = None,
    index_columns: typing.Optional[typing.Iterable[str]] = None,
    extra_column_info: typing.Optional[typing.Mapping[str, str]] = None,
    extra_schema: typing.Optional[typing.Iterable[str]] = None,
    schema_template_str: str = None,
) -> str:
    """Create a sql schema statement from an Arrow table object.

    Arguments:
        table: the Arrow table object
        table_name: the table name that should be used within the database
        column_map: a map that contains column names that should be changed in the new table
        index_columns: a list of column names (after mapping) to create indexes for
        extra_column_info: a list of extra schema instructions per column name (after mapping)
        extra_schema: a list of lines to append to the created sql statement string
        schema_template_str: a different base template (must use the same template variables than original one)
    """

    if schema_template_str is None:
        template_path = Path(TEMPLATES_FOLDER) / "sqlite_schama.sql.j2"
        schema_template_str = template_path.read_text()

    template = Environment(loader=BaseLoader()).from_string(schema_template_str)

    columns = convert_arrow_column_types_to_sqlite(table=table)

    if column_map is None:
        column_map = {}

    if extra_column_info is None:
        extra_column_info = {}

    temp: typing.Dict[str, typing.Mapping[str, typing.Any]] = {}

    for cn, data in columns.items():
        if cn in column_map.keys():
            new_key = column_map[cn]
        else:
            new_key = cn
        temp_data = dict(data)
        if new_key in extra_column_info.keys():
            temp_data["extra"] = extra_column_info[new_key]
        else:
            temp_data["extra"] = ""
        temp[new_key] = temp_data

    columns = temp
    if not columns:
        raise Exception("Resulting table schema has no columns.")

    if index_columns is None:
        index_columns = []
    else:
        for ic in index_columns:
            if ic not in columns.keys():
                raise Exception(
                    f"Can't create schema, requested index column name not available: {ic}"
                )

    if extra_schema is None:
        extra_schema = []

    lines = []
    for column, column_data in columns.items():

        line = f"    {column}    {column_data['type']}"
        if column_data.get("extra", None):
            line = f"{line}    {column_data['extra']}"
        lines.append(line)

    lines.extend(extra_schema)

    rendered = template.render(
        table_name=table_name, column_info=lines, index_columns=index_columns
    )
    return rendered
