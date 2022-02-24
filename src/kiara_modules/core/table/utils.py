# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import typing

from kiara_modules.core.database import SqliteTableSchema

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
) -> typing.Dict[str, typing.Dict[str, typing.Any]]:

    result: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
    for column_name in table.column_names:
        field = table.field(column_name)
        sqlite_type = convert_arraw_type_to_sqlite(str(field.type))
        result[column_name] = {"data_type": sqlite_type}

    return result


def create_sqlite_schema_data_from_arrow_table(
    table: "pa.Table",
    column_map: typing.Optional[typing.Mapping[str, str]] = None,
    index_columns: typing.Optional[typing.Iterable[str]] = None,
    extra_column_info: typing.Optional[
        typing.Mapping[str, typing.Iterable[str]]
    ] = None,
) -> SqliteTableSchema:
    """Create a sql schema statement from an Arrow table object.

    Arguments:
        table: the Arrow table object
        column_map: a map that contains column names that should be changed in the new table
        index_columns: a list of column names (after mapping) to create module_indexes for
        extra_column_info: a list of extra schema instructions per column name (after mapping)
    """

    columns = convert_arrow_column_types_to_sqlite(table=table)

    if column_map is None:
        column_map = {}

    if extra_column_info is None:
        extra_column_info = {}

    temp: typing.Dict[str, typing.Dict[str, typing.Any]] = {}

    if index_columns is None:
        index_columns = []

    for cn, data in columns.items():
        if cn in column_map.keys():
            new_key = column_map[cn]
        else:
            new_key = cn
        temp_data = dict(data)
        if new_key in extra_column_info.keys():
            temp_data["extra_column_info"] = extra_column_info[new_key]
        else:
            temp_data["extra_column_info"] = [""]
        if cn in index_columns:
            temp_data["create_index"] = True
        temp[new_key] = temp_data

    columns = temp
    if not columns:
        raise Exception("Resulting table schema has no columns.")
    else:
        for ic in index_columns:
            if ic not in columns.keys():
                raise Exception(
                    f"Can't create schema, requested index column name not available: {ic}"
                )

    return SqliteTableSchema(columns=columns, column_map=column_map)
