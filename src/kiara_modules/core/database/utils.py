# -*- coding: utf-8 -*-
#  Copyright (c) 2022, University of Luxembourg / DHARPA project
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import typing
from typing import Iterable, Optional

from kiara.utils.output import DictTabularWrap, TabularWrap
from sqlite_utils.cli import insert_upsert_implementation

from kiara_modules.core.metadata_models import KiaraDatabase, KiaraFile


def create_sqlite_table_from_file(
    target_db_file: str,
    file_item: KiaraFile,
    table_name: Optional[str] = None,
    is_csv: bool = True,
    is_tsv: bool = False,
    is_nl: bool = False,
    primary_key_column_names: Optional[Iterable[str]] = None,
    flatten_nested_json_objects: bool = False,
    csv_delimiter: str = None,
    quotechar: str = None,
    sniff: bool = True,
    no_headers: bool = False,
    encoding: str = "utf-8",
    batch_size: int = 100,
    detect_types: bool = True,
):

    if not table_name:
        table_name = file_item.file_name_without_extension

    with open(file_item.path, "rb") as f:

        insert_upsert_implementation(
            path=target_db_file,
            table=table_name,
            file=f,
            pk=primary_key_column_names,
            flatten=flatten_nested_json_objects,
            nl=is_nl,
            csv=is_csv,
            tsv=is_tsv,
            lines=False,
            text=False,
            convert=None,
            imports=None,
            delimiter=csv_delimiter,
            quotechar=quotechar,
            sniff=sniff,
            no_headers=no_headers,
            encoding=encoding,
            batch_size=batch_size,
            alter=False,
            upsert=False,
            ignore=False,
            replace=False,
            truncate=False,
            not_null=None,
            default=None,
            detect_types=detect_types,
            analyze=False,
            load_extension=None,
            silent=False,
            bulk_sql=None,
        )


class SqliteTabularWrap(TabularWrap):
    def __init__(self, db: "KiaraDatabase", table_name: str):
        self._db: KiaraDatabase = db
        self._table_name: str = table_name
        super().__init__()

    def retrieve_number_of_rows(self) -> int:

        from sqlalchemy import text

        with self._db.get_sqlalchemy_engine().connect() as con:
            result = con.execute(text(f"SELECT count(*) from {self._table_name}"))
            num_rows = result.fetchone()[0]

        return num_rows

    def retrieve_column_names(self) -> typing.Iterable[str]:

        from sqlalchemy import inspect

        engine = self._db.get_sqlalchemy_engine()
        inspector = inspect(engine)
        columns = inspector.get_columns(self._table_name)
        result = [column["name"] for column in columns]
        return result

    def slice(
        self, offset: int = 0, length: typing.Optional[int] = None
    ) -> "TabularWrap":

        from sqlalchemy import text

        query = f"SELECT * FROM {self._table_name}"
        if length:
            query = f"{query} LIMIT {length}"
        else:
            query = f"{query} LIMIT {self.num_rows}"
        if offset > 0:
            query = f"{query} OFFSET {offset}"
        with self._db.get_sqlalchemy_engine().connect() as con:
            result = con.execute(text(query))
            result_dict: typing.Dict[str, typing.List[typing.Any]] = {}
            for cn in self.column_names:
                result_dict[cn] = []
            for r in result:
                for i, cn in enumerate(self.column_names):
                    result_dict[cn].append(r[i])

        return DictTabularWrap(result_dict)

    def to_pydict(self) -> typing.Mapping:

        from sqlalchemy import text

        query = f"SELECT * FROM {self._table_name}"

        with self._db.get_sqlalchemy_engine().connect() as con:
            result = con.execute(text(query))
            result_dict: typing.Dict[str, typing.List[typing.Any]] = {}
            for cn in self.column_names:
                result_dict[cn] = []
            for r in result:
                for i, cn in enumerate(self.column_names):
                    result_dict[cn].append(r[i])

        return result_dict
