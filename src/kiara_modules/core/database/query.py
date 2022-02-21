# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import typing

from kiara import KiaraModule
from kiara.data import ValueSet
from kiara.data.values import ValueSchema
from kiara.module_config import ModuleTypeConfigSchema
from pydantic import Field
from sqlalchemy import create_engine

from kiara_modules.core.metadata_models import KiaraDatabase


class QueryDatabaseSQLModuleConfig(ModuleTypeConfigSchema):

    query: typing.Optional[str] = Field(
        description="The query to execute. If not specified, the user will be able to provide their own.",
        default=None,
    )


class QueryTableSQL(KiaraModule):
    """Execute a sql query against an (Arrow) table."""

    _module_type_name = "sql"
    _config_cls = QueryDatabaseSQLModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs = {
            "database": {
                "type": "database",
                "doc": "The database to query",
            }
        }

        if self.get_config_value("query") is None:
            inputs["query"] = {"type": "string", "doc": "The query."}

        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {"query_result": {"type": "table", "doc": "The query result."}}

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        import pandas as pd
        import pyarrow as pa

        if self.get_config_value("query") is None:
            _query: str = inputs.get_value_data("query")
        else:
            _query = self.get_config_value("query")

        _database: KiaraDatabase = inputs.get_value_data("database")

        # can't re-use the default engine, because pandas does not support having the 'future' flag set to 'True'
        engine = create_engine(_database.db_url)
        df = pd.read_sql(_query, con=engine)
        table = pa.Table.from_pandas(df)

        outputs.set_value("query_result", table)
