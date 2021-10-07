# -*- coding: utf-8 -*-
import typing

from kiara import KiaraModule
from kiara.data import ValueSet
from kiara.data.values import ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import ModuleTypeConfigSchema
from pydantic import Field

RESERVED_SQL_KEYWORDS = [
    "ALL",
    "AND",
    "ARRAY",
    "AS",
    "BETWEEN",
    "BOTH",
    "CASE",
    "CHECK",
    "CONSTRAINT",
    "CROSS",
    "CURRENT",
    "CURRENT",
    "CURRENT",
    "CURRENT",
    "CURRENT",
    "CURRENT",
    "DISTINCT",
    "EXCEPT",
    "EXISTS",
    "FALSE",
    "FETCH",
    "FILTER",
    "FOR",
    "FOREIGN",
    "FROM",
    "FULL",
    "GROUP",
    "GROUPS",
    "HAVING",
    "IF",
    "ILIKE",
    "IN",
    "INNER",
    "INTERSECT",
    "INTERSECTS",
    "INTERVAL",
    "IS",
    "JOIN",
    "LEADING",
    "LEFT",
    "LIKE",
    "LIMIT",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "MINUS",
    "NATURAL",
    "NOT",
    "NULL",
    "OFFSET",
    "ON",
    "OR",
    "ORDER",
    "OVER",
    "PARTITION",
    "PRIMARY",
    "QUALIFY",
    "RANGE",
    "REGEXP",
    "RIGHT",
    "ROW",
    "_ROWID",
    "ROWNUM",
    "ROWS",
    "SELECT",
    "SYSDATE",
    "SYSTIME",
    "SYSTIMESTAMP",
    "TABLE",
    "TODAY",
    "TOP",
    "TRAILING",
    "TRUE",
    "UNION",
    "UNIQUE",
    "UNKNOWN",
    "USING",
    "VALUES",
    "WHERE",
    "WINDOW",
    "WITH",
]


class QueryTableGraphQL(KiaraModule):
    """Execute a graphql aggregation query against an (Arrow) table.

    References:
        - https://vaex.io/docs/example_graphql.html

    Examples:
        An example for a query could be:

        ```
            {
              df(where: {
                Language: {_eq: "German"}
              } ) {

                row(limit: 10) {
                  Label
                  City
                }
              }
            }
        ```

    """

    _module_type_name = "graphql"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Mapping[str, typing.Any] = {
            "table": {"type": "table", "doc": "The table to query."},
            "query": {"type": "string", "doc": "The query."},
        }

        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs: typing.Mapping[str, typing.Any] = {
            "query_result": {"type": "dict", "doc": "The query result."}
        }

        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        import vaex

        table = inputs.get_value_data("table")
        query = inputs.get_value_data("query")

        df = vaex.from_arrow_table(table)

        result = df.graphql.execute(query)
        outputs.set_value("query_result", result.to_dict()["data"])


class QueryTableSQLModuleConfig(ModuleTypeConfigSchema):

    query: typing.Optional[str] = Field(
        description="The query to execute. If not specified, the user will be able to provide their own.",
        default=None,
    )
    relation_name: typing.Optional[str] = Field(
        description="The name the table is referred to in the sql query. If not specified, the user will be able to provide their own.",
        default="data",
    )


class QueryTableSQL(KiaraModule):
    """Execute a sql query against an (Arrow) table."""

    _module_type_name = "sql"
    _config_cls = QueryTableSQLModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs = {
            "table": {
                "type": "table",
                "doc": "The table to query",
            }
        }

        if self.get_config_value("query") is None:
            inputs["query"] = {"type": "string", "doc": "The query."}
            inputs["relation_name"] = {
                "type": "string",
                "doc": "The name the table is referred to in the sql query.",
                "default": "data",
            }

        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {"query_result": {"type": "table", "doc": "The query result."}}

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        import duckdb

        if self.get_config_value("query") is None:
            _query: str = inputs.get_value_data("query")
            _relation_name: str = inputs.get_value_data("relation_name")
        else:
            _query = self.get_config_value("query")
            _relation_name = self.get_config_value("relation_name")

        if _relation_name.upper() in RESERVED_SQL_KEYWORDS:
            raise KiaraProcessingException(
                f"Invalid relation name '{_relation_name}': this is a reserved sql keyword, please select a different name."
            )

        _table = inputs.get_value_data("table")

        relation: duckdb.DuckDBPyRelation = duckdb.arrow(_table)
        result: duckdb.DuckDBPyResult = relation.query(_relation_name, _query)

        outputs.set_value("query_result", result.arrow())
