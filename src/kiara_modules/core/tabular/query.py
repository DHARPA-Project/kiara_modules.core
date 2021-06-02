# -*- coding: utf-8 -*-
import typing

import vaex
from kiara import KiaraModule
from kiara.data.values import ValueSchema, ValueSet


class QueryTableGraphQL(KiaraModule):
    """Execute a graphql aggregation queries against an arrow table.

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
            "query": {"type": "string", "doc": "The query.", "optional": True},
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

        table = inputs.get_value_data("table")
        query = inputs.get_value_data("query")

        df = vaex.from_arrow_table(table)

        result = df.graphql.execute(query)
        outputs.set_value("query_result", result.to_dict()["data"])
