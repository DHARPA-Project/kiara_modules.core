# -*- coding: utf-8 -*-
import typing

import pyarrow
import pyarrow as pa
from kiara import KiaraModule
from kiara.data import ValueSet
from kiara.data.values import ValueSchema
from kiara.module_config import KiaraModuleConfig


class TableFilterModuleConfig(KiaraModuleConfig):

    pass


class CreateFilteredTableModule(KiaraModule):
    """Filter a table using a mask array."""

    _module_type_name = "with_mask"

    _config_cls = TableFilterModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        inputs = {
            "table": {"type": "table", "doc": "The table to filter."},
            "mask": {
                "type": "array",
                "doc": "An mask array of booleans of the same length as the table.",
            },
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs = {"table": {"type": "table", "doc": "The filtered table."}}
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        input_table: pa.Table = inputs.get_value_data("table")
        filter_array: pa.Array = inputs.get_value_data("mask")

        filtered = input_table.filter(filter_array)

        outputs.set_value("table", filtered)
