# -*- coding: utf-8 -*-
import typing

from kiara import KiaraModule
from kiara.data.values import ValueSchema, ValueSet


class IncludedInListCheckModule(KiaraModule):
    """Check whether an element is in a list."""

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        inputs = {
            "list": {"type": "list", "doc": "The list."},
            "item": {
                "type": "any",
                "doc": "The element to check for inclusion in the list.",
            },
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        outputs = {
            "is_included": {
                "type": "boolean",
                "doc": "Whether the element is in the list, or not.",
            }
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        item_list = inputs.get_value_data("list")
        item = inputs.get_value_data("item")

        outputs.set_value("is_included", item in item_list)
