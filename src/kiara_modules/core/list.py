# -*- coding: utf-8 -*-
import typing
from pathlib import Path

from kiara import KiaraModule
from kiara.data import ValueSet
from kiara.data.values import Value, ValueSchema
from kiara.operations.store_value import StoreValueTypeModule

from kiara_modules.core.generic import JsonSerializationConfig


class StoreDictModule(StoreValueTypeModule):

    _config_cls = JsonSerializationConfig
    _module_type_name = "store"

    @classmethod
    def retrieve_supported_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        return "list"

    def store_value(self, value: Value, base_path: str) -> typing.Dict[str, typing.Any]:

        import orjson

        options = self.get_config_value("options")
        file_name = self.get_config_value("file_name")
        json_str = orjson.dumps(value.get_value_data(), option=options)

        bp = Path(base_path)
        bp.mkdir(parents=True, exist_ok=True)

        full_path = bp / file_name
        full_path.write_bytes(json_str)

        load_config = {
            "module_type": "generic.restore_from_json",
            "base_path_input_name": "base_path",
            "inputs": {
                "base_path": base_path,
                "file_name": self.get_config_value("file_name"),
            },
            "output_name": "value_item",
        }

        return load_config


class IncludedInListCheckModule(KiaraModule):
    """Check whether an element is in a list."""

    _module_type_name = "contains"

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
