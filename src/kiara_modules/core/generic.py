# -*- coding: utf-8 -*-
import os
import typing

import orjson
from kiara import KiaraModule
from kiara.data import ValueSet
from kiara.data.values import Value, ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import ModuleTypeConfigSchema
from kiara.operations.store_value import StoreValueModuleConfig, StoreValueTypeModule
from pydantic import Field


class JsonSerializationConfig(StoreValueModuleConfig):

    options: int = Field(
        description="The options to use for the json serialization. Check https://github.com/ijl/orjson#quickstart for details.",
        default=orjson.OPT_NAIVE_UTC | orjson.OPT_SERIALIZE_NUMPY,
    )
    file_name: str = Field(
        description="The name of the serialized file.", default="dict.json"
    )


class StoreScalarModuleConfig(ModuleTypeConfigSchema):

    value_type: str = Field(description="The value type of the scalar to store.")


class StoreScalarModule(StoreValueTypeModule):

    _module_type_name = "store"

    @classmethod
    def retrieve_supported_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        return ["boolean", "integer", "float", "string"]

    def store_value(self, value: Value, base_path: str) -> typing.Dict[str, typing.Any]:

        data = value.get_value_data()

        load_config = {
            "module_type": "generic.restore_scalar",
            "module_config": {"value_type": self.get_config_value("value_type")},
            "base_path_input_name": None,
            "inputs": {"scalar_data": data},
            "output_name": "value_item",
        }

        return load_config


class RestoreScalarModuleConfig(ModuleTypeConfigSchema):

    value_type: str = Field(description="The value type of the scalar to load.")


class RestoreScalarModule(KiaraModule):
    """Utility module, only used internally."""

    _module_type_name = "restore_scalar"
    _config_cls = RestoreScalarModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "scalar_data": {
                "type": self.get_config_value("value_type"),
                "doc": "The scalar value.",
            }
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "value_item": {
                "type": self.get_config_value("value_type"),
                "doc": "The loaded item.",
            }
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        data = inputs.get_value_obj("scalar_data")
        outputs.set_value("value_item", data)


class RestoreFromJsonDictModule(KiaraModule):

    _module_type_name = "restore_from_json"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "base_path": {
                "type": "string",
                "doc": "The folder that contains the serialized dict.",
            },
            "file_name": {
                "type": "string",
                "doc": "The file name of the serialized dict.",
            },
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {"value_item": {"type": "dict", "doc": "The deserialized dict value."}}

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        base_path = inputs.get_value_data("base_path")
        file_name = inputs.get_value_data("file_name")

        full_path = os.path.join(base_path, file_name)

        if not os.path.exists(full_path):
            raise KiaraProcessingException(
                f"Can't deserialize dict, path to file does not exist: {full_path}"
            )

        if not os.path.isfile(os.path.realpath(full_path)):
            raise KiaraProcessingException(
                f"Can't deserialize dict, path is not a file: {full_path}"
            )

        with open(full_path, "r") as f:
            content = f.read()

        data = orjson.loads(content)
        outputs.set_value("value_item", data)
