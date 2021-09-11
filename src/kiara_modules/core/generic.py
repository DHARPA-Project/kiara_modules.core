# -*- coding: utf-8 -*-
import os
import typing

import orjson
from kiara import KiaraModule
from kiara.data import ValueSet
from kiara.data.values import ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.operations.save_value import SaveValueModuleConfig
from pydantic import Field


class JsonSerializationConfig(SaveValueModuleConfig):

    options: int = Field(
        description="The options to use for the json serialization. Check https://github.com/ijl/orjson#quickstart for details.",
        default=orjson.OPT_NAIVE_UTC | orjson.OPT_SERIALIZE_NUMPY,
    )
    file_name: str = Field(
        description="The name of the serialized file.", default="dict.json"
    )


class LoadFromJsonDictModule(KiaraModule):

    _module_type_name = "load_from_json"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "base_path": {
                "type": "folder_path",
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
