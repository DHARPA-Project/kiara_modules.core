# -*- coding: utf-8 -*-
import os
import typing

from kiara import KiaraModule
from kiara.data.values import Value, ValueSchema
from kiara.data.values.value_set import ValueSet
from kiara.exceptions import KiaraProcessingException
from kiara.operations.store_value import StoreValueTypeModule

KIARA_METADATA = {"tags": ["bytes", "serialization"]}

BYTES_SAVE_FILE_NAME = "bytes.bin"


class StoreBytesTypeModule(StoreValueTypeModule):

    _module_type_name = "store"

    @classmethod
    def retrieve_supported_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        return "bytes"

    def store_value(self, value: Value, base_path: str) -> typing.Dict[str, typing.Any]:

        path = os.path.join(base_path, BYTES_SAVE_FILE_NAME)

        if os.path.exists(path):
            raise KiaraProcessingException(
                f"Can't write bytes, target path already exists: {path}"
            )

        os.makedirs(os.path.dirname(path), exist_ok=True)

        bytes = value.get_value_data()

        with open(path, "wb") as f:
            f.write(bytes)

        load_config = {
            "module_type": "bytes.load",
            "inputs": {"base_path": base_path, "rel_path": BYTES_SAVE_FILE_NAME},
            "output_name": "bytes",
        }
        return load_config


class LoadBytesModule(KiaraModule):

    _module_type_name = "load"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "base_path": {
                "type": "string",
                "doc": "The base path to the file to read.",
            },
            "rel_path": {
                "type": "string",
                "doc": "The relative path of the file, within the base path.",
            },
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {"bytes": {"type": "bytes", "doc": "The content of the file."}}

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        path = inputs.get_value_data("path")

        if not os.path.exists(path):
            raise KiaraProcessingException(
                f"Can't read file, path does not exist: {path}"
            )

        with open(path, "rb") as f:
            content = f.read()

        outputs.set_value("bytes", content)
