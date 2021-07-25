# -*- coding: utf-8 -*-
import typing

import msgpack
import pyarrow as pa
from kiara import KiaraModule
from kiara.data.values import Value, ValueSchema, ValueSet
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import KiaraModuleConfig
from pyarrow import Buffer, Table
from pydantic import Field

KIARA_METADATA = {"tags": ["msgpack"]}


class SerializeToMsgPackModuleConfig(KiaraModuleConfig):

    type_name: str = Field(description="The value type to serialize/deserialize.")


class SerializeToMsgPackModule(KiaraModule):

    _module_type_name = "from_value"
    _config_cls = SerializeToMsgPackModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "value_item": {
                "type": self.config.get("type_name"),
                "doc": f"A {self.get_config_value('type_name')} value.",
            }
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "bytes": {
                "type": "bytes",
                "doc": f"The msgpack-serialized {self.get_config_value('type_name')} value.",
            }
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        type_name: str = self.get_config_value("type_name")

        if not hasattr(self, f"from_{type_name}"):
            raise KiaraProcessingException(
                f"Value type not supported for msgpack serialization: {type_name}"
            )

        func = getattr(self, f"from_{type_name}")

        value = inputs.get_value_obj("value_item")

        metadata = value.get_metadata(also_return_schema=True)

        msg = func(value=value)
        data = {"value_type": value.type_name, "metadata": metadata, "data": msg}

        msg = msgpack.packb(data, use_bin_type=True)

        outputs.set_value("bytes", msg)

    def from_table(self, value: Value) -> bytes:

        table_val: Value = value
        table: Table = table_val.get_value_data()

        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, table.schema)

        writer.write(table)
        writer.close()

        buf: Buffer = sink.getvalue()
        return memoryview(buf)

    def from_boolean(self, value: Value) -> bytes:

        return value.get_value_data()


class DeserializeFromMsgPackModule(KiaraModule):

    _module_type_name = "to_value"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {"bytes": {"type": "bytes", "doc": "The msgpack-serialized value."}}

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "value_type": {"type": "string", "doc": "The type of the value."},
            "value_data": {"type": "any", "doc": f"The {self.type_name} value."},
            "value_metadata": {
                "type": "dict",
                "doc": "A dictionary with metadata of the serialized table. The result dict has the metadata key as key, and two sub-values under each key: 'metadata_item' (the actual metadata) and 'metadata_item_schema' (the schema for the metadata).",
            },
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        msg = inputs.get_value_data("bytes")

        unpacked = msgpack.unpackb(msg, raw=False)

        value_type = unpacked["value_type"]
        outputs.set_value("value_type", value_type)

        metadata = unpacked["metadata"]
        outputs.set_value("value_metadata", metadata)

        new_data = unpacked["data"]

        if not hasattr(self, f"to_{value_type}"):
            raise KiaraProcessingException(
                f"Value type not supported for msgpack deserialization: {value_type}"
            )

        func = getattr(self, f"to_{value_type}")
        obj = func(data=new_data)
        outputs.set_value("value_data", obj)

    def to_table(self, data: bytes) -> typing.Any:

        reader = pa.ipc.open_stream(data)

        batches = [b for b in reader]
        new_table = Table.from_batches(batches)

        return new_table

    def to_boolean(self, data: bytes):

        return data
