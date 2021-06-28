# -*- coding: utf-8 -*-
import os
import typing

import pyarrow as pa
from kiara import KiaraModule
from kiara.data.values import Value, ValueSchema, ValueSet
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import KiaraModuleConfig
from kiara.modules.metadata import ExtractMetadataModule
from pyarrow import feather
from pydantic import BaseModel, Field

from kiara_modules.core.array.utils import map_with_module
from kiara_modules.core.metadata_schemas import ArrayMetadata

KIARA_METADATA = {
    "description": "Array-related kiara modules",
    "tags": ["array"],
}


class SaveArrayModule(KiaraModule):
    """Save an Arrow array to a file.

    This module wraps the input array into an Arrow Table, and saves this table as a feather file.

    The output of this module is a dictionary representing the configuration to be used with *kira* to re-assemble
    the array object from disk.
    """

    _module_type_name = "save"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        inputs: typing.Mapping[str, typing.Any] = {
            "array": {"type": "array", "doc": "The array to save."},
            "column_name": {
                "type": "string",
                "doc": "The name of the column in the wrapping table.",
                "default": "array",
            },
            "folder_path": {
                "type": "string",
                "doc": "The base folder for storing the array.",
            },
            "file_name": {
                "type": "string",
                "doc": "The name of the file.",
                "default": "array.feather",
            },
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs: typing.Mapping[str, typing.Any] = {
            "load_config": {
                "type": "load_config",
                "doc": "The load config for the array.",
            }
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        array: pa.Array = inputs.get_value_data("array")
        folder = inputs.get_value_data("folder_path")
        file_name = inputs.get_value_data("file_name")
        column_name = inputs.get_value_data("column_name")

        if not column_name:
            raise KiaraProcessingException(
                "Can't save array, column name not provided."
            )

        path = os.path.join(folder, file_name)
        if os.path.exists(path):
            raise KiaraProcessingException(
                f"Can't write file, path already exists: {path}"
            )

        os.makedirs(os.path.dirname(path))

        table = pa.Table.from_arrays([array], names=[column_name])
        feather.write_feather(table, path)

        load_config = {
            "module_type": "array.load_array_from_table_file",
            "inputs": {"path": path, "format": "feather", "column_name": column_name},
            "output_name": "array",
        }
        outputs.set_value("load_config", load_config)


class MapModuleConfig(KiaraModuleConfig):

    module_type: str = Field(
        description="The name of the kiara module to use to filter the input data."
    )
    module_config: typing.Optional[typing.Dict[str, typing.Any]] = Field(
        description="The config for the kiara filter module.", default_factory=dict
    )
    input_name: typing.Optional[str] = Field(
        description="The name of the input name of the module which will receive the items from our input array. Can be omitted if the configured module only has a single input.",
        default=None,
    )
    output_name: typing.Optional[str] = Field(
        description="The name of the output name of the module which will receive the items from our input array. Can be omitted if the configured module only has a single output.",
        default=None,
    )


class MapModule(KiaraModule):
    """Map a list of values into another list of values.

    This module must be configured with the type (and optional) configuration of another *kiara* module. This 'child'
    module will then be used to compute the array items of the result.
    """

    _config_cls = MapModuleConfig

    def module_instance_doc(self) -> str:

        config: MapModuleConfig = self.config  # type: ignore

        module_type = config.module_type
        module_config = config.module_config

        m = self._kiara.create_module(
            module_type=module_type, module_config=module_config
        )
        type_md = m.get_type_metadata()
        doc = type_md.documentation.full_doc
        link = type_md.context.get_url_for_reference("module_doc")
        if not link:
            link_str = f"``{module_type}``"
        else:
            link_str = f"[``{module_type}``]({link})"

        result = f"""Map the values of the input list onto a new list of the same length, using the {link_str} module."""

        if doc and doc != "-- n/a --":
            result = result + f"\n\n``{module_type}`` documentation:\n\n{doc}"
        return result

    def __init__(self, *args, **kwargs):

        self._child_module: typing.Optional[KiaraModule] = None
        self._module_input_name: typing.Optional[str] = None
        self._module_output_name: typing.Optional[str] = None
        super().__init__(*args, **kwargs)

    @property
    def child_module(self) -> KiaraModule:

        if self._child_module is not None:
            return self._child_module

        module_name = self.get_config_value("module_type")
        module_config = self.get_config_value("module_config")
        self._child_module = self._kiara.create_module(
            id="map_module_child", module_type=module_name, module_config=module_config
        )
        return self._child_module

    @property
    def module_input_name(self) -> str:

        if self._module_input_name is not None:
            return self._module_input_name

        self._module_input_name = self.get_config_value("input_name")
        if self._module_input_name is None:
            if len(list(self.child_module.input_names)) == 1:
                self._module_input_name = next(iter(self.child_module.input_names))
            else:
                raise KiaraProcessingException(
                    f"No 'input_name' specified, and configured module has more than one inputs. Please specify an 'input_name' value in your module config, pick one of: {', '.join(self.child_module.input_names)}"
                )

        return self._module_input_name

    @property
    def module_output_name(self) -> str:

        if self._module_output_name is not None:
            return self._module_output_name

        self._module_output_name = self.get_config_value("output_name")
        if self._module_output_name is None:
            if len(list(self.child_module.output_names)) == 1:
                self._module_output_name = next(iter(self.child_module.output_names))
            else:
                raise KiaraProcessingException(
                    f"No 'output_name' specified, and configured module has more than one outputs. Please specify an 'output_name' value in your module config, pick one of: {', '.join(self.child_module.output_names)}"
                )

        return self._module_output_name

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Dict[
            str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
        ] = {
            "array": {
                "type": "array",
                "doc": "The array containing the values the filter is applied on.",
            }
        }
        for input_name, schema in self.child_module.input_schemas.items():
            assert input_name != "array"
            if input_name == self.module_input_name:
                continue
            inputs[input_name] = schema
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs = {
            "array": {
                "type": "array",
                "doc": "An array of equal length to the input array, containing the 'mapped' values.",
            }
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        input_array: pa.Array = inputs.get_value_data("array")

        init_data: typing.Dict[str, typing.Any] = {}
        for input_name in self.input_schemas.keys():
            if input_name in ["array", self.module_input_name]:
                continue

            init_data[input_name] = inputs.get_value_obj(input_name)

        result_list = map_with_module(
            input_array,
            module_input_name=self.module_input_name,
            module_obj=self.child_module,
            init_data=init_data,
            module_output_name=self.module_output_name,
        )
        outputs.set_value("array", pa.array(result_list))


class ArrayMetadataModule(ExtractMetadataModule):
    """Extract metadata from an 'array' value."""

    _module_type_name = "metadata"

    @classmethod
    def _get_supported_types(cls) -> str:
        return "array"

    @classmethod
    def get_metadata_key(cls) -> str:
        return "array"

    def _get_metadata_schema(
        self, type: str
    ) -> typing.Union[str, typing.Type[BaseModel]]:
        return ArrayMetadata

    def extract_metadata(self, value: Value) -> typing.Mapping[str, typing.Any]:

        array: pa.Array = value.get_value_data()

        return {
            "length": len(array),
            "size": array.nbytes,
        }
