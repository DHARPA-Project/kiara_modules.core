# -*- coding: utf-8 -*-
import copy
import os
import typing
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
from kiara import KiaraModule
from kiara.data.values import Value, ValueSchema, ValueSet
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import KiaraModuleConfig
from kiara.modules.metadata import ExtractMetadataModule
from pyarrow import feather
from pydantic import BaseModel, Field


class SaveArrayModule(KiaraModule):
    """Save an Arrow array to a file.

    This will wrap the array into an Arrow Table, and save this table as a feather file.
    """

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
            "module_type": "arrays.load_array_from_table_file",
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


class MapModule(KiaraModule):
    """Map a list of values into another list of values."""

    _config_cls = MapModuleConfig

    def module_instance_doc(self) -> str:

        config: MapModuleConfig = self.config  # type: ignore

        module_type = config.module_type
        module_config = config.module_config

        m = self._kiara.create_module(
            "id", module_type=module_type, module_config=module_config
        )
        doc = m.doc()
        link = m.source_link()
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
        super().__init__(*args, **kwargs)

    @property
    def child_module(self) -> KiaraModule:

        if self._child_module is not None:
            return self._child_module

        module_name = self.get_config_value("module_type")
        module_config = self.get_config_value("module_config")
        self._child_module = self._kiara.create_module(
            "_map_module", module_name, module_config=module_config
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

        module_name = self.get_config_value("module_type")
        module_config = self.get_config_value("module_config")
        module_obj: KiaraModule = self._kiara.create_module(
            "_map_module", module_name, module_config=module_config
        )
        # TODO: validate that the selected module is appropriate
        assert len(list(module_obj.output_names)) == 1

        module_output_name = list(module_obj.output_names)[0]

        init_data: typing.Dict[str, typing.Any] = {}
        for input_name in self.input_schemas.keys():
            if input_name in ["array", self.module_input_name]:
                continue

            init_data[input_name] = inputs.get_value_obj(input_name)

        multi_threaded = False
        if multi_threaded:

            def run_module(item):
                _d = copy.copy(init_data)
                assert self._module_input_name is not None
                _d[self._module_input_name] = item
                r = module_obj.run(**_d)
                return r.get_all_value_data()

            executor = ThreadPoolExecutor()
            results: typing.Any = executor.map(run_module, input_array)
            executor.shutdown(wait=True)

        else:
            results = []
            for item in input_array:
                _d = copy.copy(init_data)
                assert self._module_input_name is not None
                _d[self._module_input_name] = item
                r = module_obj.run(**_d)
                results.append(r.get_all_value_data())

        result_list = []
        result_types = set()
        for r in results:
            r_item = r[module_output_name]  # type: ignore
            result_list.append(r_item)
            result_types.add(type(r_item))

        assert len(result_types) == 1
        outputs.set_value("array", pa.array(result_list))


class ArrayMetadata(BaseModel):

    length: int = Field(description="The number of elements the array contains.")
    size: int = Field(
        description="Total number of bytes consumed by the elements of the array."
    )


class ArrayMetadataModule(ExtractMetadataModule):
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
