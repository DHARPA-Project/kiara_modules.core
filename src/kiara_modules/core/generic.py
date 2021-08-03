# -*- coding: utf-8 -*-
import logging
import typing

from dataprofiler import Data
from kiara import Kiara, KiaraModule
from kiara.data.values import ValueSchema, ValueSet
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import ModuleTypeConfig
from kiara.operations import OperationConfig
from pydantic import Field

from kiara_modules.core.metadata_schemas import FileMetadata


class DataProfilerModuleConfig(ModuleTypeConfig):

    value_type: str = Field(description="The value type to profile.")


class DataProfilerModule(KiaraModule):
    """Generate a data profile report for a dataset.

    This uses the [DataProfiler](https://capitalone.github.io/DataProfiler/docs/0.7.0/html/index.html) Python library,
    check out its documentation for more details.
    """

    _module_type_name = "data_profile"
    _config_cls = DataProfilerModuleConfig

    @classmethod
    def retrieve_module_profiles(
        cls, kiara: "Kiara"
    ) -> typing.Mapping[
        str, typing.Union[typing.Mapping[str, typing.Any], OperationConfig]
    ]:

        supported_source_types = ["table", "file"]

        all_profiles = {}
        for sup_type in supported_source_types:

            op_config = {
                "module_type": cls._module_type_id,  # type: ignore
                "module_config": {"value_type": sup_type},
            }
            all_profiles[f"{sup_type}.data_profile"] = op_config

        return all_profiles

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Mapping[str, typing.Mapping[str, typing.Any]] = {
            "item": {
                "type": self.get_config_value("value_type"),
                "doc": f"The {self.get_config_value('value_type')} to profile.",
            }
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs: typing.Mapping[str, typing.Mapping[str, typing.Any]] = {
            "report": {"type": "dict", "doc": "Statistics/details about the dataset."}
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        import pyarrow as pa
        from dataprofiler import Profiler, ProfilerOptions, set_verbosity

        set_verbosity(logging.WARNING)

        value_type = self.get_config_value("value_type")

        profile_options = ProfilerOptions()
        profile_options.structured_options.data_labeler.is_enabled = False
        profile_options.unstructured_options.data_labeler.is_enabled = False

        if value_type == "table":
            table_item: pa.Table = inputs.get_value_data("item")
            pd = table_item.to_pandas()
            profile = Profiler(
                pd, options=profile_options
            )  # Calculate Statistics, Entity Recognition, etc
            report = profile.report()

        elif value_type == "file":
            file_item: FileMetadata = inputs.get_value_data("item")
            data = Data(file_item.path)
            profile = Profiler(data, options=profile_options)
            report = profile.report()
        else:
            raise KiaraProcessingException(
                f"Data profiling of value type '{value_type}' not supported."
            )

        outputs.set_value("report", report)
