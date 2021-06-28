# -*- coding: utf-8 -*-
import typing

from kiara import KiaraModule
from kiara.data.values import ValueSchema, ValueSet
from pyarrow import Table


class DataProfilerModule(KiaraModule):
    """Generate a data profile report for a dataset."""

    _module_type_name = "data_profile"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Mapping[str, typing.Mapping[str, typing.Any]] = {
            "item": {"type": "any", "doc": "The dataset to profile."}
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

        from dataprofiler import Profiler

        item: Table = inputs.get_value_data("item")
        pd = item.to_pandas()
        profile = Profiler(pd)  # Calculate Statistics, Entity Recognition, etc
        report = profile.report()
        outputs.set_value("report", report)
