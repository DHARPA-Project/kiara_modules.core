# -*- coding: utf-8 -*-
import typing

from kiara import KiaraModule
from kiara.data.values import ValueSchema, ValueSet
from kiara.module_config import KiaraModuleConfig
from pydantic import Field


class ExampleModuleConfig(KiaraModuleConfig):

    separator: str = Field(
        description="The seperator between the two strings.", default=" - "
    )


class ExampleModule(KiaraModule):
    """A very simple example module; concatenate two strings.

    The purpose of this modules is to show the main elements of a ``KiaraModule``:

    the (optional) configuration
    :    must inherit from ``KiaraModuleConfig``, and the config class must be set as the "_config_cls" attribute
         on the ``KiaraModule`` class. Configuration values can be retrieved via the ``self.get_config_value(key)``
         method

    the inputs description
    :    must return a dictionary, containing the input name(s) as keys, and another dictionary containing type information
         and documentation about the input data as value

    the outputs description
    :    must return a dictionary, containing the output name(s) as keys, and another dictionary containing type information
         and documentation about the output data as value

    the ``process`` method
    :    this is where the actual work gets done. Input data can be accessed via ``inputs.get_value_data(key)``, results
         can be set with the ``outputs.set_value(key, value)`` method

    Examples:

        This example module can be tested on the commandline with the ``kiara run`` command:

        ```
        kiara run network_analysis-example text_1="xxx" text_2="yyy"
        ```

    """

    _config_cls = ExampleModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs = {
            "text_1": {"type": "string", "doc": "The first text."},
            "text_2": {"type": "string", "doc": "The second text."},
        }

        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs = {
            "text": {
                "type": "string",
                "doc": "The concatenated text.",
            }
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        separator = self.get_config_value("separator")

        text_1 = inputs.get_value_data("text_1")
        text_2 = inputs.get_value_data("text_2")

        result = text_1 + separator + text_2
        outputs.set_value("text", result)
