# -*- coding: utf-8 -*-
import time
import typing

from kiara.data.values import ValueSchema, ValueSet
from kiara.module import KiaraModule
from kiara.module_config import ModuleTypeConfigSchema
from pydantic import Field


class LogicProcessingModuleConfig(ModuleTypeConfigSchema):
    """Config class for all the 'logic'-related modules."""

    delay: float = Field(
        default=0,
        description="the delay in seconds from processing start to when the output is returned.",
    )


class LogicProcessingModule(KiaraModule):

    _config_cls = LogicProcessingModuleConfig


class NotModule(LogicProcessingModule):
    """Negates the input."""

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        """The not module only has one input, a boolean that will be negated by the module."""

        return {
            "a": {"type": "boolean", "doc": "A boolean describing this input state."}
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        """The output of this module is a single boolean, the negated input."""

        return {
            "y": {
                "type": "boolean",
                "doc": "A boolean describing the module output state.",
            }
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:
        """Negates the input boolean."""

        time.sleep(self.config.get("delay"))  # type: ignore

        outputs.set_value("y", not inputs.get_value_data("a"))


class AndModule(LogicProcessingModule):
    """Returns 'True' if both inputs are 'True'."""

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "a": {"type": "boolean", "doc": "A boolean describing this input state."},
            "b": {"type": "boolean", "doc": "A boolean describing this input state."},
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "y": {
                "type": "boolean",
                "doc": "A boolean describing the module output state.",
            }
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        time.sleep(self.config.delay)  # type: ignore

        outputs.set_value(
            "y", inputs.get_value_data("a") and inputs.get_value_data("b")
        )


class OrModule(LogicProcessingModule):
    """Returns 'True' if one of the inputs is 'True'."""

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "a": {"type": "boolean", "doc": "A boolean describing this input state."},
            "b": {"type": "boolean", "doc": "A boolean describing this input state."},
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "y": {
                "type": "boolean",
                "doc": "A boolean describing the module output state.",
            }
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        time.sleep(self.config.get("delay"))  # type: ignore
        outputs.set_value("y", inputs.get_value_data("a") or inputs.get_value_data("b"))
