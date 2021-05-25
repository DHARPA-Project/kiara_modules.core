# -*- coding: utf-8 -*-
import re
import typing
from abc import abstractmethod
from pprint import pformat

from kiara import KiaraModule
from kiara.data.values import Value, ValueSchema, ValueSet
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import KiaraModuleConfig
from kiara.utils.pretty_print import pretty_print_arrow_table
from pydantic import Field


class StringManipulationModule(KiaraModule):
    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {"text": {"type": "string", "doc": "The input string."}}

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {"text": {"type": "string", "doc": "The processed string."}}

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        input_string = inputs.get_value_data("text")
        result = self.process_string(input_string)
        outputs.set_value("text", result)

    @abstractmethod
    def process_string(self, text: str) -> str:
        pass


class RegexModuleConfig(KiaraModuleConfig):

    regex: str = Field(description="The regex to apply.")
    only_first_match: bool = Field(
        description="Whether to only return the first match, or all matches.",
        default=False,
    )


class RegexModule(KiaraModule):
    """Check whether the input string matches a provided regular expression."""

    _config_cls = RegexModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {"text": {"type": "string", "doc": "The text to match."}}

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        if self.get_config_value("only_first_match"):
            output_schema = {"text": {"type": "string", "doc": "The first match."}}
        else:
            raise NotImplementedError()

        return output_schema

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        text = inputs.get_value_data("text")
        regex = self.get_config_value("regex")
        matches = re.findall(regex, text)

        if not matches:
            raise KiaraProcessingException(f"No match for regex: {regex}")

        if self.get_config_value("only_first_match"):
            result = matches[0]
        else:
            result = matches

        outputs.set_value("text", result)


class ReplaceModuleConfig(KiaraModuleConfig):

    replacement_map: typing.Dict[str, str] = Field(
        description="A map, containing the strings to be replaced as keys, and the replacements as values."
    )
    default_value: typing.Optional[str] = Field(
        description="The default value to use if the string to be replaced is not in the replacement map. By default, this just returns the string itself.",
        default=None,
    )


class ReplaceStringModule(KiaraModule):

    _config_cls = ReplaceModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {"text": {"type": "string", "doc": "The input string."}}

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {"text": {"type": "string", "doc": "The replaced string."}}

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        text = inputs.get_value_data("text")
        repl_map = self.get_config_value("replacement_map")
        default = self.get_config_value("default_value")

        if text not in repl_map.keys():
            if default is None:
                result = text
            else:
                result = default
        else:
            result = repl_map[text]

        outputs.set_value("text", result)


class PrettyPrintModule(KiaraModule):
    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Dict[str, typing.Dict[str, typing.Any]] = {
            "item": {
                "type": "any",
                "doc": "The object to convert into a pretty string.",
            },
            "max_lines": {
                "type": "integer",
                "doc": "Maximum number of lines the output should have.",
                "optional": True,
            },
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs = {
            "pretty_string": {
                "type": "string",
                "doc": "Pretty string output for the input object.",
            }
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        value_type = inputs.get_value_obj("item").type_name
        input_value: Value = inputs.get_value_data("item")

        max_lines = inputs.get_value_data("max_lines")

        if value_type == "table":

            half_lines: typing.Optional[int] = None
            if max_lines:
                half_lines = int(max_lines / 2)

            input_value_str = pretty_print_arrow_table(
                input_value, num_head=half_lines, num_tail=half_lines
            )
        else:
            input_value_str = pformat(input_value)

        outputs.set_value("pretty_string", input_value_str)
