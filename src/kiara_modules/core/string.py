# -*- coding: utf-8 -*-
import re
import typing
from abc import abstractmethod
from pprint import pformat

from kiara import KiaraModule
from kiara.data.values import Value, ValueSchema, ValueSet
from kiara.defaults import DEFAULT_NO_DESC_VALUE
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import ModuleTypeConfig
from kiara.utils import StringYAML
from kiara.utils.output import pretty_print_arrow_table
from pydantic import Field
from rich import box
from rich.console import RenderableType, RenderGroup
from rich.markdown import Markdown
from rich.panel import Panel


def convert_to_renderable(
    value_type: str, data: typing.Any, render_config: typing.Mapping[str, typing.Any]
) -> typing.List[RenderableType]:

    if value_type == "table":

        max_rows = render_config.get("max_no_rows")
        max_row_height = render_config.get("max_row_height")
        max_cell_length = render_config.get("max_cell_length")

        half_lines: typing.Optional[int] = None
        if max_rows:
            half_lines = int(max_rows / 2)

        result = [
            pretty_print_arrow_table(
                data,
                rows_head=half_lines,
                rows_tail=half_lines,
                max_row_height=max_row_height,
                max_cell_length=max_cell_length,
            )
        ]
    elif value_type == "value_set":
        value: Value
        result_dict = {}
        for field_name, value in data.items():
            _vt = value.value_schema.type
            _data = value.get_value_data()
            if value.value_schema.doc == DEFAULT_NO_DESC_VALUE:
                _temp: typing.List[RenderableType] = []
            else:
                md = Markdown(value.value_schema.doc)
                _temp = [Panel(md, box=box.SIMPLE)]
            _strings = convert_to_renderable(
                value_type=_vt, data=_data, render_config=render_config
            )
            _temp.extend(_strings)
            result_dict[(field_name, _vt)] = _temp

        result = []
        for k, v in result_dict.items():
            result.append(
                Panel(
                    RenderGroup(*v),
                    title=f"field: [b]{k[0]}[/b] (type: [i]{k[1]}[/i])",
                    title_align="left",
                )
            )

    else:
        result = [pformat(data)]

    return result


def convert_to_string(
    value_type: str, data: typing.Any, render_config: typing.Mapping[str, typing.Any]
) -> str:

    if value_type == "dict":
        yaml = StringYAML()
        result_string = yaml.dump(data)
    if value_type == "value_set":
        value: Value
        result = {}
        for field_name, value in data.items():
            _vt = value.value_schema.type
            _data = value.get_value_data()
            _string = convert_to_string(
                value_type=_vt, data=_data, render_config=render_config
            )
            result[field_name] = _string

        result_string = convert_to_string(
            value_type="dict", data=result, render_config=render_config
        )
    else:
        result_string = pformat(data)

    return result_string


class StringManipulationModule(KiaraModule):
    """Base module to simplify creating other modules that do string manipulation."""

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


class RegexModuleConfig(ModuleTypeConfig):

    regex: str = Field(description="The regex to apply.")
    only_first_match: bool = Field(
        description="Whether to only return the first match, or all matches.",
        default=False,
    )


class RegexModule(KiaraModule):
    """Match a string using a regular expression."""

    _config_cls = RegexModuleConfig
    _module_type_name = "match_regex"

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


class ReplaceModuleConfig(ModuleTypeConfig):

    replacement_map: typing.Dict[str, str] = Field(
        description="A map, containing the strings to be replaced as keys, and the replacements as values."
    )
    default_value: typing.Optional[str] = Field(
        description="The default value to use if the string to be replaced is not in the replacement map. By default, this just returns the string itself.",
        default=None,
    )


class ReplaceStringModule(KiaraModule):
    """Replace a string if it matches a key in a mapping dictionary."""

    _config_cls = ReplaceModuleConfig
    _module_type_name = "replace"

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
