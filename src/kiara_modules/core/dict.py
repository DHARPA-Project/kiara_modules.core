# -*- coding: utf-8 -*-
import typing

from kiara.data import Value
from kiara.operations.pretty_print import PrettyPrintValueModule
from rich.pretty import Pretty


class PrettyPrintDictModule(PrettyPrintValueModule):

    _module_type_name = "pretty_print"

    @classmethod
    def retrieve_supported_source_types(cls) -> typing.Union[str, typing.Iterable[str]]:

        return ["dict"]

    @classmethod
    def retrieve_supported_target_types(cls) -> typing.Union[str, typing.Iterable[str]]:

        return ["renderables"]

    def pretty_print(
        self,
        value: Value,
        value_type: str,
        target_type: str,
        print_config: typing.Mapping[str, typing.Any],
    ) -> typing.Dict[str, typing.Any]:

        result = None
        if value_type == "dict":
            if target_type == "renderables":
                result = self.pretty_print_table_as_renderables(
                    value=value, print_config=print_config
                )

        if result is None:
            raise Exception(
                f"Pretty printing of type '{value_type}' as '{target_type}' not supported."
            )
        return result

    def pretty_print_table_as_renderables(
        self, value: Value, print_config: typing.Mapping[str, typing.Any]
    ):

        # max_rows = print_config.get("max_no_rows")
        # max_row_height = print_config.get("max_row_height")
        # max_cell_length = print_config.get("max_cell_length")
        #
        # half_lines: typing.Optional[int] = None
        # if max_rows:
        #     half_lines = int(max_rows / 2)

        result = Pretty(value.get_value_data())

        return [result]
