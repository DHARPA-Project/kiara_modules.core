# -*- coding: utf-8 -*-
import typing

import pyarrow
from kiara.data.types import ValueType


class TableType(ValueType):
    @classmethod
    def get_type_transformation_configs(
        self,
    ) -> typing.Optional[typing.Mapping[str, typing.Mapping[str, typing.Any]]]:
        """Return a dictionary of configuration for modules that can transform this type.

        The name of the transformation is the key of the result dictionary, the configuration is a module configuration
        (dictionary wth 'module_type' and optional 'module_config', 'input_name' and 'output_name' keys).
        """
        return {
            "string": {
                "module_type": "strings.pretty_print",
                "input_name": "item",
                "defaults": {"max_cell_length": 240, "max_no_rows": 20},
            },
            "json": {"module_type": "json.to_json", "input_name": "item"},
        }

    @classmethod
    def relevant_python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [pyarrow.Table]

    @classmethod
    def check_data_type(cls, data: typing.Any) -> typing.Optional["ValueType"]:

        if isinstance(data, pyarrow.Table):
            return TableType()

        return None

    def validate(cls, value: typing.Any) -> None:
        assert isinstance(value, pyarrow.Table)

    def extract_type_metadata(
        cls, value: typing.Any
    ) -> typing.Mapping[str, typing.Any]:

        table: pyarrow.Table = value
        table_schema = {}
        for name in table.schema.names:
            field = table.schema.field(name)
            md = field.metadata
            if not md:
                md = {}
            _type = field.type
            _d = {"item_type": str(_type), "arrow_type_id": _type.id, "metadata": md}
            table_schema[name] = _d

        return {
            "column_names": table.column_names,
            "schema": table_schema,
            "rows": table.num_rows,
            "size_in_bytes": table.nbytes,
        }
