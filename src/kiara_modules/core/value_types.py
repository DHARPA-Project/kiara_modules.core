# -*- coding: utf-8 -*-

"""This module contains the value type classes that are used in the ``kiara_modules.core`` package.
"""

import datetime
import typing

import pyarrow as pa
from dateutil import parser
from kiara.data.types import ValueHashMarker, ValueType


class TableType(ValueType):
    @classmethod
    def transformation_configs(
        self,
    ) -> typing.Optional[typing.Mapping[str, typing.Mapping[str, typing.Any]]]:
        """Return a dictionary of configuration for modules that can transform this type.

        The name of the transformation is the key of the result dictionary, the configuration is a module configuration
        (dictionary wth 'module_type' and optional 'module_config', 'input_name' and 'output_name' keys).
        """
        return {
            "string": {
                "module_type": "string.pretty_print",
                "input_name": "item",
                "defaults": {"max_cell_length": 240, "max_no_rows": 20},
            },
            "json": {"module_type": "json.to_json", "input_name": "item"},
        }

    @classmethod
    def python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [pa.Table]

    @classmethod
    def save_config(cls) -> typing.Mapping[str, typing.Any]:

        return {
            "module_type": "table.save",
            "module_config": {
                "constants": {
                    "format": "feather",
                    "force_overwrite": False,
                    "file_name": "table.feather",
                }
            },
            "input_name": "table",
            "target_name": "folder_path",
            "load_config_output": "load_config",
        }

    @classmethod
    def check_data(cls, data: typing.Any) -> typing.Optional["ValueType"]:

        if isinstance(data, pa.Table):
            return TableType()

        return None

    def validate(cls, value: typing.Any) -> None:
        assert isinstance(value, pa.Table)


class ArrayType(ValueType):
    """An Apache arrow array."""

    @classmethod
    def save_config(cls) -> typing.Optional[typing.Mapping[str, typing.Any]]:

        return {
            "module_type": "array.save",
            "module_config": {
                "constants": {"column_name": "array", "file_name": "array.feather"}
            },
            "input_name": "array",
            "target_name": "folder_path",
            "load_config_output": "load_config",
        }

    def extract_type_metadata(
        cls, value: typing.Any
    ) -> typing.Mapping[str, typing.Any]:

        metadata = {
            "item_type": str(value.type),
            "arrow_type_id": value.type.id,
            "length": len(value),
        }
        return metadata


class DateType(ValueType):
    def defer_hash_calc(self) -> bool:
        return False

    def calculate_value_hash(
        self, value: typing.Any
    ) -> typing.Union[int, ValueHashMarker]:

        return hash(value)

    def parse_value(self, v: typing.Any) -> typing.Any:

        if isinstance(v, str):
            d = parser.parse(v)
            return d

        return None

    def validate(cls, value: typing.Any):
        assert isinstance(value, datetime.datetime)


class FileType(ValueType):
    pass


class FileBundleType(ValueType):
    @classmethod
    def python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [FileBundleType]

    @classmethod
    def save_config(cls) -> typing.Optional[typing.Mapping[str, typing.Any]]:

        return {
            "module_type": "onboarding.file_bundle.save",
            "input_name": "files",
            "target_name": "target",
            "load_config_output": "load_config",
        }
