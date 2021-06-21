# -*- coding: utf-8 -*-

"""This module contains the value type classes that are used in the ``kiara_modules.core`` package.
"""

import datetime
import typing

import pyarrow as pa
from dateutil import parser
from deepdiff import DeepHash
from kiara import KiaraEntryPointItem
from kiara.data.types import ValueHashMarker, ValueType, get_type_name
from kiara.utils.class_loading import find_value_types_under

value_types: KiaraEntryPointItem = (
    find_value_types_under,
    ["kiara_modules.core.value_types"],
)


class AnyType(ValueType):
    """Any type / No type information."""

    _value_type_name = "any"


class StringType(ValueType):
    """A string."""

    def defer_hash_calc(self) -> bool:
        return False

    def calculate_value_hash(
        self, value: typing.Any
    ) -> typing.Union[int, ValueHashMarker]:
        return hash(value)

    def validate(cls, value: typing.Any) -> None:

        if not isinstance(value, str):
            raise ValueError(f"Invalid type '{type(value)}': string required")


class BooleanType(ValueType):
    "A boolean."

    def defer_hash_calc(self) -> bool:
        return False

    def calculate_value_hash(
        self, value: typing.Any
    ) -> typing.Union[int, ValueHashMarker]:
        return hash(value)

    def validate(cls, value: typing.Any):
        if not isinstance(value, bool):
            # if isinstance(v, str):
            #     if v.lower() in ["true", "yes"]:
            #         v = True
            #     elif v.lower() in ["false", "no"]:
            #         v = False
            #     else:
            #         raise ValueError(f"Can't parse string into boolean: {v}")
            # else:
            raise ValueError(f"Invalid type '{type(value)}' for boolean: {value}")


class IntegerType(ValueType):
    """An integer."""

    def defer_hash_calc(self) -> bool:
        return False

    def calculate_value_hash(
        self, value: typing.Any
    ) -> typing.Union[int, ValueHashMarker]:
        return hash(value)

    def validate(cls, value: typing.Any) -> None:

        if not isinstance(value, int):
            #     if isinstance(v, str):
            #         try:
            #             v = int(v)
            #         except Exception:
            #             raise ValueError(f"Can't parse string into integer: {v}")
            # else:
            raise ValueError(f"Invalid type '{type(value)}' for integer: {value}")


class FloatType(ValueType):
    "A float."

    def defer_hash_calc(self) -> bool:
        return False

    def calculate_value_hash(
        self, value: typing.Any
    ) -> typing.Union[int, ValueHashMarker]:
        return hash(value)

    def validate(cls, value: typing.Any) -> typing.Any:

        if not isinstance(value, float):
            raise ValueError(f"Invalid type '{type(value)}' for float: {value}")


class DictType(ValueType):
    """A dict-like object."""

    def defer_hash_calc(self) -> bool:
        return True

    def calculate_value_hash(
        self, value: typing.Any
    ) -> typing.Union[int, ValueHashMarker]:

        dh = DeepHash(value)
        return dh[value]

    def validate(cls, value: typing.Any) -> None:

        if not isinstance(value, typing.Mapping):
            raise ValueError(f"Invalid type '{type(value)}', not a mapping.")

    def extract_type_metadata(
        cls, value: typing.Any
    ) -> typing.Mapping[str, typing.Any]:
        value_types = set()
        for val in value.values():
            value_types.add(get_type_name(val))
        result = {"keys": list(value.keys()), "value_types.py": list(value_types)}
        return result


class ListType(ValueType):
    """A list-like object."""

    def defer_hash_calc(self) -> bool:
        return True

    def calculate_value_hash(
        self, value: typing.Any
    ) -> typing.Union[int, ValueHashMarker]:

        dh = DeepHash(value)
        return dh[value]

    def validate(cls, value: typing.Any) -> None:

        assert isinstance(value, typing.Iterable)

    def extract_type_metadata(
        cls, value: typing.Any
    ) -> typing.Mapping[str, typing.Any]:

        metadata = {"length": len(value)}
        return metadata


class TableType(ValueType):
    """A table.

    Internally, this is backed by the [Apache Arrow](https://arrow.apache.org) [``Table``](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html) class.
    """

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
    """A date.

    Internally, this will always be represented as a Python ``datetime`` object. Iff provided as input, it can also
    be as string, in which case the [``dateutils.parser.parse``](https://dateutil.readthedocs.io/en/stable/parser.html#dateutil.parser.parse) method will be used to parse the string into a datetime object.
    """

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
    """Represents a file that was imported into the *kiara* data store.

    This means the file can be considered immutable, and it can be accessed via a *kiara* dataset id and *kiara* value metadata is available for it."""


class FileBundleType(ValueType):
    """Represents a set of files that were imported into the *kiara* data store.

    This means the files can be considered immutable, and they can be accessed via a *kiara* dataset id and *kiara value metadata is availble for the file_bundle dataset.
    """

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
