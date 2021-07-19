# -*- coding: utf-8 -*-

"""This module contains the value type classes that are used in the ``kiara_modules.core`` package.
"""

import datetime
import typing

import pyarrow as pa
from dateutil import parser
from deepdiff import DeepHash
from kiara import KiaraEntryPointItem
from kiara.data.types import ValueType
from kiara.utils.class_loading import find_value_types_under

from kiara_modules.core.metadata_schemas import FileBundleMetadata, FileMetadata

value_types: KiaraEntryPointItem = (
    find_value_types_under,
    ["kiara_modules.core.value_types"],
)


class AnyType(ValueType):
    """Any type / No type information."""

    _value_type_name = "any"


class BytesType(ValueType):
    """An array of bytes."""

    _value_type_name = "bytes"

    @classmethod
    def calculate_value_hash(cls, value: typing.Any) -> str:
        return str(hash(value))

    # @classmethod
    # def get_operations(
    #     cls,
    # ) -> typing.Mapping[str, typing.Mapping[str, typing.Mapping[str, typing.Any]]]:
    #
    #     return {
    #         "save_value": {
    #             "default": {
    #                 "module_type": "bytes.save",
    #                 "input_name": "bytes",
    #             }
    #         }
    #     }


class StringType(ValueType):
    """A string."""

    @classmethod
    def calculate_value_hash(cls, value: typing.Any) -> str:
        return str(hash(value))

    def validate(cls, value: typing.Any) -> None:

        if not isinstance(value, str):
            raise ValueError(f"Invalid type '{type(value)}': string required")


class BooleanType(ValueType):
    "A boolean."

    @classmethod
    def calculate_value_hash(cls, value: typing.Any) -> str:
        return str(hash(value))

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

    def calculate_value_hash(self, value: typing.Any) -> str:
        return str(hash(value))

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

    def calculate_value_hash(self, value: typing.Any) -> str:
        return str(hash(value))

    def validate(cls, value: typing.Any) -> typing.Any:

        if not isinstance(value, float):
            raise ValueError(f"Invalid type '{type(value)}' for float: {value}")


class DictType(ValueType):
    """A dict-like object."""

    def calculate_value_hash(self, value: typing.Any) -> str:

        dh = DeepHash(value)
        return str(dh[value])

    def validate(cls, value: typing.Any) -> None:

        if not isinstance(value, typing.Mapping):
            raise ValueError(f"Invalid type '{type(value)}', not a mapping.")

    # def extract_type_metadata(
    #     cls, value: typing.Any
    # ) -> typing.Mapping[str, typing.Any]:
    #     value_types = set()
    #     for val in value.values():
    #         value_types.add(get_type_name(val))
    #     result = {"keys": list(value.keys()), "value_types.py": list(value_types)}
    #     return result


class ListType(ValueType):
    """A list-like object."""

    def calculate_value_hash(self, value: typing.Any) -> str:

        dh = DeepHash(value)
        return str(dh[value])

    def validate(cls, value: typing.Any) -> None:

        assert isinstance(value, typing.Iterable)

    # def extract_type_metadata(
    #     cls, value: typing.Any
    # ) -> typing.Mapping[str, typing.Any]:
    #
    #     metadata = {"length": len(value)}
    #     return metadata


class TableType(ValueType):
    """A table.

    Internally, this is backed by the [Apache Arrow](https://arrow.apache.org) [``Table``](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html) class.
    """

    @classmethod
    def candidate_python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [pa.Table]

    @classmethod
    def check_data(cls, data: typing.Any) -> typing.Optional["ValueType"]:

        if isinstance(data, pa.Table):
            return TableType()

        return None

    def validate(cls, value: typing.Any) -> None:
        assert isinstance(value, pa.Table)


class ArrayType(ValueType):
    """An Apache arrow array."""

    # def extract_type_metadata(
    #     cls, value: typing.Any
    # ) -> typing.Mapping[str, typing.Any]:
    #
    #     metadata = {
    #         "item_type": str(value.type),
    #         "arrow_type_id": value.type.id,
    #         "length": len(value),
    #     }
    #     return metadata


class DateType(ValueType):
    """A date.

    Internally, this will always be represented as a Python ``datetime`` object. Iff provided as input, it can also
    be as string, in which case the [``dateutils.parser.parse``](https://dateutil.readthedocs.io/en/stable/parser.html#dateutil.parser.parse) method will be used to parse the string into a datetime object.
    """

    @classmethod
    def calculate_value_hash(cls, value: typing.Any) -> str:

        return str(hash(value))

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

    @classmethod
    def candidate_python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [FileType]

    @classmethod
    def calculate_value_hash(cls, value: typing.Any) -> str:

        assert isinstance(value, FileMetadata)
        return value.file_hash


class FileBundleType(ValueType):
    """Represents a set of files that were imported into the *kiara* data store.

    This means the files can be considered immutable, and they can be accessed via a *kiara* dataset id and *kiara value metadata is availble for the file_bundle dataset.
    """

    @classmethod
    def candidate_python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [FileBundleType]

    @classmethod
    def calculate_value_hash(cls, value: typing.Any) -> str:

        assert isinstance(value, FileBundleMetadata)
        return value.file_bundle_hash
