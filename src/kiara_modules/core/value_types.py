# -*- coding: utf-8 -*-

"""This module contains the value type classes that are used in the ``kiara_modules.core`` package.
"""

import datetime
import typing

from kiara import KiaraEntryPointItem
from kiara.data.types import ValueType
from kiara.utils.class_loading import find_value_types_under
from rich.console import ConsoleRenderable, RichCast

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
    def calculate_value_hash(cls, value: typing.Any, hash_type: str) -> str:
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
    def calculate_value_hash(cls, value: typing.Any, hash_type: str) -> str:
        return str(hash(value))

    def validate(cls, value: typing.Any) -> None:

        if not isinstance(value, str):
            raise ValueError(f"Invalid type '{type(value)}': string required")


class BooleanType(ValueType):
    "A boolean."

    @classmethod
    def calculate_value_hash(cls, value: typing.Any, hash_type: str) -> str:
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

    @classmethod
    def calculate_value_hash(cls, value: typing.Any, hash_type: str) -> str:
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

    @classmethod
    def calculate_value_hash(cls, value: typing.Any, hash_type: str) -> str:
        return str(hash(value))

    def validate(cls, value: typing.Any) -> typing.Any:

        if not isinstance(value, float):
            raise ValueError(f"Invalid type '{type(value)}' for float: {value}")


class DictType(ValueType):
    """A dict-like object."""

    @classmethod
    def calculate_value_hash(cls, value: typing.Any, hash_type: str) -> str:

        from deepdiff import DeepHash

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

    @classmethod
    def calculate_value_hash(self, value: typing.Any, hash_type: str) -> str:

        from deepdiff import DeepHash

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
        import pyarrow as pa

        return [pa.Table]

    @classmethod
    def check_data(cls, data: typing.Any) -> typing.Optional["ValueType"]:

        import pyarrow as pa

        if isinstance(data, pa.Table):
            return TableType()

        return None

    @classmethod
    def get_supported_hash_types(cls) -> typing.Iterable[str]:

        return ["pandas_df_hash"]

    @classmethod
    def calculate_value_hash(cls, value: typing.Any, hash_type: str) -> str:

        import pyarrow as pa

        # this is only for testing, and will be replaced with a native arrow table hush function, once I figure out how to do that efficiently
        table: pa.Table = value
        from pandas.util import hash_pandas_object

        hash_result = hash_pandas_object(table.to_pandas()).sum()

        return str(hash_result)

    def validate(cls, value: typing.Any) -> None:
        import pyarrow as pa

        assert isinstance(value, pa.Table)


class ArrayType(ValueType):
    """An Apache arrow array."""


class DateType(ValueType):
    """A date.

    Internally, this will always be represented as a Python ``datetime`` object. Iff provided as input, it can also
    be as string, in which case the [``dateutils.parser.parse``](https://dateutil.readthedocs.io/en/stable/parser.html#dateutil.parser.parse) method will be used to parse the string into a datetime object.
    """

    @classmethod
    def calculate_value_hash(cls, value: typing.Any, hash_typpe: str) -> str:

        return str(hash(value))

    def parse_value(self, v: typing.Any) -> typing.Any:

        from dateutil import parser

        if isinstance(v, str):
            d = parser.parse(v)
            return d

        return None

    def validate(cls, value: typing.Any):
        assert isinstance(value, datetime.datetime)


class FileType(ValueType):
    """A representation of a file.

    It is recommended to 'onboard' files before working with them, otherwise metadata consistency can not be guaranteed.
    """

    @classmethod
    def candidate_python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [FileType]

    @classmethod
    def get_supported_hash_types(cls) -> typing.Iterable[str]:
        return ["sha3_256"]

    @classmethod
    def calculate_value_hash(cls, value: typing.Any, hash_type: str) -> str:

        assert hash_type == "sha3_256"
        assert isinstance(value, FileMetadata)
        return value.file_hash


class FileBundleType(ValueType):
    """A representation of a set of files (folder, archive, etc.).

    It is recommended to 'onboard' files before working with them, otherwise metadata consistency can not be guaranteed.
    """

    @classmethod
    def candidate_python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [FileBundleType]

    @classmethod
    def get_supported_hash_types(cls) -> typing.Iterable[str]:
        return ["sha3_256"]

    @classmethod
    def calculate_value_hash(cls, value: typing.Any, hash_type: str) -> str:

        assert hash_type == "sha3_256"

        assert isinstance(value, FileBundleMetadata)
        return value.file_bundle_hash


class RenderablesType(ValueType):
    """A list of renderable objects, used in the 'rich' Python library, to print to the terminal or in Jupyter.

    Internally, the result list items can be either a string, a 'rich.console.ConsoleRenderable', or a 'rich.console.RichCast'.
    """

    @classmethod
    def candidate_python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [str, ConsoleRenderable, RichCast]
