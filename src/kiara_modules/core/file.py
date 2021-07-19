# -*- coding: utf-8 -*-
import os
import typing

from kiara import KiaraModule
from kiara.data import ValueSet
from kiara.data.operations.save_value import SaveValueModule
from kiara.data.values import Value, ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.modules.metadata import ExtractMetadataModule
from pydantic import BaseModel

from kiara_modules.core.metadata_schemas import FileMetadata


class SaveFileModule(SaveValueModule):
    """Save a file to disk."""

    _module_type_name = "save"

    @classmethod
    def _get_supported_types(self) -> typing.Union[str, typing.Iterable[str]]:
        return "file"

    def save_value(
        self, value: Value, value_id: str, base_path: str
    ) -> typing.Dict[str, typing.Any]:

        file_obj = value.get_value_data()

        file_name = file_obj.file_name
        full_target = os.path.join(base_path, file_name)

        os.makedirs(os.path.dirname(full_target), exist_ok=True)

        if os.path.exists(full_target):
            raise KiaraProcessingException(
                f"Can't save file, path already exists: {full_target}"
            )

        fm = file_obj.copy_file(full_target)

        # the following changes the input value, which is usually not allowed, but the file type is a special case
        file_obj.is_onboarded = True
        file_obj.path = fm.path

        load_config = {
            "module_type": "file.load",
            "base_path_input_name": "base_path",
            "inputs": {"base_path": base_path, "rel_path": file_name},
            "output_name": "file",
        }
        return load_config


class LoadLocalFileModule(KiaraModule):
    """Load a file and its metadata.

    This module does not read or load the content of a file, but contains the path to the local representation/version of the
    file so it can be read by a subsequent process.
    """

    # _config_cls = ImportLocalPathConfig
    _module_type_name = "load"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {
            "base_path": {
                "type": "string",
                "doc": "The path to the base directory where the file is stored.",
            },
            "rel_path": {
                "type": "string",
                "doc": "The relative path of the file within the base directory.",
            },
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {
            "file": {
                "type": "file",
                "doc": "A representation of the original file content in the kiara data registry.",
            }
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        base_path = inputs.get_value_data("base_path")
        rel_path = inputs.get_value_data("rel_path")

        path = os.path.join(base_path, rel_path)

        file_model = FileMetadata.load_file(path)
        outputs.set_value("file", file_model)


class FileMetadataModule(ExtractMetadataModule):

    _module_type_name = "metadata"

    @classmethod
    def _get_supported_types(cls) -> str:
        return "file"

    @classmethod
    def get_metadata_key(cls) -> str:
        return "file"

    def _get_metadata_schema(
        self, type: str
    ) -> typing.Union[str, typing.Type[BaseModel]]:
        return FileMetadata

    def extract_metadata(self, value: Value) -> typing.Mapping[str, typing.Any]:

        return value.get_value_data().dict()


# class CalculateFileHash(KiaraModule):
#     """Calculate the hash for a file item."""
#
#     _module_type_name = "hash"
#
#     def create_input_schema(
#         self,
#     ) -> typing.Mapping[
#         str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
#     ]:
#
#         return {
#             "file": {
#                 "type": "file",
#                 "doc": "The file item."
#             },
#             "hash_func": {
#                 "type": "string",
#                 "doc": "The hash function to use (not implemented yet).",
#                 "default": "sha3-256"
#             }
#         }
#
#     def create_output_schema(
#         self,
#     ) -> typing.Mapping[
#         str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
#     ]:
#
#         return {
#             "hash": {
#                 "type": "string",
#                 "doc": "The hash for the file."
#             }
#         }
#
#     def process(self, inputs: ValueSet, outputs: ValueSet) -> None:
#
#         hash_func = inputs.get_value_data("hash_func")
#         if hash_func != "sha3-256":
#             raise KiaraProcessingException(f"Hash function not supported (yet): {hash_func}")
#
#         f: FileMetadata = inputs.get_value_data("file")
#         hash = f.file_hash
#
#         outputs.set_value("hash", hash)
