# -*- coding: utf-8 -*-
import os
import typing

from kiara import KiaraModule
from kiara.data import Value, ValueSet
from kiara.data.values import ValueSchema
from kiara.defaults import DEFAULT_EXCLUDE_DIRS, DEFAULT_EXCLUDE_FILES
from kiara.operations.data_import import DataImportModule
from kiara.operations.extract_metadata import ExtractMetadataModule
from kiara.operations.save_value import SaveValueTypeModule
from pydantic import BaseModel

from kiara_modules.core.metadata_schemas import FileBundleMetadata, FolderImportConfig


class FileImportModule(DataImportModule):
    @classmethod
    def retrieve_supported_value_type(cls) -> str:
        return "file_bundle"

    def import_from_path_string(
        self, source: str, base_aliases: typing.List[str]
    ) -> FileBundleMetadata:

        file_bundle_model = FileBundleMetadata.import_folder(source)
        return file_bundle_model


class SaveFileBundleType(SaveValueTypeModule):
    """Save a file bundle to disk."""

    _module_type_name = "save"

    @classmethod
    def retrieve_supported_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        return "file_bundle"

    def save_value(self, value: Value, base_path: str):

        bundle: FileBundleMetadata = value.get_value_data()
        rel_path = bundle.bundle_name

        target_path = os.path.join(base_path, rel_path)
        fb = bundle.copy_bundle(target_path)

        # the following changes the input value, which is usually not allowed, but the file_bundle type is a special case
        bundle.included_files = fb.included_files
        bundle.is_onboarded = True
        bundle.path = fb.path
        for path, f in bundle.included_files.items():
            f.is_onboarded = True

        load_config = {
            "module_type": "file_bundle.load",
            "base_path_input_name": "base_path",
            "inputs": {"base_path": base_path, "rel_path": rel_path},
            "output_name": "file_bundle",
        }

        return load_config


class LoadFileBundleModule(KiaraModule):
    """Load a file bundle and its metadata.

    This module does not read or load the content of all included files, but contains the path to the local representation/version of them
    so they can be read by a subsequent process.
    """

    _module_type_name = "load"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {
            "base_path": {
                "type": "string",
                "doc": "The base path where the folder lives.",
            },
            "rel_path": {
                "type": "string",
                "doc": "The relative path of the folder, within the base path location.",
            },
            "include_files": {
                "type": "list",
                "doc": "A list of strings, include all files where the filename ends with one of those strings.\n\nOnly full string matches are supported at the moment, globs and regex might be in the future.",
                "optional": True,
            },
            "exclude_dirs": {
                "type": "list",
                "doc": f"A list of strings, exclude all folders whose name ends with that string. Defaults to: {DEFAULT_EXCLUDE_DIRS}.\n\nOnly full string matches are supported at the moment, globs and regex might be in the future.",
                "default": DEFAULT_EXCLUDE_DIRS,
                "optional": True,
            },
            "exclude_files": {
                "type": "list",
                "doc": f"A list of strings, exclude all files that end with that one of those strings (takes precedence over 'include_files'). Defaults to: {DEFAULT_EXCLUDE_FILES}\n\nOnly full string matches are supported at the moment, globs and regex might be in the future.",
                "default": DEFAULT_EXCLUDE_FILES,
                "optional": True,
            },
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "file_bundle": {
                "type": "file_bundle",
                "doc": "The collection of files contained in the bundle.",
            }
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        base_path = inputs.get_value_data("base_path")
        rel_path = inputs.get_value_data("rel_path")

        path = os.path.join(base_path, rel_path)

        included_files = inputs.get_value_data("include_files")
        excluded_dirs = inputs.get_value_data("exclude_dirs")
        excluded_files = inputs.get_value_data("exclude_files")

        import_config = FolderImportConfig(
            include_files=included_files,
            exclude_dirs=excluded_dirs,
            excluded_files=excluded_files,
        )

        bundle = FileBundleMetadata.import_folder(
            source=path, import_config=import_config
        )

        outputs.set_values(file_bundle=bundle)


class FileBundleMetadataModule(ExtractMetadataModule):

    _module_type_name = "metadata"

    @classmethod
    def _get_supported_types(cls) -> str:
        return "file_bundle"

    @classmethod
    def get_metadata_key(cls) -> str:
        return "file_bundle"

    def _get_metadata_schema(
        self, type: str
    ) -> typing.Union[str, typing.Type[BaseModel]]:
        return FileBundleMetadata

    def extract_metadata(self, value: Value) -> typing.Mapping[str, typing.Any]:

        return value.get_value_data().dict()


# class CalculateFileBundleHash(KiaraModule):
#     """Calculate the hash for a file bundle."""
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
#             "file_bundle": {
#                 "type": "file_bundle",
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
#                 "doc": "The hash for the file bundle."
#             }
#         }
#
#     def process(self, inputs: ValueSet, outputs: ValueSet) -> None:
#
#         hash_func = inputs.get_value_data("hash_func")
#         if hash_func != "sha3-256":
#             raise KiaraProcessingException(f"Hash function not supported (yet): {hash_func}")
#
#         f: FileBundleMetadata = inputs.get_value_data("file_bundle")
#         hash = f.file_bundle_hash
#
#         outputs.set_value("hash", hash)
