# -*- coding: utf-8 -*-
import os
import typing

from kiara import KiaraModule
from kiara.data import ValueSet
from kiara.data.values import Value, ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.operations.data_import import FileImportModule
from kiara.operations.extract_metadata import ExtractMetadataModule
from kiara.operations.store_value import StoreValueTypeModule
from pydantic import BaseModel

from kiara_modules.core.metadata_schemas import KiaraFile


class DefaultFileImportModule(FileImportModule):
    """Import an external file into a kiara session."""

    _module_type_name = "import"

    def import_from__file_path__string(self, source: str) -> KiaraFile:

        file_model = KiaraFile.load_file(source)
        return file_model


class StoreFileTypeModule(StoreValueTypeModule):
    """Save a file to disk."""

    _module_type_name = "store"

    @classmethod
    def retrieve_supported_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        return "file"

    def store_value(
        self, value: Value, base_path: str
    ) -> typing.Tuple[typing.Dict[str, typing.Any], typing.Any]:

        file_obj = value.get_value_data()

        file_name = file_obj.file_name
        full_target = os.path.join(base_path, file_name)

        os.makedirs(os.path.dirname(full_target), exist_ok=True)

        if os.path.exists(full_target):
            raise KiaraProcessingException(
                f"Can't save file, path already exists: {full_target}"
            )

        fm = file_obj.copy_file(full_target, is_onboarded=True)

        load_config = {
            "module_type": "file.load",
            "base_path_input_name": "base_path",
            "inputs": {"base_path": base_path, "rel_path": file_name},
            "output_name": "file",
        }
        return (load_config, fm)


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

        file_model = KiaraFile.load_file(path)
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
        return KiaraFile

    def extract_metadata(self, value: Value) -> typing.Mapping[str, typing.Any]:

        return value.get_value_data().dict(exclude_none=True)


# class ReadFileContentModuleConfig(ModuleTypeConfigSchema):
#
#     read_as_text: bool = Field(
#         description="Whether to read the file as text, or binary.", default=True
#     )
#
#
# class ReadFileContentModule(KiaraModule):
#     """Import a file into the kiara data store.
#
#     This module will support multiple source types and profiles in the future, but at the moment only import from
#     local file is supported. Thus, requiring the config value 'local' for 'source_profile', and 'file_path' for 'source_type'.
#     """
#
#     _module_type_name = "read_content"
#     _config_cls = ReadFileContentModuleConfig
#
#     def create_input_schema(
#         self,
#     ) -> typing.Mapping[
#         str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
#     ]:
#
#         return {
#             "base_path": {
#                 "type": "folder_path",
#                 "doc": "The folder where the file is located.",
#             },
#             "file_name": {"type": "string", "doc": "The file name."},
#         }
#
#     def create_output_schema(
#         self,
#     ) -> typing.Mapping[
#         str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
#     ]:
#
#         return {
#             "value_item": {
#                 "type": "string" if self.get_config_value("read_as_text") else "bytes",
#                 "doc": "The file content.",
#             }
#         }
#
#     def process(self, inputs: ValueSet, outputs: ValueSet) -> None:
#
#         as_text = self.get_config_value("read_as_text")
#         base_path = inputs.get_value_data("base_path")
#         file_name = inputs.get_value_data("file_name")
#
#         full_path = os.path.join(base_path, file_name)
#
#         if not os.path.exists(full_path):
#             raise KiaraProcessingException(
#                 f"Can't read string value, path to file does not exist: {full_path}"
#             )
#
#         if not os.path.isfile(os.path.realpath(full_path)):
#             raise KiaraProcessingException(
#                 f"Can't read string value, path is not a file: {full_path}"
#             )
#
#         if as_text:
#             mode = "r"
#         else:
#             mode = "rb"
#
#         with open(full_path, mode) as f:
#             content = f.read()
#
#         outputs.set_value("value_item", content)
