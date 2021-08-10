# -*- coding: utf-8 -*-
import os
import typing

from kiara import KiaraModule
from kiara.data import ValueSet
from kiara.data.values import Value, ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.operations.data_import import DataImportModule
from kiara.operations.extract_metadata import ExtractMetadataModule
from kiara.operations.save_value import SaveValueTypeModule
from pydantic import BaseModel

from kiara_modules.core.metadata_schemas import FileMetadata


class FileImportModule(DataImportModule):
    @classmethod
    def retrieve_supported_value_type(cls) -> str:
        return "file"

    def import_from_path_string(
        self, source: str, base_aliases: typing.List[str]
    ) -> FileMetadata:

        file_model = FileMetadata.load_file(source)
        return file_model


class SaveFileTypeModule(SaveValueTypeModule):
    """Save a file to disk."""

    _module_type_name = "save"

    @classmethod
    def retrieve_supported_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        return "file"

    def save_value(self, value: Value, base_path: str) -> typing.Dict[str, typing.Any]:

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
