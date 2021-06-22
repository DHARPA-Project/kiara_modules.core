# -*- coding: utf-8 -*-
import typing

from kiara import KiaraModule
from kiara.data import ValueSet
from kiara.data.values import Value, ValueSchema
from kiara.modules.metadata import ExtractMetadataModule
from pydantic import BaseModel

from kiara_modules.core.metadata_schemas import FileMetadata
from kiara_modules.core.onboarding import ImportLocalPathConfig


class ImportLocalFileModule(KiaraModule):
    """Read a file into the data registry."""

    _config_cls = ImportLocalPathConfig
    _module_type_name = "import"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {"path": {"type": "string", "doc": "The path to the file."}}

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

        print("path")
        path = inputs.get_value_data("path")
        file_model = FileMetadata.import_file(path)
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
