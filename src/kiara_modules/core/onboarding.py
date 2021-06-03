# -*- coding: utf-8 -*-
import typing

from kiara import KiaraModule
from kiara.data.types.files import FileBundleModel, FileModel, FolderImportConfig
from kiara.data.values import Value, ValueSchema, ValueSet
from kiara.module_config import KiaraModuleConfig
from kiara.modules.metadata import ExtractMetadataModule
from pydantic import BaseModel, Field


class ImportLocalPathConfig(KiaraModuleConfig):

    source_is_immutable: bool = Field(
        description="Whether the data that lives in source path can be relied upon to not change, and always be available",
        default=False,
    )


class ImportLocalFileModule(KiaraModule):
    """Read a file into the data registry."""

    _config_cls = ImportLocalPathConfig
    _module_type_name = "load_file"

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

        path = inputs.get_value_data("path")
        file_model = FileModel.import_file(path)
        outputs.set_value("file", file_model)


class ImportLocalFolderModule(KiaraModule):

    _config_cls = ImportLocalPathConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {
            "path": {"type": "string", "doc": "The path to the folder."},
            "included_files": {
                "type": "list",
                "doc": "A list of strings, include all files where the filename ends with that string.",
                "optional": True,
            },
            "excluded_dirs": {
                "type": "list",
                "doc": "A list of strings, exclude all folders whose name ends with that string.",
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

        path = inputs.get_value_data("path")

        included_files = inputs.get_value_data("included_files")
        excluded_dirs = inputs.get_value_data("excluded_dirs")

        import_config = FolderImportConfig(
            include_files=included_files, exclude_dirs=excluded_dirs
        )

        bundle = FileBundleModel.import_folder(source=path, import_config=import_config)

        outputs.set_values(file_bundle=bundle)


class SaveFileBundle(KiaraModule):

    _module_type_name = "save_file_bundle"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Mapping[str, typing.Any] = {
            "files": {"type": "file_bundle", "doc": "The files."},
            "target": {"type": "string", "doc": "The base path to save the folder to."},
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs: typing.Mapping[str, typing.Any] = {
            "load_config": {
                "type": "load_config",
                "doc": "The configuration to use with kiara to load the saved value.",
            },
            "files": {"type": "file_bundle", "doc": "The copied file bundle."},
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        bundle: FileBundleModel = inputs.get_value_data("files")
        target: str = inputs.get_value_data("target")
        fm = bundle.save(target)
        outputs.set_value("files", fm)

        load_config = {
            "module_type": "onboarding.import_local_folder",
            "inputs": {"path": target},
            "output_name": "file_bundle",
        }

        outputs.set_value("load_config", load_config)


class FileBundleMetadataModule(ExtractMetadataModule):
    @classmethod
    def _get_supported_types(cls) -> str:
        return "file_bundle"

    @classmethod
    def get_metadata_key(cls) -> str:
        return "file_bundle"

    def _get_metadata_schema(
        self, type: str
    ) -> typing.Union[str, typing.Type[BaseModel]]:
        return FileBundleModel

    def extract_metadata(self, value: Value) -> typing.Mapping[str, typing.Any]:

        return value.get_value_data().dict()
