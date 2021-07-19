# -*- coding: utf-8 -*-
import typing

from kiara import KiaraModule
from kiara.data import ValueSet
from kiara.data.values import NonRegistryValue, ValueSchema

from kiara_modules.core.metadata_schemas import (
    FileBundleMetadata,
    FileMetadata,
    FolderImportConfig,
)

KIARA_METADATA = {"tags": ["onboarding"]}


class OnboardFileModule(KiaraModule):
    """Import (copy) a file and its metadata into the internal data store.

    This module is usually the first step to import a local folder into the *kiara* data store. It is necessary,
    because the folder needs to be copied to a different location, so we can be sure it isn't modified outside of
    *kiara*. In most cases, this step will be followed by a ``onboarding.file_bundle.save`` step.
    """

    _module_type_name = "file"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {
            "path": {"type": "string", "doc": "The path to the file."},
            "aliases": {
                "type": "list",
                "doc": "A list of aliases to give the dataset in the internal data store.",
                "optional": True,
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
            },
            "dataset_id": {
                "type": "string",
                "doc": "The id of the dataset in the internal data store.",
            },
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        path = inputs.get_value_data("path")
        aliases = inputs.get_value_data("aliases")

        if aliases:
            pass

        file_model = FileMetadata.load_file(path)

        file_schema = ValueSchema(
            type="file", optional=False, doc=f"Onboarded item from: {path}"
        )
        value = NonRegistryValue(
            _init_value=file_model,
            value_schema=file_schema,
            is_constant=True,
            kiara=self._kiara,
        )

        dataset_id = self._kiara.data_store.save_value(value=value)

        outputs.set_values(file=file_model, dataset_id=dataset_id)


class OnboardFolderModule(KiaraModule):
    """Import (copy) a folder and its metadata into the internal data store.

    This module is usually the first step to import a local folder into the *kiara* data store. It is necessary,
    because the folder needs to be copied to a different location, so we can be sure it isn't modified outside of
    *kiara*. In most cases, this step will be followed by a ``onboarding.file_bundle.save`` step.
    """

    _module_type_name = "folder"

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
            },
            "dataset_id": {
                "type": "string",
                "doc": "The id of the dataset in the internal data store.",
            },
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        path = inputs.get_value_data("path")

        included_files = inputs.get_value_data("included_files")
        excluded_dirs = inputs.get_value_data("excluded_dirs")

        import_config = FolderImportConfig(
            include_files=included_files, exclude_dirs=excluded_dirs
        )

        bundle = FileBundleMetadata.import_folder(
            source=path, import_config=import_config
        )

        file_bundle_schema = ValueSchema(
            type="file_bundle", optional=False, doc=f"Onboarded item from: {path}"
        )
        value = NonRegistryValue(
            _init_value=bundle,
            value_schema=file_bundle_schema,
            is_constant=True,
            kiara=self._kiara,
        )

        dataset_id = self._kiara.data_store.save_value(value)

        outputs.set_values(file_bundle=bundle, dataset_id=dataset_id)
