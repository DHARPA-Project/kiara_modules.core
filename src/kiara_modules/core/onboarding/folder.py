# -*- coding: utf-8 -*-
import typing

from kiara import KiaraModule
from kiara.data import ValueSet
from kiara.data.values import ValueSchema

from kiara_modules.core.metadata_schemas import FileBundleMetadata, FolderImportConfig
from kiara_modules.core.onboarding import ImportLocalPathConfig


class ImportFolderModule(KiaraModule):
    """Import a local folder and its metadata.

    This module is usually the first step to import a local folder into the *kiara* data store. It is necessary,
    because the folder needs to be copied to a different location, so we can be sure it isn't modified outside of
    *kiara*. In most cases, this step will be followed by a ``onboarding.file_bundle.save`` step.
    """

    _config_cls = ImportLocalPathConfig
    _module_type_name = "import"

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

        bundle = FileBundleMetadata.import_folder(
            source=path, import_config=import_config
        )

        outputs.set_values(file_bundle=bundle)
