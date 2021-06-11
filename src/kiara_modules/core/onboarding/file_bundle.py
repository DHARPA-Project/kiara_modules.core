# -*- coding: utf-8 -*-
import typing

from kiara import KiaraModule
from kiara.data import Value, ValueSet
from kiara.data.values import ValueSchema
from kiara.modules.metadata import ExtractMetadataModule
from pydantic import BaseModel

from kiara_modules.core.metadata_models import FileBundleModel


class SaveFileBundle(KiaraModule):
    """Save a file bundle to disk."""

    _module_type_name = "save"

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
        return FileBundleModel

    def extract_metadata(self, value: Value) -> typing.Mapping[str, typing.Any]:

        return value.get_value_data().dict()
