# -*- coding: utf-8 -*-


KIARA_METADATA = {"tags": ["onboarding", "import"]}


# class OnboardBytesModule(KiaraModule):
#     """Import a byte-array, and save it as 'file'."""
#
#     _module_type_name = "bytes"
#
#     def create_input_schema(
#         self,
#     ) -> typing.Mapping[
#         str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
#     ]:
#
#         return {
#             "bytes": {
#                 "type": "bytes",
#                 "doc": "The byte array."
#             },
#             "file_name": {
#                 "type": "string",
#                 "doc": "The name to give the file internally. This is only used in metadata.",
#                 "optional": True
#             },
#             "aliases": {
#                 "type": "list",
#                 "doc": "A list of aliases to give the dataset in the internal data store.",
#                 "optional": True
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
#             "file": {
#                 "type": "file",
#                 "doc": "The representation of the byte-array as file/file-metadata."
#             }
#         }

#
# class OnboardFileModule(KiaraModule):
#     """Import (copy) a file and its metadata into the internal data store.
#
#     This module is used to import a local file into the *kiara* data store. It is necessary,
#     because the file needs to be copied to a different location, so we can be sure it isn't modified outside of
#     *kiara*.
#     """
#
#     _module_type_name = "local_file"
#
#     def create_input_schema(
#         self,
#     ) -> typing.Mapping[
#         str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
#     ]:
#         return {
#             "path": {"type": "string", "doc": "The path to the file."},
#             "aliases": {
#                 "type": "list",
#                 "doc": "A list of aliases to give the dataset in the internal data store.",
#                 "optional": True,
#             },
#         }
#
#     def create_output_schema(
#         self,
#     ) -> typing.Mapping[
#         str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
#     ]:
#         return {
#             "file": {
#                 "type": "file",
#                 "doc": "A representation of the original file content in the kiara data registry.",
#             },
#             "dataset_id": {
#                 "type": "string",
#                 "doc": "The id of the dataset in the internal data store.",
#             },
#         }
#
#     def process(self, inputs: ValueSet, outputs: ValueSet) -> None:
#
#         path = inputs.get_value_data("path")
#         aliases = inputs.get_value_data("aliases")
#
#         file_model = FileMetadata.load_file(path)
#
#         file_schema = ValueSchema(
#             type="file", optional=False, doc=f"Onboarded item from: {path}"
#         )
#
#         value = NonRegistryValue(  # type: ignore
#             _init_value=file_model,
#             value_schema=file_schema,
#             is_constant=True,
#             kiara=self._kiara,
#         )
#
#         dataset_md = self._kiara.data_store.save_value(value=value, aliases=aliases)
#
#         outputs.set_values(file=file_model, dataset_id=dataset_md.value_id)
#
#
# class OnboardFolderModule(KiaraModule):
#     """Import (copy) a folder and its metadata into the internal data store.
#
#     This module is usually the first step to import a local folder into the *kiara* data store. It is necessary,
#     because the folder needs to be copied to a different location, so we can be sure it isn't modified outside of
#     *kiara*.
#     """
#
#     _module_type_name = "local_folder"
#
#     def create_input_schema(
#         self,
#     ) -> typing.Mapping[
#         str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
#     ]:
#         return {
#             "path": {"type": "string", "doc": "The path to the folder."},
#             "included_files": {
#                 "type": "list",
#                 "doc": "A list of strings, include all files where the filename ends with that string.",
#                 "optional": True,
#             },
#             "excluded_dirs": {
#                 "type": "list",
#                 "doc": "A list of strings, exclude all folders whose name ends with that string.",
#                 "optional": True,
#             },
#             "aliases": {
#                 "type": "list",
#                 "doc": "A list of aliases to give the dataset in the internal data store.",
#                 "optional": True,
#             },
#         }
#
#     def create_output_schema(
#         self,
#     ) -> typing.Mapping[
#         str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
#     ]:
#
#         return {
#             "file_bundle": {
#                 "type": "file_bundle",
#                 "doc": "The collection of files contained in the bundle.",
#             },
#             "dataset_id": {
#                 "type": "string",
#                 "doc": "The id of the dataset in the internal data store.",
#             },
#         }
#
#     def process(self, inputs: ValueSet, outputs: ValueSet) -> None:
#
#         path = inputs.get_value_data("path")
#         aliases = inputs.get_value_data("aliases")
#
#         included_files = inputs.get_value_data("included_files")
#         excluded_dirs = inputs.get_value_data("excluded_dirs")
#
#         import_config = FolderImportConfig(
#             include_files=included_files, exclude_dirs=excluded_dirs
#         )
#
#         bundle = FileBundleMetadata.import_folder(
#             source=path, import_config=import_config
#         )
#
#         file_bundle_schema = ValueSchema(
#             type="file_bundle", optional=False, doc=f"Onboarded item from: {path}"
#         )
#         value = NonRegistryValue(  # type: ignore
#             _init_value=bundle,
#             value_schema=file_bundle_schema,
#             is_constant=True,
#             kiara=self._kiara,
#         )
#
#         dataset_md = self._kiara.data_store.save_value(value, aliases=aliases)
#
#         outputs.set_values(file_bundle=bundle, dataset_id=dataset_md.value_id)
