# -*- coding: utf-8 -*-
import typing

import pyarrow
import pyarrow as pa
from kiara import KiaraModule
from kiara.data import ValueSet
from kiara.data.values import ValueSchema
from kiara.module_config import KiaraModuleConfig
from pyarrow import csv
from pydantic import Field, validator

from kiara_modules.core.metadata_schemas import FileBundleMetadata, FileMetadata

AVAILABLE_FILE_COLUMNS = [
    "id",
    "rel_path",
    "orig_filename",
    "orig_path",
    "import_time",
    "mime_type",
    "size",
    "content",
    "path",
    "file_name",
]
DEFAULT_COLUMNS = ["id", "rel_path", "content"]


class CreateTableModuleConfig(KiaraModuleConfig):

    allow_column_filter: bool = Field(
        description="Whether to add an input option to filter columns.", default=False
    )


class CreateTableFromFileModule(KiaraModule):
    """Load table-like data from a *kiara* file object (not a path!)."""

    _config_cls = CreateTableModuleConfig
    _module_type_name = "from_file"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs = {
            "file": {
                "type": "file",
                "doc": "The file that contains table data.",
                "optional": False,
            }
        }

        if self.get_config_value("allow_column_filter"):

            inputs["columns"] = {
                "type": "array",
                "doc": "If provided, only import the columns that match items in this list.",
                "optional": False,
            }

        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {"table": {"type": "table", "doc": "the imported table"}}

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        input_file: FileMetadata = inputs.get_value_data("file")
        imported_data = csv.read_csv(input_file.path)

        if self.get_config_value("allow_column_filter"):
            if self.get_config_value("columns"):
                imported_data = imported_data.select(
                    self.get_config_value("only_columns")
                )

        outputs.set_value("table", imported_data)


class CreateTableFromTextFilesConfig(KiaraModuleConfig):

    columns: typing.List[str] = Field(
        description=f"A list of columns to add to the table. Available properties: {', '.join(AVAILABLE_FILE_COLUMNS)}",
        default=DEFAULT_COLUMNS,
    )

    @validator("columns")
    def _validate_columns(cls, v):

        if isinstance(v, str):
            v = [v]

        if not isinstance(v, typing.Iterable):
            raise ValueError("'columns' value must be a list.")

        invalid = set()
        for item in v:
            if item not in AVAILABLE_FILE_COLUMNS:
                invalid.add(item)

        if invalid:
            raise ValueError(
                f"Items in the 'columns' value must be one of {AVAILABLE_FILE_COLUMNS}. Invalid value(s): {', '.join(invalid)}"
            )

        return v


class CreateTableFromTextFilesModule(KiaraModule):
    """Create a table from a 'file_bundle'."""

    _config_cls = CreateTableFromTextFilesConfig
    _module_type_name = "from_file_bundle"

    _convert_profiles = {
        "file_bundle": {"table": {"constants": {"columns": AVAILABLE_FILE_COLUMNS}}}
    }

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs = {
            "files": {"type": "file_bundle", "doc": "The files to use for the table."}
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        id_column = "id"
        path_column = "rel_path"
        content = "content"

        outputs = {
            "table": {
                "type": "table",
                "doc": f"A table with the index column '{id_column}', a column '{path_column}' that indicates the relative path of the file in the bundle, and a column '{content}' that holds the (text) content of every file.",
            }
        }

        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        bundle: FileBundleMetadata = inputs.get_value_data("files")

        columns = self.get_config_value("columns")
        if not columns:
            columns = DEFAULT_COLUMNS

        if "content" in columns:
            file_dict = bundle.read_text_file_contents()
        else:
            file_dict = {}
            for rel_path in bundle.included_files.keys():
                file_dict[rel_path] = None  # type: ignore

        tabular: typing.Dict[str, typing.List[typing.Any]] = {}
        for column in columns:
            for index, rel_path in enumerate(sorted(file_dict.keys())):

                if column == "content":
                    value: typing.Any = file_dict[rel_path]
                elif column == "id":
                    value = index
                elif column == "rel_path":
                    value = rel_path
                else:
                    file_model = bundle.included_files[rel_path]
                    value = getattr(file_model, column)

                tabular.setdefault(column, []).append(value)

        table = pa.Table.from_pydict(tabular)

        outputs.set_value("table", table)
