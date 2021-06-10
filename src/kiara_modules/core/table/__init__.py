# -*- coding: utf-8 -*-
import os
import typing

import pyarrow
import pyarrow as pa
from kiara import KiaraModule
from kiara.data import Value, ValueSet
from kiara.data.types.files import FileBundleModel, FileModel
from kiara.data.values import ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import KiaraModuleConfig
from kiara.modules.metadata import ExtractMetadataModule
from pyarrow import csv
from pyarrow import feather as feather
from pydantic import BaseModel, Field, validator

from kiara_modules.core.array import map_with_module

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

        input_file: FileModel = inputs.get_value_data("file")
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

        bundle: FileBundleModel = inputs.get_value_data("files")

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


class MergeTableModule(KiaraModule):
    """Create a table from other tables and/or arrays."""

    _module_type_name = "merge"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs = {
            "sources": {"type": "dict", "doc": "The source tables and/or columns."}
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs = {
            "table": {
                "type": "table",
                "doc": "The merged table, including all source tables and columns.",
            }
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        sources = inputs.get_value_data("sources")

        len_dict = {}
        arrays = []
        column_names = []
        for source_key, table_or_column in sources.items():

            if isinstance(table_or_column, pa.Table):
                rows = table_or_column.num_rows
                for name in table_or_column.schema.names:
                    column = table_or_column.column(name)
                    arrays.append(column)
                    column_names.append(name)

            elif isinstance(table_or_column, pa.Array):
                rows = len(table_or_column)
                arrays.append(table_or_column)
                column_names.append(source_key)
            else:
                raise KiaraProcessingException(
                    f"Can't merge table: invalid type '{type(table_or_column)}' for source '{source_key}'."
                )

            len_dict[source_key] = rows

        all_rows = None
        for source_key, rows in len_dict.items():
            if all_rows is None:
                all_rows = rows
            else:
                if all_rows != rows:
                    all_rows = None
                    break

        if all_rows is None:
            len_str = ""
            for name, rows in len_dict.items():
                len_str = f" {name} ({rows})"

            raise KiaraProcessingException(
                f"Can't merge table, sources have different lengths:{len_str}"
            )

        table = pa.Table.from_arrays(arrays=arrays, names=column_names)

        outputs.set_value("table", table)


class ColumnSchema(BaseModel):

    arrow_type_name: str = Field(description="The arrow type name of the column.")
    arrow_type_id: int = Field(description="The arrow type id of the column.")
    metadata: typing.Dict[str, typing.Any] = Field(
        description="Other metadata for the column.", default_factory=dict
    )


class TableMetadata(BaseModel):
    column_names: typing.List[str] = Field(
        description="The name of the columns of the table."
    )
    column_schema: typing.Dict[str, ColumnSchema] = Field(
        description="The schema description of the table.", alias="schema"
    )
    rows: int = Field(description="The number of rows the table contains.")
    size: int = Field(description="The tables size in bytes.")


class TableMetadataModule(ExtractMetadataModule):
    """Extract metadata from a table object."""

    _module_type_name = "metadata"

    @classmethod
    def _get_supported_types(cls) -> str:
        return "table"

    @classmethod
    def get_metadata_key(cls) -> str:
        return "table"

    def _get_metadata_schema(
        self, type: str
    ) -> typing.Union[str, typing.Type[BaseModel]]:
        return TableMetadata

    def extract_metadata(self, value: Value) -> typing.Mapping[str, typing.Any]:

        table: pyarrow.Table = value.get_value_data()
        table_schema = {}
        for name in table.schema.names:
            field = table.schema.field(name)
            md = field.metadata
            if not md:
                md = {}
            _type = field.type
            _d = {
                "arrow_type_name": str(_type),
                "arrow_type_id": _type.id,
                "metadata": md,
            }
            table_schema[name] = _d

        return {
            "column_names": table.column_names,
            "schema": table_schema,
            "rows": table.num_rows,
            "size": table.nbytes,
        }


class SaveArrowTable(KiaraModule):
    """Save a table object to disk."""

    _module_type_name = "save"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Mapping[str, typing.Any] = {
            "table": {"type": "table", "doc": "The table object."},
            "folder_path": {
                "type": "string",
                "doc": "The path to the folder to write the file.",
            },
            "file_name": {"type": "string", "doc": "The file name."},
            "format": {
                "type": "string",
                "doc": "The format of the table file ('feather' or 'parquet').",
                "default": "feather",
            },
            "force_overwrite": {
                "type": "boolean",
                "doc": "Whether to overwrite an existing file.",
                "default": False,
            },
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
            }
        }

        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        table: pa.Table = inputs.get_value_data("table")
        path: str = inputs.get_value_data("folder_path")
        file_name: str = inputs.get_value_data("file_name")
        full_path = os.path.join(path, file_name)
        force_overwrite = inputs.get_value_data("force_overwrite")

        if os.path.exists(full_path) and not force_overwrite:
            raise KiaraProcessingException(
                f"Can't write table to file, file already exists: {full_path}"
            )

        os.makedirs(path, exist_ok=True)

        feather.write_feather(table, full_path)

        result = {
            "module_type": "table.load",
            "inputs": {"path": full_path, "format": "feather"},
            "output_name": "table",
        }
        outputs.set_value("load_config", result)


class LoadArrowTable(KiaraModule):
    """Load a table object from disk."""

    _module_type_name = "load"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Mapping[str, typing.Any] = {
            "path": {"type": "string", "doc": "The path to the table file."},
            "format": {
                "type": "string",
                "doc": "The format of the table file ('feather' or 'parquet').",
                "default": "feather",
            },
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs: typing.Mapping[str, typing.Any] = {
            "table": {"type": "table", "doc": "The pyarrow table object."}
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        path = inputs.get_value_data("path")
        format = inputs.get_value_data("format")

        if format != "feather":
            raise NotImplementedError()

        table = feather.read_table(path)
        outputs.set_value("table", table)


class CutColumnModule(KiaraModule):
    """Cut off one column from a table, returning an array."""

    _module_type_name = "cut_column"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Mapping[str, typing.Any] = {
            "table": {"type": "table", "doc": "A table."},
            "column_name": {
                "type": "string",
                "doc": "The name of the column to extract.",
            },
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs: typing.Mapping[str, typing.Any] = {
            "array": {"type": "array", "doc": "The column."}
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        table_value = inputs.get_value_obj("table")

        column_name: str = inputs.get_value_data("column_name")
        available = table_value.get_metadata("table")["table"]["column_names"]
        if column_name not in available:
            raise KiaraProcessingException(
                f"Invalid column name '{column_name}'. Available column names: {available}"
            )

        table: pa.Table = inputs.get_value_data("table")
        column = table.column(column_name)

        outputs.set_value("array", column)


class MapColumnsModuleConfig(KiaraModuleConfig):

    module_type: str = Field(
        description="The name of the kiara module to use to filter the input data."
    )
    module_config: typing.Optional[typing.Dict[str, typing.Any]] = Field(
        description="The config for the kiara filter module.", default_factory=dict
    )
    input_name: typing.Optional[str] = Field(
        description="The name of the input name of the module which will receive the rows from our input table. Can be omitted if the configured module only has a single input.",
        default=None,
    )
    output_name: typing.Optional[str] = Field(
        description="The name of the output name of the module which will receive the items from our input array. Can be omitted if the configured module only has a single output.",
        default=None,
    )


class MapColumnModule(KiaraModule):
    """Map the items of one column of a table onto an array, using another module."""

    _config_cls = MapColumnsModuleConfig
    _module_type_name = "map_column"

    def module_instance_doc(self) -> str:

        config: MapColumnsModuleConfig = self.config  # type: ignore

        module_type = config.module_type
        module_config = config.module_config

        m = self._kiara.create_module(
            id="map_column_child", module_type=module_type, module_config=module_config
        )
        doc = m.doc()
        link = m.source_link()
        if not link:
            link_str = f"``{module_type}``"
        else:
            link_str = f"[``{module_type}``]({link})"

        result = f"""Map the values of the rows of an input table onto a new array of the same length, using the {link_str} module."""

        if doc and doc != "-- n/a --":
            result = result + f"\n\n``{module_type}`` documentation:\n\n{doc}"
        return result

    def __init__(self, *args, **kwargs):

        self._child_module: typing.Optional[KiaraModule] = None
        self._module_input_name: typing.Optional[str] = None
        self._module_output_name: typing.Optional[str] = None
        super().__init__(*args, **kwargs)

    @property
    def child_module(self) -> KiaraModule:

        if self._child_module is not None:
            return self._child_module

        module_name = self.get_config_value("module_type")
        module_config = self.get_config_value("module_config")
        self._child_module = self._kiara.create_module(
            module_type=module_name, module_config=module_config
        )
        return self._child_module

    @property
    def module_input_name(self) -> str:

        if self._module_input_name is not None:
            return self._module_input_name

        self._module_input_name = self.get_config_value("input_name")
        if self._module_input_name is None:
            if len(list(self.child_module.input_names)) == 1:
                self._module_input_name = next(iter(self.child_module.input_names))
            else:
                raise KiaraProcessingException(
                    f"No 'input_name' specified, and configured module has more than one inputs. Please specify an 'input_name' value in your module config, pick one of: {', '.join(self.child_module.input_names)}"
                )

        return self._module_input_name

    @property
    def module_output_name(self) -> str:

        if self._module_output_name is not None:
            return self._module_output_name

        self._module_output_name = self.get_config_value("output_name")
        if self._module_output_name is None:
            if len(list(self.child_module.output_names)) == 1:
                self._module_output_name = next(iter(self.child_module.output_names))
            else:
                raise KiaraProcessingException(
                    f"No 'output_name' specified, and configured module has more than one outputs. Please specify an 'output_name' value in your module config, pick one of: {', '.join(self.child_module.output_names)}"
                )

        return self._module_output_name

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Dict[
            str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
        ] = {
            "table": {
                "type": "table",
                "doc": "The table to use as input.",
            },
            "column_name": {
                "type": "string",
                "doc": "The name of the table column to run the mapping operation on.",
            },
        }
        for input_name, schema in self.child_module.input_schemas.items():
            assert input_name != "table"
            assert input_name != "column_name"
            if input_name == self.module_input_name:
                continue
            inputs[input_name] = schema
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs = {
            "array": {
                "type": "array",
                "doc": "An array of equal length to the input array, containing the 'mapped' values.",
            }
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        table: pa.Table = inputs.get_value_data("table")
        column_name = inputs.get_value_data("column_name")

        if column_name not in table.column_names:
            raise KiaraProcessingException(
                f"Table column '{column_name}' not available in table. Available columns: {', '.join(table.column_names)}."
            )

        input_array: pa.Array = table.column(column_name)

        init_data: typing.Dict[str, typing.Any] = {}
        for input_name in self.input_schemas.keys():
            if input_name in ["table", "column_name", self.module_input_name]:
                continue

            init_data[input_name] = inputs.get_value_obj(input_name)

        result_list = map_with_module(
            input_array,
            module_input_name=self.module_input_name,
            module_obj=self.child_module,
            init_data=init_data,
            module_output_name=self.module_output_name,
        )
        outputs.set_value("array", pa.array(result_list))