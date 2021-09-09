# -*- coding: utf-8 -*-
import base64
import os
import typing

from kiara import KiaraModule
from kiara.data import Value, ValueSet
from kiara.data.values import ValueSchema
from kiara.defaults import NO_VALUE_ID_MARKER
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import ModuleTypeConfigSchema
from kiara.operations.extract_metadata import ExtractMetadataModule
from kiara.operations.save_value import SaveValueModuleConfig, SaveValueTypeModule
from kiara.operations.type_convert import ConvertValueModule
from pydantic import BaseModel, Field

from kiara_modules.core.array import map_with_module
from kiara_modules.core.metadata_schemas import (
    FileBundleMetadata,
    FileMetadata,
    TableMetadata,
)

DEFAULT_SAVE_TABLE_FILE_NAME = "table.feather"

FILE_BUNDLE_IMPORT_AVAILABLE_COLUMNS = [
    "id",
    "rel_path",
    "orig_filename",
    "orig_path",
    "import_time",
    "mime_type",
    "size",
    "content",
    "file_name",
]


class SaveArrowTableConfig(SaveValueModuleConfig):

    compression: str = Field(
        description="The compression to use when saving the table.", default="zstd"
    )


class SaveArrowTable(SaveValueTypeModule):

    _config_cls = SaveArrowTableConfig
    _module_type_name = "save"

    @classmethod
    def retrieve_supported_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        return "table"

    def save_value(self, value: Value, base_path: str) -> typing.Dict[str, typing.Any]:

        import pyarrow as pa
        from pyarrow import feather

        table: pa.Table = value.get_value_data()
        full_path: str = os.path.join(base_path, DEFAULT_SAVE_TABLE_FILE_NAME)

        if os.path.exists(full_path):
            raise KiaraProcessingException(
                f"Can't save table, file already exists: {full_path}"
            )

        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        compression = self.get_config_value("compression")

        feather.write_feather(table, full_path, compression=compression)

        result = {
            "module_type": "table.load",
            "base_path_input_name": "base_path",
            "inputs": {
                "base_path": os.path.dirname(full_path),
                "rel_path": os.path.basename(full_path),
                "format": "feather",
            },
            "output_name": "table",
        }
        return result


class LoadArrowTable(KiaraModule):
    """Load a table object from disk."""

    _module_type_name = "load"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Mapping[str, typing.Any] = {
            "base_path": {
                "type": "string",
                "doc": "The path to the folder that contains the table file.",
            },
            "rel_path": {
                "type": "string",
                "doc": "The relative path to the table file within base_path.",
            },
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

        from pyarrow import feather

        base_path = inputs.get_value_data("base_path")
        rel_path = inputs.get_value_data("rel_path")
        format = inputs.get_value_data("format")

        if format != "feather":
            raise NotImplementedError()

        path = os.path.join(base_path, rel_path)

        table = feather.read_table(path)
        outputs.set_value("table", table)


# class ImportArrowTable(DataImportModule):
#     """Onboard tabular data as an Arrow table.
#
#     """
#     @classmethod
#     def retrieve_supported_value_type(cls) -> str:
#         return "table"
#
#     def import_from_file_path__string(
#         self, source: str, base_aliases: typing.List[str]
#     ) -> FileMetadata:
#
#         op = self._kiara.operation_mgmt.profiles["file.import_from.path.string"]
#         aliases = [f"{x}_table_source_file" for x in base_aliases]
#         result = op.module.run(source=source, aliases=aliases)
#
#         file_value = result.get_value_obj("value_item")
#
#         op_convert = self._kiara.operation_mgmt.profiles["file.convert_to.table"]
#         result_table = op_convert.module.run(value_item=file_value)
#
#         return result_table.get_value_data("value_item")
#
#     def import_from_folder_path__string(
#         self, source: str, base_aliases: typing.List[str]
#     ) -> FileMetadata:
#
#         op = self._kiara.operation_mgmt.profiles["file_bundle.import_from.path.string"]
#         aliases = [f"{x}_table_source_file_bundle" for x in base_aliases]
#         result = op.module.run(source=source, aliases=aliases)
#
#         file_value = result.get_value_obj("value_item")
#
#         op_convert = self._kiara.operation_mgmt.profiles["file_bundle.convert_to.table"]
#         result_table = op_convert.module.run(value_item=file_value)
#
#         return result_table.get_value_data("value_item")


class ExportArrowTable(KiaraModule):
    """Export a table object to disk."""

    _module_type_name = "export"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Mapping[str, typing.Any] = {
            "table": {"type": "table", "doc": "The table object."},
            "path": {
                "type": "string",
                "doc": "The path to the file to write.",
            },
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
            "compression": {
                "type": "string",
                "doc": "The compression to use. Use either: 'zstd' (default), 'lz4', or 'uncompressed'.",
                "default": "zstd",
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

        import pyarrow as pa
        from pyarrow import feather

        table: pa.Table = inputs.get_value_data("table")
        full_path: str = inputs.get_value_data("path")
        force_overwrite = inputs.get_value_data("force_overwrite")
        format: str = inputs.get_value_data("format")
        compression = inputs.get_value_data("compression")

        if compression not in ["zstd", "lz4", "uncompressed"]:
            raise KiaraProcessingException(
                f"Invalid compression format '{compression}'. Allowed: 'zstd', 'lz4', 'uncompressed'."
            )

        if format != "feather":
            raise KiaraProcessingException(
                f"Can't export table to format '{format}': only 'feather' supported at the moment."
            )

        if os.path.exists(full_path) and not force_overwrite:
            raise KiaraProcessingException(
                f"Can't write table to file, file already exists: {full_path}"
            )

        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        feather.write_feather(table, full_path, compression=compression)

        result = {
            "module_type": "table.load",
            "base_path_input_name": "base_path",
            "inputs": {
                "base_path": os.path.dirname(full_path),
                "rel_path": os.path.basename(full_path),
                "format": format,
            },
            "value_id": NO_VALUE_ID_MARKER,
            "output_name": "table",
        }
        outputs.set_value("load_config", result)


class MergeTableModuleConfig(ModuleTypeConfigSchema):

    input_schema: typing.Dict[str, typing.Any] = Field(
        description="A dict describing the inputs for this merge process."
    )


class MergeTableModule(KiaraModule):
    """Create a table from other tables and/or arrays."""

    _module_type_name = "merge"
    _config_cls = MergeTableModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        input_schema_dict = self.get_config_value("input_schema")
        return input_schema_dict

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

        import pyarrow as pa

        input_schema: typing.Dict[str, typing.Any] = self.get_config_value(
            "input_schema"
        )

        sources = {}
        for field_name in input_schema.keys():
            sources[field_name] = inputs.get_value_data(field_name)

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

        import pyarrow as pa

        table: pa.Table = value.get_value_data()
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

        import pyarrow as pa

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


class MapColumnsModuleConfig(ModuleTypeConfigSchema):

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
        type_md = m.get_type_metadata()
        doc = type_md.documentation.full_doc
        link = type_md.context.get_url_for_reference("module_doc")
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

        import pyarrow as pa

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


class TableConversionModule(ConvertValueModule):
    """Convert an Arrow table.

    This module supportes two conversion targets currently:

     - bytes: a memoryview of the byte-representation of the Table
     - string: the base64-encoded byte-representation of the Table
    """

    _module_type_name = "convert"

    @classmethod
    def _get_supported_value_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        return "table"

    def to_bytes(self, value: Value) -> bytes:

        import pyarrow as pa

        table_val: Value = value
        table: pa.Table = table_val.get_value_data()

        batches = table.to_batches()

        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, batches[0].schema)

        for batch in batches:
            writer.write_batch(batch)
        writer.close()

        buf: pa.Buffer = sink.getvalue()
        return memoryview(buf)

    def to_string(self, value: Value):

        _bytes: bytes = self.to_bytes(value)
        string = base64.b64encode(_bytes)
        return string.decode()

    def from_bytes(self, value: Value):
        raise NotImplementedError()

    def from_string(self, value: Value):
        raise NotImplementedError()

    def from_file(self, value: Value):

        from pyarrow import csv

        input_file: FileMetadata = value.get_value_data()
        imported_data = csv.read_csv(input_file.path)
        return imported_data

    def from_file_bundle(self, value: Value):

        import pyarrow as pa

        bundle: FileBundleMetadata = value.get_value_data()

        columns = FILE_BUNDLE_IMPORT_AVAILABLE_COLUMNS

        file_dict = bundle.read_text_file_contents()

        tabular: typing.Dict[str, typing.List[typing.Any]] = {}
        for column in columns:
            for index, rel_path in enumerate(sorted(file_dict.keys())):

                if column == "content":
                    _value: typing.Any = file_dict[rel_path]
                elif column == "id":
                    _value = index
                elif column == "rel_path":
                    _value = rel_path
                else:
                    file_model = bundle.included_files[rel_path]
                    _value = getattr(file_model, column)

                tabular.setdefault(column, []).append(_value)

        table = pa.Table.from_pydict(tabular)
        return table


class SampleTableModule(KiaraModule):
    """Sample a table.

    Samples are used to randomly select a subset of a dataset, which helps test queries and workflows on smaller versions
    of the original data, to adjust parameters before a full run.
    """

    _module_type_name = "sample"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "table": {"type": "table", "doc": "The table to sample data from."},
            "sample_size": {
                "type": "integer",
                "doc": "The percentage or number of rows to sample (depending on 'sample_unit' input).",
            },
            "sample_unit": {
                "type": "string",
                "doc": "The sample size unit ('percent' or 'rows'), defaults to 'percent'.",
                "default": "percent",
            },
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {"sampled_table": {"type": "table", "doc": "A sampled table."}}

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        import duckdb
        import pyarrow as pa

        table: pa.Table = inputs.get_value_data("table")
        sample_size: int = inputs.get_value_data("sample_size")
        sample_unit: str = inputs.get_value_data("sample_unit")

        if sample_size < 0:
            raise KiaraProcessingException(
                f"Invalid sample size '{sample_size}': can't be negative."
            )

        if sample_unit == "percent":
            if sample_size > 100:
                sample_size = 100
        elif not sample_unit == "rows":
            raise KiaraProcessingException(
                f"Invalid sample unit '{sample_unit}': must be 'percent' or 'rows'."
            )

        query = "SELECT * FROM data USING SAMPLE "
        if sample_unit == "percent":
            query = query + f"{sample_size} PERCENT (bernoulli);"
        else:
            query = query + str(sample_size) + ";"

        relation: duckdb.DuckDBPyRelation = duckdb.arrow(table)
        result: duckdb.DuckDBPyResult = relation.query("data", query)

        outputs.set_value("sampled_table", result.arrow())


# class PrettyPrintTableModule(PrettyPrintValueModule):
#
#     _module_type_name = "pretty_print"
#
#     @classmethod
#     def retrieve_supported_source_types(cls) -> typing.Union[str, typing.Iterable[str]]:
#
#         return ["table"]
#
#     @classmethod
#     def retrieve_supported_target_types(cls) -> typing.Union[str, typing.Iterable[str]]:
#
#         return ["renderables"]
#
#     def pretty_print(
#         self,
#         value: Value,
#         value_type: str,
#         target_type: str,
#         print_config: typing.Mapping[str, typing.Any],
#     ) -> typing.Dict[str, typing.Any]:
#
#         result = None
#         if value_type == "table":
#             if target_type == "renderables":
#                 result = self.pretty_print_table_as_renderables(
#                     value=value, print_config=print_config
#                 )
#
#         if result is None:
#             raise Exception(
#                 f"Pretty printing of type '{value_type}' as '{target_type}' not supported."
#             )
#         return result
#
#     def pretty_print_table_as_renderables(
#         self, value: Value, print_config: typing.Mapping[str, typing.Any]
#     ):
#
#         max_rows = print_config.get("max_no_rows")
#         max_row_height = print_config.get("max_row_height")
#         max_cell_length = print_config.get("max_cell_length")
#
#         half_lines: typing.Optional[int] = None
#         if max_rows:
#             half_lines = int(max_rows / 2)
#
#         result = [
#             pretty_print_arrow_table(
#                 value.get_value_data(),
#                 rows_head=half_lines,
#                 rows_tail=half_lines,
#                 max_row_height=max_row_height,
#                 max_cell_length=max_cell_length,
#             )
#         ]
#         return result
