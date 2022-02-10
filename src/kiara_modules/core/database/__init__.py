# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import atexit
import os
import shutil
import tempfile
import typing

from kiara import KiaraModule
from kiara.data import Value, ValueSet
from kiara.data.values import ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.operations.create_value import CreateValueModule, CreateValueModuleConfig
from kiara.operations.extract_metadata import ExtractMetadataModule
from kiara.operations.store_value import StoreValueTypeModule
from kiara.utils import find_free_id, log_message
from pydantic import BaseModel, Field
from sqlalchemy.engine import Inspector

from kiara_modules.core.database.utils import create_sqlite_table_from_file
from kiara_modules.core.metadata_schemas import (
    ColumnSchema,
    KiaraDatabase,
    KiaraDatabaseInfo,
    KiaraFile,
    KiaraFileBundle,
    TableMetadata,
)

DEFAULT_DATABASE_SAVE_FILE_NAME = "db.sqlite"


class DatabaseConversionModuleConfig(CreateValueModuleConfig):

    ignore_errors: bool = Field(
        description="Whether to ignore convert errors and omit the failed items.",
        default=False,
    )


class ConvertToDatabaseModule(CreateValueModule):
    """Create a database from files, file_bundles, etc."""

    _module_type_name = "create"
    _config_cls = DatabaseConversionModuleConfig

    @classmethod
    def get_target_value_type(cls) -> str:
        return "database"

    def from_csv_file(self, value: Value):

        f = tempfile.mkdtemp()
        db_path = os.path.join(f, "db.sqlite")

        def cleanup():
            shutil.rmtree(f, ignore_errors=True)

        atexit.register(cleanup)

        create_sqlite_table_from_file(
            target_db_file=db_path, file_item=value.get_value_data()
        )

        return db_path

    def from_csv_file_bundle(self, value: Value):

        include_file_information: bool = True
        include_raw_content_in_file_info: bool = False

        temp_f = tempfile.mkdtemp()
        db_path = os.path.join(temp_f, "db.sqlite")

        def cleanup():
            shutil.rmtree(db_path, ignore_errors=True)

        atexit.register(cleanup)

        db = KiaraDatabase(db_file_path=db_path)
        db.create_if_not_exists()

        bundle: KiaraFileBundle = value.get_value_data()
        table_names: typing.List[str] = []
        for rel_path in sorted(bundle.included_files.keys()):
            file_item = bundle.included_files[rel_path]
            table_name = find_free_id(
                stem=file_item.file_name_without_extension, current_ids=table_names
            )
            try:
                table_names.append(table_name)
                create_sqlite_table_from_file(
                    target_db_file=db_path, file_item=file_item, table_name=table_name
                )
            except Exception as e:
                if self.get_config_value("ignore_errors") is True or True:
                    log_message(
                        f"Ignoring file '{rel_path}': could not import data from file -- {e}"
                    )
                    continue
                raise KiaraProcessingException(e)

        if include_file_information:
            create_table_from_file_bundle(
                file_bundle=value.get_value_data(),
                db_file_path=db_path,
                table_name="source_files_metadata",
                include_content=include_raw_content_in_file_info,
            )

        return db_path

    def from_text_file_bundle(self, value: Value):

        return create_table_from_file_bundle(
            file_bundle=value.get_value_data(), include_content=True
        )


def create_table_from_file_bundle(
    file_bundle: KiaraFileBundle,
    db_file_path: str = None,
    table_name: str = "file_items",
    include_content: bool = True,
) -> str:

    from sqlalchemy import (
        Column,
        DateTime,
        Integer,
        MetaData,
        String,
        Table,
        Text,
        insert,
    )
    from sqlalchemy.engine import Engine

    if db_file_path is None:
        temp_f = tempfile.mkdtemp()
        db_file_path = os.path.join(temp_f, "db.sqlite")

        def cleanup():
            shutil.rmtree(db_file_path, ignore_errors=True)

        atexit.register(cleanup)

    metadata_obj = MetaData()
    file_items = Table(
        table_name,
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("size", Integer(), nullable=False),
        Column("import_time", DateTime(), nullable=False),
        Column("mime_type", String(length=64), nullable=False),
        Column("rel_path", String(), nullable=False),
        Column("file_name", String(), nullable=False),
        Column("orig_filename", String(), nullable=False),
        Column("orig_path", String(), nullable=True),
        Column("content", Text(), nullable=not include_content),
    )

    db = KiaraDatabase(db_file_path=db_file_path)
    db.create_if_not_exists()

    engine: Engine = db.get_sqlalchemy_engine()
    metadata_obj.create_all(engine)

    with engine.connect() as con:

        for index, rel_path in enumerate(sorted(file_bundle.included_files.keys())):
            f: KiaraFile = file_bundle.included_files[rel_path]
            if not include_content:
                content: typing.Optional[str] = f.read_content(as_str=True)  # type: ignore
            else:
                content = None
            stmt = insert(file_items).values(
                id=index,
                size=f.size,
                import_time=f.import_time_as_datetime,
                mime_type=f.mime_type,
                rel_path=rel_path,
                file_name=f.file_name,
                orig_filename=f.orig_filename,
                orig_path=f.orig_path,
                content=content,
            )
            con.execute(stmt)
        con.commit()

    return db_file_path


class BaseDatabaseMetadataModule(ExtractMetadataModule):
    """Extract basic metadata from a database object."""

    def _get_metadata_schema(
        self, type: str
    ) -> typing.Union[str, typing.Type[BaseModel]]:
        return KiaraDatabase

    def extract_metadata(self, value: Value) -> KiaraDatabase:

        database: KiaraDatabase = value.get_value_data()
        return database


class DatabaseMetadataModule(BaseDatabaseMetadataModule):
    """Extract basic metadata from a database object."""

    _module_type_name = "metadata"

    @classmethod
    def _get_supported_types(cls) -> str:
        return "database"

    @classmethod
    def get_metadata_key(cls) -> str:
        return "database"


class BaseDatabaseInfoMetadataModule(ExtractMetadataModule):
    """Extract extended metadata (like tables, schemas) from a database object."""

    def _get_metadata_schema(
        self, type: str
    ) -> typing.Union[str, typing.Type[BaseModel]]:
        return KiaraDatabase

    def extract_metadata(self, value: Value) -> BaseModel:

        from sqlalchemy import inspect, text

        database: KiaraDatabase = value.get_value_data()
        inspector: Inspector = inspect(database.get_sqlalchemy_engine())

        table_names = inspector.get_table_names()
        view_names = inspector.get_view_names()

        table_infos = {}
        for table in table_names:
            columns = inspector.get_columns(table_name=table)
            columns_info = {}
            with database.get_sqlalchemy_engine().connect() as con:
                result = con.execute(text(f"SELECT count(*) from {table}"))
                num_rows = result.fetchone()[0]
                try:
                    result = con.execute(
                        text(f'SELECT SUM("pgsize") FROM "dbstat" WHERE name="{table}"')
                    )
                    table_size = result.fetchone()[0]
                except Exception:
                    table_size = None

            for column in columns:
                column_name = column["name"]
                cs = ColumnSchema(
                    type_name=column["type"].__visit_name__,
                    metadata={
                        "is_primary_key": column["primary_key"] == 1,
                        "nullable": column["nullable"],
                    },
                )
                columns_info[column_name] = cs
            table_infos[table] = TableMetadata(
                column_names=list(columns_info.keys()),
                column_schema=columns_info,
                rows=num_rows,
                size=table_size,
            )

        file_stats = os.stat(database.db_file_path)
        size = file_stats.st_size

        kdi = KiaraDatabaseInfo(
            table_names=table_names,
            view_names=view_names,
            tables=table_infos,
            size=size,
        )
        return kdi


class DatabaseInfoMetadataModule(BaseDatabaseInfoMetadataModule):
    """Extract extended metadata (like tables, schemas) from a database object."""

    _module_type_name = "info"

    @classmethod
    def _get_supported_types(cls) -> str:
        return "database"

    @classmethod
    def get_metadata_key(cls) -> str:
        return "database_info"


class BaseStoreDatabaseTypeModule(StoreValueTypeModule):
    """Save an sqlite database to a file."""

    def store_value(self, value: Value, base_path: str):

        database: KiaraDatabase = value.get_value_data()

        path = os.path.join(base_path, DEFAULT_DATABASE_SAVE_FILE_NAME)
        if os.path.exists(path):
            raise KiaraProcessingException(
                f"Can't write file, path already exists: {path}"
            )

        new_db = database.copy_database_file(path)

        load_config = {
            "module_type": "database.load",
            "inputs": {
                "base_path": base_path,
                "rel_path": DEFAULT_DATABASE_SAVE_FILE_NAME,
            },
            "output_name": "database",
        }
        return (load_config, new_db)


class StoreDatabaseTypeModule(StoreValueTypeModule):
    """Save an sqlite database to a file."""

    _module_type_name = "store"

    @classmethod
    def retrieve_supported_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        return "database"


class LoadDatabaseModule(KiaraModule):

    _module_type_name = "load"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "base_path": {
                "type": "string",
                "doc": "The path to the base directory where the database file is stored.",
            },
            "rel_path": {
                "type": "string",
                "doc": "The relative path of the database file within the base directory.",
            },
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs: typing.Mapping[str, typing.Any] = {
            "database": {"type": "database", "doc": "The database value object."}
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        base_path = inputs.get_value_data("base_path")
        rel_path = inputs.get_value_data("rel_path")

        path = os.path.join(base_path, rel_path)

        outputs.set_value("database", path)
