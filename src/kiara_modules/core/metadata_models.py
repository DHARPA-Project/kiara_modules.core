# -*- coding: utf-8 -*-

"""This module contains the metadata models that are used in the ``kiara_modules.core`` package.

Metadata models are convenience wrappers that make it easier for *kiara* to find, create, manage and version metadata that
is attached to data, as well as *kiara* modules. It is possible to register metadata using a JSON schema string, but
it is recommended to create a metadata model, because it is much easier overall.

Metadata models must be a sub-class of [kiara.metadata.MetadataModel][kiara.metadata.MetadataModel].
"""
import atexit
import datetime
import hashlib
import logging
import os.path
import shutil
import tempfile
import typing

from kiara import KiaraEntryPointItem
from kiara.defaults import DEFAULT_EXCLUDE_FILES
from kiara.metadata import MetadataModel
from kiara.metadata.core_models import HashedMetadataModel
from kiara.utils import log_message
from kiara.utils.class_loading import find_metadata_models_under
from pydantic import BaseModel, Field, PrivateAttr, validator
from sqlalchemy import inspect

if typing.TYPE_CHECKING:
    from sqlalchemy.engine import Engine, Inspector

metadata_models: KiaraEntryPointItem = (
    find_metadata_models_under,
    ["kiara_modules.core.metadata_models"],
)


class ColumnSchema(BaseModel):
    """Describes properties of a single column of the 'table' data type."""

    _metadata_key: typing.ClassVar[str] = "column"

    type_name: str = Field(
        description="The type name of the column (backend-specific)."
    )
    metadata: typing.Dict[str, typing.Any] = Field(
        description="Other metadata for the column.", default_factory=dict
    )


class TableMetadata(HashedMetadataModel):
    """Describes properties for the 'table' data type."""

    _metadata_key: typing.ClassVar[str] = "table"

    column_names: typing.List[str] = Field(
        description="The name of the columns of the table."
    )
    column_schema: typing.Dict[str, ColumnSchema] = Field(
        description="The schema description of the table."
    )
    rows: int = Field(description="The number of rows the table contains.")
    size: typing.Optional[int] = Field(
        description="The tables size in bytes.", default=None
    )

    def _obj_to_hash(self) -> typing.Any:

        return {
            "column_names": self.column_names,
            "column_schemas": {k: v.dict() for k, v in self.column_schema.items()},
            "rows": self.rows,
            "size": self.size,
        }

    def get_category_alias(self) -> str:
        return "instance.metadata.table"


class ArrayMetadata(HashedMetadataModel):
    """Describes properties fo the 'array' type."""

    _metadata_key: typing.ClassVar[str] = "array"

    length: int = Field(description="The number of elements the array contains.")
    size: int = Field(
        description="Total number of bytes consumed by the elements of the array."
    )

    def _obj_to_hash(self) -> typing.Any:
        return {"length": self.length, "size": self.size}

    def get_category_alias(self) -> str:
        return "instance.metadata.array"


log = logging.getLogger("kiara")


class KiaraDatabase(MetadataModel):

    _metadata_key: typing.ClassVar[str] = "database"

    @classmethod
    def create_in_temp_dir(cls, init_sql: typing.Optional[str] = None):

        temp_f = tempfile.mkdtemp()
        db_path = os.path.join(temp_f, "db.sqlite")

        def cleanup():
            shutil.rmtree(db_path, ignore_errors=True)

        atexit.register(cleanup)

        db = cls(db_file_path=db_path)
        db.create_if_not_exists()

        if init_sql:
            db.execute_sql(sql_script=init_sql, invalidate=True)

        return db

    db_file_path: str = Field(description="The path to the sqlite database file.")
    _cached_engine = PrivateAttr(default=None)
    _cached_inspector = PrivateAttr(default=None)
    _table_names = PrivateAttr(default=None)
    _table_schemas = PrivateAttr(default=None)

    def get_id(self) -> str:
        return self.db_file_path

    def get_category_alias(self) -> str:
        return "instance.metadata.database"

    @validator("db_file_path", allow_reuse=True)
    def ensure_absolute_path(cls, path: str):

        path = os.path.abspath(path)
        if not os.path.exists(os.path.dirname(path)):
            raise ValueError(f"Parent folder for database file does not exist: {path}")
        return path

    @property
    def db_url(self) -> str:
        return f"sqlite:///{self.db_file_path}"

    def get_sqlalchemy_engine(self) -> "Engine":

        if self._cached_engine is not None:
            return self._cached_engine

        from sqlalchemy import create_engine

        self._cached_engine = create_engine(self.db_url, future=True)
        # with self._cached_engine.connect() as con:
        #     con.execute(text("PRAGMA query_only = ON"))

        return self._cached_engine

    def create_if_not_exists(self):

        from sqlalchemy_utils import create_database, database_exists

        if not database_exists(self.db_url):
            create_database(self.db_url)

    def execute_sql(self, sql_script: str, invalidate: bool = False):
        """Execute an sql script.

        Arguments:
          sql_script: the sql script
          invalidate: whether to invalidate cached values within this object
        """

        self.create_if_not_exists()
        conn = self.get_sqlalchemy_engine().raw_connection()
        cursor = conn.cursor()
        cursor.executescript(sql_script)
        conn.commit()
        conn.close()

        if invalidate:
            self._cached_inspector = None
            self._table_names = None
            self._table_schemas = None

    def copy_database_file(self, target: str):

        os.makedirs(os.path.dirname(target))

        shutil.copy2(self.db_file_path, target)

        new_db = KiaraDatabase(db_file_path=target)
        return new_db

    def get_sqlalchemy_inspector(self) -> "Inspector":

        if self._cached_inspector is not None:
            return self._cached_inspector

        self._cached_inspector = inspect(self.get_sqlalchemy_engine())
        return self._cached_inspector

    @property
    def table_names(self) -> typing.Iterable[str]:
        if self._table_names is not None:
            return self._table_names

        self._table_names = self.get_sqlalchemy_inspector().get_table_names()
        return self._table_names

    def get_schema_for_table(self, table_name: str):

        if self._table_schemas is not None:
            if table_name not in self._table_schemas.keys():
                raise Exception(
                    f"Can't get table schema, database does not contain table with name '{table_name}'."
                )
            return self._table_schemas[table_name]

        ts: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
        inspector = self.get_sqlalchemy_inspector()
        for tn in inspector.get_table_names():
            columns = self.get_sqlalchemy_inspector().get_columns(tn)
            ts[tn] = {}
            for c in columns:
                ts[tn][c["name"]] = c

        self._table_schemas = ts
        if table_name not in self._table_schemas.keys():
            raise Exception(
                f"Can't get table schema, database does not contain table with name '{table_name}'."
            )

        return self._table_schemas[table_name]


class KiaraDatabaseInfo(HashedMetadataModel):

    _metadata_key: typing.ClassVar[str] = "database_info"

    table_names: typing.List[str] = Field(
        description="The names of all tables in this database."
    )
    view_names: typing.List[str] = Field(
        description="The names of all views in this database."
    )
    tables: typing.Dict[str, TableMetadata] = Field(
        description="Information about the tables within this database."
    )
    size: int = Field(description="The size of the database file.")

    def _obj_to_hash(self) -> typing.Any:
        return {
            "table_names": self.table_names,
            "view_names": self.view_names,
            "tables": self.tables,
            "size": self.size,
        }

    def get_category_alias(self) -> str:
        return "instance.metadata.database_info"


class KiaraFile(MetadataModel):
    """Describes properties for the 'file' value type."""

    _metadata_key: typing.ClassVar[str] = "file"

    @classmethod
    def load_file(
        cls,
        source: str,
        target: typing.Optional[str] = None,
        incl_orig_path: bool = False,
    ):
        """Utility method to read metadata of a file from disk and optionally move it into a data archive location."""

        import mimetypes

        import filetype

        if not source:
            raise ValueError("No source path provided.")

        if not os.path.exists(os.path.realpath(source)):
            raise ValueError(f"Path does not exist: {source}")

        if not os.path.isfile(os.path.realpath(source)):
            raise ValueError(f"Path is not a file: {source}")

        orig_filename = os.path.basename(source)
        orig_path: str = os.path.abspath(source)
        file_import_time = datetime.datetime.now().isoformat()  # TODO: timezone

        file_stats = os.stat(orig_path)
        size = file_stats.st_size

        if target:
            if os.path.exists(target):
                raise ValueError(f"Target path exists: {target}")
            os.makedirs(os.path.dirname(target), exist_ok=True)
            shutil.copy2(source, target)
        else:
            target = orig_path

        r = mimetypes.guess_type(target)
        if r[0] is not None:
            mime_type = r[0]
        else:
            _mime_type = filetype.guess(target)
            if not _mime_type:
                mime_type = "application/octet-stream"
            else:
                mime_type = _mime_type.MIME

        if not incl_orig_path:
            _orig_path: typing.Optional[str] = None
        else:
            _orig_path = orig_path

        m = KiaraFile(
            orig_filename=orig_filename,
            orig_path=_orig_path,
            import_time=file_import_time,
            mime_type=mime_type,
            size=size,
            file_name=orig_filename,
            path=target,
        )
        return m

    _file_hash: typing.Optional[str] = PrivateAttr(default=None)

    orig_filename: str = Field(
        description="The original filename of this file at the time of import."
    )
    orig_path: typing.Optional[str] = Field(
        description="The original path to this file at the time of import.",
        default=None,
    )
    import_time: str = Field(description="The time when the file was imported.")
    mime_type: str = Field(description="The mime type of the file.")
    file_name: str = Field("The name of the file.")
    size: int = Field(description="The size of the file.")
    path: str = Field(description="The archive path of the file.")
    is_onboarded: bool = Field(
        description="Whether the file is imported into the kiara data store.",
        default=False,
    )

    def get_id(self) -> str:
        return self.path

    def get_category_alias(self) -> str:
        return "instance.metadata.file"

    def copy_file(
        self, target: str, incl_orig_path: bool = False, is_onboarded: bool = False
    ):

        fm = KiaraFile.load_file(self.path, target)
        if incl_orig_path:
            fm.orig_path = self.orig_path
        else:
            fm.orig_path = None
        fm.orig_filename = self.orig_filename
        fm.import_time = self.import_time
        if self._file_hash is not None:
            fm._file_hash = self._file_hash

        fm.is_onboarded = is_onboarded

        return fm

    @property
    def file_hash(self):

        if self._file_hash is not None:
            return self._file_hash

        sha256_hash = hashlib.sha3_256()
        with open(self.path, "rb") as f:
            # Read and update hash string value in blocks of 4K
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)

        self._file_hash = sha256_hash.hexdigest()
        return self._file_hash

    @property
    def file_name_without_extension(self) -> str:

        return self.file_name.split(".")[0]

    @property
    def import_time_as_datetime(self) -> datetime.datetime:
        from dateutil import parser

        return parser.parse(self.import_time)

    def read_content(
        self, as_str: bool = True, max_lines: int = -1
    ) -> typing.Union[str, bytes]:
        """Read the content of a file."""

        mode = "r" if as_str else "rb"

        with open(self.path, mode) as f:
            if max_lines <= 0:
                content = f.read()
            else:
                content = "".join((next(f) for x in range(max_lines)))
        return content

    def __repr__(self):
        return f"FileMetadata(name={self.file_name})"

    def __str__(self):
        return self.__repr__()


class FolderImportConfig(BaseModel):

    include_files: typing.Optional[typing.List[str]] = Field(
        description="A list of strings, include all files where the filename ends with that string.",
        default=None,
    )
    exclude_dirs: typing.Optional[typing.List[str]] = Field(
        description="A list of strings, exclude all folders whose name ends with that string.",
        default=None,
    )
    exclude_files: typing.Optional[typing.List[str]] = Field(
        description=f"A list of strings, exclude all files that match those (takes precedence over 'include_files'). Defaults to: {DEFAULT_EXCLUDE_FILES}.",
        default=DEFAULT_EXCLUDE_FILES,
    )


class KiaraFileBundle(MetadataModel):
    """Describes properties for the 'file_bundle' value type."""

    _metadata_key: typing.ClassVar[str] = "file_bundle"

    @classmethod
    def import_folder(
        cls,
        source: str,
        target: typing.Optional[str] = None,
        import_config: typing.Union[
            None, typing.Mapping[str, typing.Any], FolderImportConfig
        ] = None,
        incl_orig_path: bool = False,
    ):

        if not source:
            raise ValueError("No source path provided.")

        if not os.path.exists(os.path.realpath(source)):
            raise ValueError(f"Path does not exist: {source}")

        if not os.path.isdir(os.path.realpath(source)):
            raise ValueError(f"Path is not a file: {source}")

        if target and os.path.exists(target):
            raise ValueError(f"Target path already exists: {target}")

        if source.endswith(os.path.sep):
            source = source[0:-1]

        if target and target.endswith(os.path.sep):
            target = target[0:-1]

        if import_config is None:
            _import_config = FolderImportConfig()
        elif isinstance(import_config, typing.Mapping):
            _import_config = FolderImportConfig(**import_config)
        elif isinstance(import_config, FolderImportConfig):
            _import_config = import_config
        else:
            raise TypeError(
                f"Invalid type for folder import config: {type(import_config)}."
            )

        included_files: typing.Dict[str, KiaraFile] = {}
        exclude_dirs = _import_config.exclude_dirs
        invalid_extensions = _import_config.exclude_files

        valid_extensions = _import_config.include_files

        sum_size = 0

        def include_file(filename: str) -> bool:

            if invalid_extensions and any(
                filename.endswith(ext) for ext in invalid_extensions
            ):
                return False
            if not valid_extensions:
                return True
            else:
                return any(filename.endswith(ext) for ext in valid_extensions)

        for root, dirnames, filenames in os.walk(source, topdown=True):

            if exclude_dirs:
                dirnames[:] = [d for d in dirnames if d not in exclude_dirs]

            for filename in [
                f
                for f in filenames
                if os.path.isfile(os.path.join(root, f)) and include_file(f)
            ]:

                full_path = os.path.join(root, filename)
                rel_path = os.path.relpath(full_path, source)
                if target:
                    target_path: typing.Optional[str] = os.path.join(target, rel_path)
                else:
                    target_path = None

                file_model = KiaraFile.load_file(
                    full_path, target_path, incl_orig_path=incl_orig_path
                )
                sum_size = sum_size + file_model.size
                included_files[rel_path] = file_model

        orig_bundle_name = os.path.basename(source)
        if incl_orig_path:
            orig_path: typing.Optional[str] = source
        else:
            orig_path = None

        if target:
            path = target
        else:
            path = source

        return KiaraFileBundle.create_from_file_models(
            files=included_files,
            orig_bundle_name=orig_bundle_name,
            orig_path=orig_path,
            path=path,
            sum_size=sum_size,
        )

    @classmethod
    def create_from_file_models(
        self,
        files: typing.Mapping[str, KiaraFile],
        orig_bundle_name: str,
        orig_path: typing.Optional[str],
        path: str,
        sum_size: typing.Optional[int] = None,
        is_onboarded: bool = False,
    ):

        result: typing.Dict[str, typing.Any] = {}

        result["included_files"] = files

        result["orig_path"] = orig_path
        result["path"] = path
        result["import_time"] = datetime.datetime.now().isoformat()
        result["number_of_files"] = len(files)
        result["bundle_name"] = os.path.basename(result["path"])
        result["orig_bundle_name"] = orig_bundle_name
        result["is_onboarded"] = is_onboarded

        if sum_size is None:
            sum_size = 0
            for f in files.values():
                sum_size = sum_size + f.size
        result["size"] = sum_size

        return KiaraFileBundle(**result)

    _file_bundle_hash: typing.Optional[str] = PrivateAttr(default=None)

    orig_bundle_name: str = Field(
        description="The original name of this folder at the time of import."
    )
    bundle_name: str = Field(description="The name of this bundle.")
    orig_path: typing.Optional[str] = Field(
        description="The original path to this folder at the time of import.",
        default=None,
    )
    import_time: str = Field(description="The time when the file was imported.")
    number_of_files: int = Field(
        description="How many files are included in this bundle."
    )
    included_files: typing.Dict[str, KiaraFile] = Field(
        description="A map of all the included files, incl. their properties. Uses the relative path of each file as key."
    )
    size: int = Field(description="The size of all files in this folder, combined.")
    path: str = Field(description="The archive path of the folder.")
    is_onboarded: bool = Field(
        description="Whether this bundle is imported into the kiara data store.",
        default=False,
    )

    def get_id(self) -> str:
        return self.path

    def get_category_alias(self) -> str:
        return "instance.metadata.file_bundle"

    def get_relative_path(self, file: KiaraFile):

        return os.path.relpath(file.path, self.path)

    def read_text_file_contents(
        self, ignore_errors: bool = False
    ) -> typing.Mapping[str, str]:

        content_dict: typing.Dict[str, str] = {}

        def read_file(rel_path: str, fm: KiaraFile):
            with open(fm.path, encoding="utf-8") as f:
                try:
                    content = f.read()
                    content_dict[rel_path] = content  # type: ignore
                except Exception as e:
                    if ignore_errors:
                        log_message(f"Can't read file: {e}")
                        log.warning(f"Ignoring file: {fm.path}")
                    else:
                        raise Exception(f"Can't read file (as text) '{fm.path}: {e}")

        # TODO: common ignore files and folders
        for f in self.included_files.values():
            rel_path = self.get_relative_path(f)
            read_file(rel_path=rel_path, fm=f)

        return content_dict

    @property
    def file_bundle_hash(self):

        if self._file_bundle_hash is not None:
            return self._file_bundle_hash

        # hash_format ="sha3-256"

        hashes = ""
        for path in sorted(self.included_files.keys()):
            fm = self.included_files[path]
            hashes = hashes + "_" + path + "_" + fm.file_hash

        self._file_bundle_hash = hashlib.sha3_256(hashes.encode("utf-8")).hexdigest()
        return self._file_bundle_hash

    def copy_bundle(
        self, target_path: str, incl_orig_path: bool = False, is_onboarded: bool = False
    ) -> "KiaraFileBundle":

        if target_path == self.path:
            raise Exception(f"Target path and current path are the same: {target_path}")

        result = {}
        for rel_path, item in self.included_files.items():
            _target_path = os.path.join(target_path, rel_path)
            new_fm = item.copy_file(_target_path, is_onboarded=is_onboarded)
            result[rel_path] = new_fm

        if incl_orig_path:
            orig_path = self.orig_path
        else:
            orig_path = None
        fb = KiaraFileBundle.create_from_file_models(
            result,
            orig_bundle_name=self.orig_bundle_name,
            orig_path=orig_path,
            path=target_path,
            sum_size=self.size,
            is_onboarded=is_onboarded,
        )
        if self._file_bundle_hash is not None:
            fb._file_bundle_hash = self._file_bundle_hash

        return fb

    def __repr__(self):
        return f"FileBundle(name={self.bundle_name})"

    def __str__(self):
        return self.__repr__()
