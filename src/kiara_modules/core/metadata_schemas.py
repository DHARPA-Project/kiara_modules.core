# -*- coding: utf-8 -*-

"""This module contains the metadata models that are used in the ``kiara_modules.core`` package.

Metadata models are convenience wrappers that make it easier for *kiara* to find, create, manage and version metadata that
is attached to data, as well as *kiara* modules. It is possible to register metadata using a JSON schema string, but
it is recommended to create a metadata model, because it is much easier overall.

Metadata models must be a sub-class of [kiara.metadata.MetadataModel][kiara.metadata.MetadataModel].
"""


import datetime
import hashlib
import logging
import os.path
import shutil
import typing

from anyio import create_task_group, open_file, start_blocking_portal
from kiara import KiaraEntryPointItem
from kiara.metadata import MetadataModel
from kiara.utils.class_loading import find_metadata_schemas_under
from pydantic import BaseModel, Field, PrivateAttr

metadata_schemas: KiaraEntryPointItem = (
    find_metadata_schemas_under,
    ["kiara_modules.core.metadata_schemas"],
)


class ColumnSchema(BaseModel):
    """Describes properties of a single column of the 'table' data type."""

    _metadata_key: typing.ClassVar[str] = "column"

    arrow_type_name: str = Field(description="The arrow type name of the column.")
    arrow_type_id: int = Field(description="The arrow type id of the column.")
    metadata: typing.Dict[str, typing.Any] = Field(
        description="Other metadata for the column.", default_factory=dict
    )


class TableMetadata(MetadataModel):
    """Describes properties for the 'table' data type."""

    _metadata_key: typing.ClassVar[str] = "table"

    column_names: typing.List[str] = Field(
        description="The name of the columns of the table."
    )
    column_schema: typing.Dict[str, ColumnSchema] = Field(
        description="The schema description of the table."
    )
    rows: int = Field(description="The number of rows the table contains.")
    size: int = Field(description="The tables size in bytes.")


class ArrayMetadata(MetadataModel):
    """Describes properties fo the 'array' type."""

    _metadata_key: typing.ClassVar[str] = "array"

    length: int = Field(description="The number of elements the array contains.")
    size: int = Field(
        description="Total number of bytes consumed by the elements of the array."
    )


log = logging.getLogger("kiara")


class FileMetadata(MetadataModel):
    """Describes properties for the 'file' value type."""

    @classmethod
    def load_file(cls, source: str, target: typing.Optional[str] = None):
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
        orig_path = os.path.abspath(source)
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

        m = FileMetadata(
            orig_filename=orig_filename,
            orig_path=orig_path,
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
    orig_path: str = Field(
        description="The original path to this file at the time of import."
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

    def copy_file(self, target: str):

        fm = FileMetadata.load_file(self.path, target)
        fm.orig_path = self.orig_path
        fm.orig_filename = self.orig_filename
        fm.import_time = self.import_time
        if self._file_hash is not None:
            fm._file_hash = self._file_hash

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

    def __repr__(self):
        return f"FileMetadata(name={self.file_name})"

    def __str__(self):
        return self.__repr__()


class FolderImportConfig(BaseModel):

    exclude_dirs: typing.Optional[typing.List[str]] = Field(
        description="A list of strings, exclude all folders whose name ends with that string.",
        default=None,
    )
    include_files: typing.Optional[typing.List[str]] = Field(
        description="A list of strings, include all files where the filename ends with that string.",
        default=None,
    )


class FileBundleMetadata(MetadataModel):
    """Describes properties for the 'file_bundle' value type."""

    @classmethod
    def import_folder(
        cls,
        source: str,
        target: typing.Optional[str] = None,
        import_config: typing.Union[
            None, typing.Mapping[str, typing.Any], FolderImportConfig
        ] = None,
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

        included_files: typing.Dict[str, FileMetadata] = {}
        exclude_dirs = _import_config.exclude_dirs
        valid_extensions = _import_config.include_files

        sum_size = 0

        for root, dirnames, filenames in os.walk(source, topdown=True):

            if exclude_dirs:
                dirnames[:] = [d for d in dirnames if d not in exclude_dirs]

            for filename in [
                f
                for f in filenames
                if os.path.isfile(os.path.join(root, f))
                and (
                    not valid_extensions
                    or any(f.endswith(ext) for ext in valid_extensions)
                )
            ]:

                full_path = os.path.join(root, filename)
                rel_path = os.path.relpath(full_path, source)
                if target:
                    target_path: typing.Optional[str] = os.path.join(target, rel_path)
                else:
                    target_path = None

                file_model = FileMetadata.load_file(full_path, target_path)
                sum_size = sum_size + file_model.size
                included_files[rel_path] = file_model

        orig_bundle_name = os.path.basename(source)
        orig_path = source
        if target:
            path = target
        else:
            path = source

        return FileBundleMetadata.create_from_file_models(
            files=included_files,
            orig_bundle_name=orig_bundle_name,
            orig_path=orig_path,
            path=path,
            sum_size=sum_size,
        )

    @classmethod
    def create_from_file_models(
        self,
        files: typing.Mapping[str, FileMetadata],
        orig_bundle_name: str,
        orig_path: typing.Optional[str],
        path: str,
        sum_size: typing.Optional[int] = None,
    ):

        result: typing.Dict[str, typing.Any] = {}

        result["included_files"] = files

        result["orig_path"] = orig_path
        result["path"] = path
        result["import_time"] = datetime.datetime.now().isoformat()
        result["number_of_files"] = len(files)
        result["bundle_name"] = os.path.basename(result["path"])
        result["orig_bundle_name"] = orig_bundle_name

        if sum_size is None:
            sum_size = 0
            for f in files.values():
                sum_size = sum_size + f.size
        result["size"] = sum_size

        return FileBundleMetadata(**result)

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
    included_files: typing.Dict[str, FileMetadata] = Field(
        description="A map of all the included files, incl. their properties."
    )
    size: int = Field(description="The size of all files in this folder, combined.")
    path: str = Field(description="The archive path of the folder.")
    is_onboarded: bool = Field(
        description="Whether this bundle is imported into the kiara data store.",
        default=False,
    )

    def get_relative_path(self, file: FileMetadata):

        return os.path.relpath(file.path, self.path)

    def read_text_file_contents(self) -> typing.Mapping[str, str]:

        content_dict: typing.Dict[str, str] = {}

        with start_blocking_portal() as portal:

            async def read_file(rel_path: str, fm: FileMetadata):
                async with await open_file(fm.path) as f:
                    content = await f.read()
                    content_dict[rel_path] = content  # type: ignore

            async def read_files():

                async with create_task_group() as tg:
                    for f in self.included_files.values():
                        rel_path = self.get_relative_path(f)
                        tg.start_soon(read_file, rel_path, f)

            portal.call(read_files)

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

    def copy_bundle(self, target_path: str) -> "FileBundleMetadata":

        if target_path == self.path:
            raise Exception(f"Target path and current path are the same: {target_path}")

        result = {}
        for rel_path, item in self.included_files.items():
            _target_path = os.path.join(target_path, rel_path)
            new_fm = item.copy_file(_target_path)
            result[rel_path] = new_fm

        fb = FileBundleMetadata.create_from_file_models(
            result,
            orig_bundle_name=self.orig_bundle_name,
            orig_path=self.orig_path,
            path=target_path,
            sum_size=self.size,
        )
        if self._file_bundle_hash is not None:
            fb._file_bundle_hash = self._file_bundle_hash

        return fb

    def __repr__(self):
        return f"FileBundle(name={self.bundle_name})"

    def __str__(self):
        return self.__repr__()
