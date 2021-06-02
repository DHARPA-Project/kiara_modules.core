# -*- coding: utf-8 -*-
import typing

import pyarrow as pa
from kiara.data.types import ValueType


class TableType(ValueType):
    @classmethod
    def transformation_configs(
        self,
    ) -> typing.Optional[typing.Mapping[str, typing.Mapping[str, typing.Any]]]:
        """Return a dictionary of configuration for modules that can transform this type.

        The name of the transformation is the key of the result dictionary, the configuration is a module configuration
        (dictionary wth 'module_type' and optional 'module_config', 'input_name' and 'output_name' keys).
        """
        return {
            "string": {
                "module_type": "strings.pretty_print",
                "input_name": "item",
                "defaults": {"max_cell_length": 240, "max_no_rows": 20},
            },
            "json": {"module_type": "json.to_json", "input_name": "item"},
        }

    @classmethod
    def python_types(cls) -> typing.Optional[typing.Iterable[typing.Type]]:
        return [pa.Table]

    @classmethod
    def save_config(cls) -> typing.Mapping[str, typing.Any]:

        return {
            "module_type": "tabular.write_table",
            "module_config": {
                "constants": {
                    "format": "feather",
                    "force_overwrite": False,
                    "file_name": "table.feather",
                }
            },
            "input_name": "table",
            "target_name": "folder_path",
            "load_config_output": "load_config",
        }

    @classmethod
    def check_data(cls, data: typing.Any) -> typing.Optional["ValueType"]:

        if isinstance(data, pa.Table):
            return TableType()

        return None

    # @classmethod
    # def generate_load_inputs(self, value_id: str, persistance_mgmt: PersistanceMgmt):
    #
    #     path = persistance_mgmt.get_path(value_id=value_id) / "table.feather"
    #     return {
    #         "path": path
    #     }

    # @classmethod
    # def generate_save_inputs(self, value: Value, persistance_mgmt: PersistanceMgmt):
    #
    #     path = persistance_mgmt.get_path(value_id=value.id) / "table.feather"
    #     return {
    #         "path": path
    #     }

    def validate(cls, value: typing.Any) -> None:
        assert isinstance(value, pa.Table)
