# -*- coding: utf-8 -*-
import typing
from pathlib import Path

from kiara.data import Value
from kiara.operations.save_value import SaveValueTypeModule

from kiara_modules.core.generic import JsonSerializationConfig


class SaveDictModule(SaveValueTypeModule):

    _config_cls = JsonSerializationConfig
    _module_type_name = "save"

    @classmethod
    def retrieve_supported_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        return "dict"

    def save_value(self, value: Value, base_path: str) -> typing.Dict[str, typing.Any]:

        import orjson

        options = self.get_config_value("options")
        file_name = self.get_config_value("file_name")
        json_str = orjson.dumps(value.get_value_data(), option=options)

        bp = Path(base_path)
        bp.mkdir(parents=True, exist_ok=True)

        full_path = bp / file_name
        full_path.write_bytes(json_str)

        load_config = {
            "module_type": "generic.load_from_json",
            "base_path_input_name": "base_path",
            "inputs": {
                "base_path": base_path,
                "file_name": self.get_config_value("file_name"),
            },
            "output_name": "value_item",
        }

        return load_config
