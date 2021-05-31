# -*- coding: utf-8 -*-
import json
import typing

from kiara import Kiara
from kiara.data.types import ValueType
from kiara.data.values import Value
from kiara.modules.type_conversion import TypeConversionModule

from kiara_modules.core.python import convert_to_py_obj


class JsonType(ValueType):

    pass


JSON_SUPPORTED_SOURCE_TYPES: typing.Iterable[str] = ["value_set", "table"]


def convert_to_json(
    kiara: Kiara,
    data: typing.Any,
    convert_config: typing.Mapping[str, typing.Any],
    data_type: typing.Optional[str] = None,
) -> str:

    if data_type:
        value_type_name = data_type
    else:
        if isinstance(data, Value):
            value_type_name = data.value_schema.type
        else:
            _value_type = kiara.determine_type(data)
            value_type_name = _value_type.type_name()

        if not value_type_name:
            value_type_name = "any"

    if value_type_name == "value_set":
        result = {}
        for field_name, value in data.items():
            _data = value.get_value_data()
            obj = convert_to_py_obj(
                kiara=kiara, data=_data, convert_config=convert_config
            )
            result[field_name] = obj
        return convert_to_json(
            kiara=kiara, data=result, convert_config=convert_config, data_type="dict"
        )
    elif value_type_name == "dict":

        indent = convert_config.get("indent", None)
        sort_keys = convert_config.get("sort_keys", None)

        json_str = json.dumps(data, indent=indent, sort_keys=sort_keys)
        return json_str
    else:
        raise Exception(
            f"Can't convert data into json, value type '{value_type_name}' not supported."
        )


DEFAULT_TO_JSON_CONFIG: typing.Mapping[str, typing.Any] = {
    "indent": 2,
}


class ToJsonModule(TypeConversionModule):
    @classmethod
    def _get_supported_source_types(self) -> typing.Union[typing.Iterable[str], str]:
        return JSON_SUPPORTED_SOURCE_TYPES

    @classmethod
    def _get_target_types(self) -> typing.Union[typing.Iterable[str], str]:
        return ["json"]

    def convert(
        self, value: Value, config: typing.Mapping[str, typing.Any]
    ) -> typing.Any:

        input_value: typing.Any = value.get_value_data()

        input_value_str = convert_to_json(
            self._kiara, data=input_value, convert_config=config
        )
        return input_value_str
