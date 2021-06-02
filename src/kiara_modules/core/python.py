# -*- coding: utf-8 -*-
import typing

from kiara import Kiara
from kiara.data import Value
from pyarrow import Table

PY_OBJ_SUPPORTED_SOURCE_TYPES = ["value_set", "table"]


def convert_to_py_obj(
    kiara: Kiara,
    data: typing.Any,
    convert_config: typing.Mapping[str, typing.Any],
    data_type: typing.Optional[str] = None,
) -> typing.Any:

    if data_type:
        value_type_name: typing.Optional[str] = data_type
    else:
        value_type_name = None
        if isinstance(data, Value):
            value_type_name = data.value_schema.type
        else:
            _value_type = kiara.determine_type(data)
            if _value_type:
                value_type_name = _value_type.type_name()

        if not value_type_name:
            value_type_name = "any"

    if value_type_name == "table":
        t: Table = data
        py_dict = t.to_pydict()
        return py_dict
    else:
        raise Exception(
            f"Can't convert data into python object, value type '{value_type_name}' not supported."
        )
