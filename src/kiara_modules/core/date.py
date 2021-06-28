# -*- coding: utf-8 -*-

"""A collection of date related modules.

Most of those are very bare-bones, not really dealing with more advanced (but very important) concepts like timezones
and resolution yet.
"""
import datetime
import re
import typing

from dateutil import parser
from kiara import KiaraModule
from kiara.data.values import ValueSchema, ValueSet

# flake8: noqa


class ExtractDateModule(KiaraModule):
    """Extract a date object from a string.

    This module is not really smart yet, currently it uses the following regex to extract a date (which might fail in a lot of cases):

        r"_(\d{4}-\d{2}-\d{2})_"

    """

    _module_type_name = "extract_from_string"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {"text": {"type": "string", "doc": "The input string."}}

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {
            "date": {"type": "date", "doc": "The date extracted from the input string."}
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        text = inputs.get_value_data("text")
        date_match = re.findall(r"_(\d{4}-\d{2}-\d{2})_", text)
        assert date_match

        d_obj = parser.parse(date_match[0])  # type: ignore

        outputs.set_value("date", d_obj)


class DateRangeCheckModule(KiaraModule):
    """Check whether a date falls within a specified date range.

    If none one of the inputs 'earliest' or 'latest' is set, this module will always return 'True'.

    Return ``True`` if that's the case, otherwise ``False``.
    """

    _module_type_name = "range_check"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        inputs: typing.Dict[str, typing.Dict[str, typing.Any]] = {
            "date": {"type": "date", "doc": "The date to check."},
            "earliest": {
                "type": "date",
                "doc": "The earliest date that is allowed.",
                "optional": True,
            },
            "latest": {
                "type": "date",
                "doc": "The latest date that is allowed.",
                "optional": True,
            },
        }

        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        outputs = {
            "within_range": {
                "type": "boolean",
                "doc": "A boolean indicating whether the provided date was within the allowed range ('true'), or not ('false')",
            }
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        d = inputs.get_value_data("date")
        earliest: typing.Optional[datetime.datetime] = inputs.get_value_data("earliest")
        latest: typing.Optional[datetime.datetime] = inputs.get_value_data("latest")

        if not earliest and not latest:
            outputs.set_value("within_range", True)
            return

        if hasattr(d, "as_py"):
            d = d.as_py()

        if isinstance(d, str):
            d = parser.parse(d)

        if earliest and latest:
            matches = earliest <= d <= latest
        elif earliest:
            matches = earliest <= d
        else:
            matches = d <= latest

        outputs.set_value("within_range", matches)
