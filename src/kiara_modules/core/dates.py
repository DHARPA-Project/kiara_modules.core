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
from kiara.exceptions import KiaraProcessingException


class ExtractDateModule(KiaraModule):
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

    At least one of the inputs 'earliest' or 'latest' must be set, it's allowed to set both, though.

    Return ``True`` if that's the case, otherwise ``False``.
    """

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

        if hasattr(d, "as_py"):
            d = d.as_py()

        if not earliest and not latest:
            raise KiaraProcessingException(
                "Can't process date range check: need at least one of 'earliest' or 'latest'"
            )
        elif earliest and latest:
            matches = earliest <= d <= latest
        elif earliest:
            matches = earliest <= d
        else:
            matches = d <= latest

        outputs.set_value("within_range", matches)
