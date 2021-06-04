# -*- coding: utf-8 -*-

from kiara.module_config import KiaraModuleConfig
from pydantic import Field


class ImportLocalPathConfig(KiaraModuleConfig):

    source_is_immutable: bool = Field(
        description="Whether the data that lives in source path can be relied upon to not change, and always be available",
        default=False,
    )
