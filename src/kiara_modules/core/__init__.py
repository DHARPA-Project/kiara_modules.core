# -*- coding: utf-8 -*-

"""Top-level package for kiara_modules.core."""


import logging
import os

from kiara import (
    KiaraEntryPointItem,
    find_kiara_modules_under,
    find_kiara_pipelines_under,
)

__author__ = """Markus Binsteiner"""
__email__ = "markus.binsteiner@uni.lu"

from kiara.utils.class_loading import (
    find_metadata_schemas_under,
    find_value_types_under,
)

log = logging.getLogger("kiara_modules")

metadata_schemas: KiaraEntryPointItem = (
    find_metadata_schemas_under,
    ["kiara_modules.core.metadata_schemas"],
)
modules: KiaraEntryPointItem = (find_kiara_modules_under, ["kiara_modules.core"])
pipelines: KiaraEntryPointItem = (find_kiara_pipelines_under, ["kiara_modules.core"])
value_types: KiaraEntryPointItem = (
    find_value_types_under,
    ["kiara_modules.core.value_types"],
)

module_package_metadata = {
    "description": "Core modules for kiara.",
    "references": {
        "homepage": {
            "desc": "The module package homepage.",
            "url": "https://github.com/DHARPA-Project/kiara_modules.core",
        },
        "documentation": {
            "desc": "The url for the module package documentation.",
            "url": "https://dharpa.org/kiara_modules.core/",
        },
    },
}


def get_version():
    from pkg_resources import DistributionNotFound, get_distribution

    try:
        # Change here if project is renamed and does not equal the package name
        dist_name = __name__
        __version__ = get_distribution(dist_name).version
    except DistributionNotFound:

        try:
            version_file = os.path.join(os.path.dirname(__file__), "version.txt")

            if os.path.exists(version_file):
                with open(version_file, encoding="utf-8") as vf:
                    __version__ = vf.read()
            else:
                __version__ = "unknown"

        except (Exception):
            pass

        if __version__ is None:
            __version__ = "unknown"

    return __version__
