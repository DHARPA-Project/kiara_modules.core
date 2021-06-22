# -*- coding: utf-8 -*-

"""Virtual module that is used as base for [PipelineModule][kiara.pipeline.module.PipelineModule] classes that are auto-generated
from pipeline descriptions under this folder."""
from kiara import KiaraEntryPointItem, find_kiara_pipelines_under

pipelines: KiaraEntryPointItem = (
    find_kiara_pipelines_under,
    ["kiara_modules.core.pipelines"],
)
