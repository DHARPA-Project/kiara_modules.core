# -*- coding: utf-8 -*-
import copy
import typing
from concurrent.futures import ThreadPoolExecutor

from kiara import KiaraModule

if typing.TYPE_CHECKING:
    import pyarrow as pa


def map_with_module(
    array: "pa.Array",
    module_input_name: str,
    module_obj: KiaraModule,
    init_data: typing.Mapping[str, typing.Any],
    module_output_name: typing.Optional[str] = None,
    attache_lineage_to_result: bool = False,
) -> typing.List[typing.Any]:

    # TODO: validate that the selected module is appropriate
    assert len(list(module_obj.output_names)) == 1

    if module_output_name is None:
        module_output_name = list(module_obj.output_names)[0]

    assert module_output_name in module_obj.output_names

    multi_threaded = False
    if multi_threaded:

        def run_module(item):
            _d = copy.copy(init_data)
            _d[module_input_name] = item
            r = module_obj.run(_attach_lineage=attache_lineage_to_result, **_d)
            return r.get_value_data(module_output_name)

        executor = ThreadPoolExecutor()
        results: typing.Any = executor.map(run_module, array)
        executor.shutdown(wait=True)

    else:
        results = []
        for item in array:
            _d = dict(init_data)
            _d[module_input_name] = item
            r = module_obj.run(_attach_lineage=attache_lineage_to_result, **_d)
            results.append(r.get_value_data(module_output_name))

    result_list = []
    result_types = set()
    for r in results:
        result_list.append(r)
        result_types.add(type(r))

    assert len(result_types) <= 1

    return result_list
