# -*- coding: utf-8 -*-
from kiara.operations import OperationConfig, Operations

from kiara_modules.core.value import DataProfilerModule


class DataProfileOperations(Operations):
    def is_matching_operation(self, op_config: OperationConfig) -> bool:

        return issubclass(op_config.module_cls, DataProfilerModule)

    # def get_pretty_print_operation(self, value_type: str, target_type: str) -> OperationConfig:
    #
    #     result = []
    #     for op_config in self.operation_configs.values():
    #         if op_config.module_config["value_type"] != value_type:
    #             continue
    #         if op_config.module_config["target_type"] != target_type:
    #             continue
    #         result.append(op_config)
    #
    #     if not result:
    #         raise Exception(
    #             f"No pretty print operation for value type '{value_type}' and output '{target_type}' registered.")
    #     elif len(result) != 1:
    #         raise Exception(
    #             f"Multiple pretty print operations for value type '{value_type}' and output '{target_type}' registered.")
    #
    #     return result[0]
    #
    # def pretty_print(self, value: Value, target_type: str, print_config: typing.Optional[typing.Mapping[str, typing.Any]]=None) -> typing.Any:
    #
    #     ops_config = self.get_pretty_print_operation(value_type=value.type_name, target_type=target_type)
    #     result = ops_config.module.run(value_item=value, print_config=print_config)
    #     printed = result.get_value_data("printed")
    #
    #     return printed
