from database_schema_plugin.target_types import PythonConstantTarget, PythonConstantTargetGenerator
from database_schema_plugin.target_types import rules as target_types_rules
from pants.backend.python.dependency_inference.module_mapper import (
    rules as module_mapper_rules,
)


def target_types():
    return [PythonConstantTarget, PythonConstantTargetGenerator]


def rules():
    return [
        *target_types_rules(),
        *module_mapper_rules(),
    ]
