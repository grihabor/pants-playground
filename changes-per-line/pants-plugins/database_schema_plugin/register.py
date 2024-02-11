from database_schema_plugin.changed import rules as changed_rules
from database_schema_plugin.target_types import (
    PythonConstantTarget,
    PythonConstantTargetGenerator,
)
from database_schema_plugin.target_types import rules as target_types_rules


def target_types():
    return [PythonConstantTarget, PythonConstantTargetGenerator]


def rules():
    return [
        *target_types_rules(),
        *changed_rules(),
    ]
