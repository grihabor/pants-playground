from database_schema_plugin.target_types import TableTargetGenerator, TableTarget
from database_schema_plugin.target_types import rules as target_types_rules


def target_types():
    return [TableTarget, TableTargetGenerator]


def rules():
    return [
        *target_types_rules(),
    ]
