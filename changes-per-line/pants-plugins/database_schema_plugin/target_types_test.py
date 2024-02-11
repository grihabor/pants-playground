from textwrap import dedent

import pytest
from database_schema_plugin.target_types import PythonConstant, PythonConstantVisitor
from database_schema_plugin.target_types import rules as target_types_rules
from pants.testutil.rule_runner import RuleRunner


@pytest.fixture
def rule_runner():
    return RuleRunner(
        rules=[
            *target_types_rules(),
        ]
    )


def test_parse_python_constants():
    content = dedent(
        """
        int_constant = 42
        array_constant = [
            'pants',
            'is',
            'awesome',
        ]

        dict_constant = {
            'mercury': 2440,
            'venus': 6052,
            'earth': 6371,
        }
        """
    ).strip()
    constants = PythonConstantVisitor.parse_constants(content.encode("utf-8"))
    assert constants == [
        PythonConstant(python_contant="int_constant", lineno=1, end_lineno=1),
        PythonConstant(python_contant="array_constant", lineno=2, end_lineno=6),
        PythonConstant(python_contant="dict_constant", lineno=8, end_lineno=12),
    ]
