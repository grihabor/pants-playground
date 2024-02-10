import ast
import logging
from dataclasses import dataclass
from typing import Any

from pants.engine.fs import Digest, DigestContents
from pants.engine.rules import Get, collect_rules, rule
from pants.engine.target import (
    COMMON_TARGET_FIELDS,
    GeneratedTargets,
    GenerateTargetsRequest,
    HydratedSources,
    HydrateSourcesRequest,
    IntField,
    SingleSourceField,
    StringField,
    Target,
    TargetGenerator,
)
from pants.engine.unions import UnionRule

logger = logging.getLogger(__name__)


class TableSourceField(SingleSourceField):
    required = True


class TableLinenoField(IntField):
    alias = "lineno"


class TableEndLinenoField(IntField):
    alias = "end_lineno"


class TableNameField(StringField):
    alias = "table"


class TableTarget(Target):
    alias = "table"
    core_fields = (
        TableSourceField,
        TableNameField,
        TableLinenoField,
        TableEndLinenoField,
    )


class TableTargetGenerator(TargetGenerator):
    alias = "tables"
    generated_target_cls = TableTarget
    core_fields = (
        *COMMON_TARGET_FIELDS,
        TableSourceField,
    )
    copied_fields = (
        *COMMON_TARGET_FIELDS,
        TableSourceField,
    )
    moved_fields = ()


class GenerateTableTargetsRequest(GenerateTargetsRequest):
    generate_from = TableTargetGenerator


@dataclass
class Table:
    table: str
    lineno: int
    end_lineno: int


class TableVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        super().__init__()
        self._tables = []

    def visit_Dict(self, node: ast.Dict) -> Any:
        for key, val in zip(node.keys, node.values):
            if (
                isinstance(key, ast.Str)
                and key.value == "table"
                and isinstance(val, ast.Str)
            ):
                assert node.end_lineno is not None
                self._tables.append(Table(val.value, node.lineno, node.end_lineno))


@rule
async def generate_table_targets(
    request: GenerateTableTargetsRequest,
) -> GeneratedTargets:
    hydrated_sources = await Get(
        HydratedSources, HydrateSourcesRequest(request.generator[TableSourceField])
    )
    logger.debug("table sources: %s", hydrated_sources)
    digest_files = await Get(DigestContents, Digest, hydrated_sources.snapshot.digest)
    digest_file = digest_files[0]
    content = digest_file.content.decode("utf-8")
    parsed = ast.parse(source=content, filename=digest_file.path)
    v = TableVisitor()
    v.visit(parsed)
    tables = v._tables
    logger.debug("parsed tables: %s", tables)
    return GeneratedTargets(
        request.generator,
        [
            TableTarget(
                {
                    **request.template,
                    "table": table.table,
                    "lineno": table.lineno,
                    "end_lineno": table.end_lineno,
                },
                request.template_address.create_generated(table.table),
            )
            for table in tables
        ],
    )


def rules():
    return (
        *collect_rules(),
        UnionRule(GenerateTargetsRequest, GenerateTableTargetsRequest),
    )
