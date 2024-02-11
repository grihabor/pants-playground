import ast
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, DefaultDict, List, Tuple

from pants.backend.python.dependency_inference.module_mapper import (
    ResolveName,
)
from pants.backend.python.subsystems.setup import PythonSetup
from pants.backend.python.target_types import (
    PythonResolveField,
    PythonSourceField,
)
from pants.engine.addresses import Address, Addresses
from pants.engine.fs import Digest, DigestContents
from pants.engine.rules import Get, collect_rules, rule
from pants.engine.target import (
    COMMON_TARGET_FIELDS,
    AllTargets,
    Dependencies,
    FieldSet,
    GeneratedTargets,
    GenerateTargetsRequest,
    HydratedSources,
    HydrateSourcesRequest,
    InferDependenciesRequest,
    InferredDependencies,
    IntField,
    SingleSourceField,
    StringField,
    Target,
    TargetGenerator,
    Targets,
)
from pants.engine.unions import UnionRule
from pants.util.frozendict import FrozenDict
from pants.util.ordered_set import FrozenOrderedSet

logger = logging.getLogger(__name__)


class TableSourceField(SingleSourceField):
    required = True


class TableDependencies(Dependencies):
    pass


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
        TableDependencies,
    )


class TableTargetGenerator(TargetGenerator):
    alias = "tables"
    generated_target_cls = TableTarget
    core_fields = (
        *COMMON_TARGET_FIELDS,
        TableSourceField,
        TableDependencies,
    )
    copied_fields = (
        *COMMON_TARGET_FIELDS,
        TableSourceField,
    )
    moved_fields = (TableDependencies,)


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
    parsed = ast.parse(content, filename=digest_file.path)
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
                    TableNameField.alias: table.table,
                    TableLinenoField.alias: table.lineno,
                    TableEndLinenoField.alias: table.end_lineno,
                },
                request.template_address.create_generated(table.table),
            )
            for table in tables
        ],
    )


@dataclass(frozen=True)
class InferTableDependenciesFieldSet(FieldSet):
    required_fields = (PythonSourceField, PythonResolveField)

    source: PythonSourceField
    resolve: PythonResolveField


class InferTableDependenciesRequest(
    InferDependenciesRequest[InferTableDependenciesFieldSet]
):
    infer_from = InferTableDependenciesFieldSet


class ImportVisitor(ast.NodeVisitor):
    def __init__(self, search_for_modules: set[str]) -> None:
        super().__init__()
        self._search_for = search_for_modules
        self._found = set()

    def visit_ImportFrom(self, node: ast.ImportFrom) -> Any:
        for name in node.names:
            obj = f"{node.module}.{name}"
            if obj in self._search_for:
                self._found.add(obj)

    @classmethod
    def search_for_modules(cls, node: ast.AST, modules: set[str]) -> set[str]:
        v = cls(modules)
        v.visit(node)
        return v._found


@dataclass(frozen=True)
class AllTableTargets(Targets):
    pass


@rule
async def get_table_targets(targets: AllTargets) -> AllTableTargets:
    return AllTableTargets(
        target for target in targets if target.has_field(TableSourceField)
    )


class BackwardMapping(FrozenDict[ResolveName, FrozenDict[Address, Tuple[str, ...]]]):
    pass


@dataclass
class BackwardMappingRequest:
    addresses: Addresses


@rule
async def get_backward_mapping(
    table_targets: AllTableTargets,
    # mapping: FirstPartyPythonMappingImpl,
) -> BackwardMapping:
    search_for = set(target.address for target in table_targets)
    result: DefaultDict[str, DefaultDict[Address, List[str]]] = defaultdict(
        lambda: defaultdict(list)
    )
    mapping = {}
    for resolve, m in mapping.items():
        for module, addresses in m.items():
            for address in addresses:
                if address in search_for:
                    result[resolve][address.addr].append(module)

    return BackwardMapping(
        FrozenDict(
            (
                resolve,
                FrozenDict(
                    (address, tuple(sorted(modules))) for address, modules in m.items()
                ),
            )
            for resolve, m in result.items()
        )
    )


@rule
async def infer_line_aware_python_dependencies(
    request: InferTableDependenciesRequest,
    python_setup: PythonSetup,
    # table_targets: AllTableTargets,
    # mapping: FirstPartyPythonMappingImpl,
    # backward_mapping: BackwardMapping,
) -> InferredDependencies:
    sources = await Get(
        HydratedSources, HydrateSourcesRequest(request.field_set.source)
    )
    digest_files = await Get(DigestContents, Digest, sources.snapshot.digest)
    content = digest_files[0].content
    resolve = request.field_set.resolve.normalized_value(python_setup)
    assert resolve is not None, "resolve is None"

    search_for_modules = {
        # module
        # for table in table_targets
        # for module in backward_mapping[resolve][table.address]
    }

    parsed = ast.parse(content)
    logger.debug("parsed: %s", parsed)

    ImportVisitor.search_for_modules(parsed, search_for_modules)

    return InferredDependencies(
        include=FrozenOrderedSet(
            # address.addr for module in modules for address in mapping[resolve][module]
        ),
        exclude=FrozenOrderedSet(),
    )


def rules():
    return (
        *collect_rules(),
        UnionRule(GenerateTargetsRequest, GenerateTableTargetsRequest),
        UnionRule(InferDependenciesRequest, InferTableDependenciesRequest),
    )
