import ast
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, DefaultDict, List, Set, Tuple

from pants.backend.python.dependency_inference.module_mapper import (
    FirstPartyPythonModuleMapping,
    ResolveName,
)
from pants.backend.python.subsystems.setup import PythonSetup
from pants.backend.python.target_types import PythonResolveField, PythonSourceField
from pants.engine.addresses import Addresses
from pants.engine.fs import Digest, DigestContents
from pants.engine.rules import Get, MultiGet, collect_rules, rule
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
    SourcesPaths,
    SourcesPathsRequest,
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
    alias = "table"  # rename to python_constant


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


@dataclass(frozen=True)
class Var:
    module: str
    name: str


class ImportVisitor(ast.NodeVisitor):
    def __init__(self, search_for_modules: set[str]) -> None:
        super().__init__()
        self._search_for = search_for_modules
        self._found: Set[Var] = set()

    def visit_ImportFrom(self, node: ast.ImportFrom) -> Any:
        if node.module not in self._search_for:
            return

        for alias in node.names:
            self._found.add(Var(node.module, alias.name))

    @classmethod
    def search_for_vars(cls, content: bytes, modules: set[str]) -> set[Var]:
        parsed = ast.parse(content.decode("utf-8"))
        v = cls(modules)
        v.visit(parsed)
        return v._found


class AllTableTargets(Targets):
    pass


@rule
async def get_table_targets(targets: AllTargets) -> AllTableTargets:
    return AllTableTargets(
        target for target in targets if target.has_field(TableSourceField)
    )


class BackwardMapping(FrozenDict[ResolveName, FrozenDict[str, Tuple[str, ...]]]):
    pass


@dataclass
class BackwardMappingRequest:
    addresses: Addresses


@rule
async def get_backward_mapping(
    table_targets: AllTableTargets,
    mapping: FirstPartyPythonModuleMapping,
) -> BackwardMapping:
    paths = await MultiGet(
        Get(SourcesPaths, SourcesPathsRequest(tgt.get(TableSourceField)))
        for tgt in table_targets
    )
    search_for = {file for path in paths for file in path.files}

    result: DefaultDict[str, DefaultDict[str, List[str]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for resolve, m in mapping.resolves_to_modules_to_providers.items():
        for module, module_providers in m.items():
            for module_provider in module_providers:
                filename = module_provider.addr.filename
                if filename in search_for:
                    result[resolve][filename].append(module)

    return BackwardMapping(
        FrozenDict(
            (
                resolve,
                FrozenDict(
                    (filename, tuple(sorted(modules)))
                    for filename, modules in m.items()
                ),
            )
            for resolve, m in result.items()
        )
    )


@rule
async def infer_line_aware_python_dependencies(
    request: InferTableDependenciesRequest,
    python_setup: PythonSetup,
    table_targets: AllTableTargets,
    mapping: FirstPartyPythonModuleMapping,
    backward_mapping: BackwardMapping,
) -> InferredDependencies:
    """Infers dependencies on TableTarget-s based on python source imports."""

    sources = await Get(
        HydratedSources, HydrateSourcesRequest(request.field_set.source)
    )
    digest_files = await Get(DigestContents, Digest, sources.snapshot.digest)
    content = digest_files[0].content
    resolve = request.field_set.resolve.normalized_value(python_setup)
    assert resolve is not None, "resolve is None"

    if not backward_mapping:
        raise ValueError("empty backward mapping")

    paths = await MultiGet(
        Get(SourcesPaths, SourcesPathsRequest(tgt.get(TableSourceField)))
        for tgt in table_targets
    )
    logger.debug("backward mapping %s", backward_mapping)
    interesting_modules = {
        module
        for path in paths
        for filename in path.files
        for module in backward_mapping[resolve][filename]
    }

    logger.debug("interesting_modules %s", interesting_modules)
    vars = ImportVisitor.search_for_vars(content, interesting_modules)
    logger.debug("vars %s", vars)

    filenames_to_table_targets: DefaultDict[str, List[Target]] = defaultdict(list)
    for path, target in zip(paths, table_targets):
        for filename in path.files:
            filenames_to_table_targets[filename].append(target)

    include = set()
    for var in vars:
        for provider in mapping.resolves_to_modules_to_providers[resolve][var.module]:
            targets = filenames_to_table_targets[provider.addr.filename]
            for target in targets:
                name = target.get(TableNameField).value
                logger.debug("check for var %s %s", name, var.name)
                if name == var.name:
                    include.add(target.address)

    logger.debug("include %s", include)
    return InferredDependencies(
        include=FrozenOrderedSet(include),
        exclude=FrozenOrderedSet(),
    )


def rules():
    return (
        *collect_rules(),
        UnionRule(GenerateTargetsRequest, GenerateTableTargetsRequest),
        UnionRule(InferDependenciesRequest, InferTableDependenciesRequest),
    )
