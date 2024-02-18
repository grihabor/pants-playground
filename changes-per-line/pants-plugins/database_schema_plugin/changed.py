import itertools
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Tuple

from database_schema_plugin.target_types import (
    AllPythonConstantTargets,
    PythonConstantEndLinenoField,
    PythonConstantLinenoField,
    PythonConstantSourceField,
    PythonConstantTarget,
)
from pants.engine.internals.graph import BlockOwnersRequest, Owners
from pants.engine.rules import Get, MultiGet, collect_rules, rule
from pants.engine.target import SourcesPaths, SourcesPathsRequest
from pants.engine.unions import UnionRule
from pants.util.frozendict import FrozenDict

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PythonConstantBlockOwnersRequest(BlockOwnersRequest):
    pass


class PythonConstantMapping(FrozenDict[str, Tuple[PythonConstantTarget, ...]]):
    pass


@rule
async def make_python_constant_mapping(
    targets: AllPythonConstantTargets,
) -> PythonConstantMapping:
    sources = await MultiGet(
        Get(SourcesPaths, SourcesPathsRequest(tgt.get(PythonConstantSourceField)))
        for tgt in targets
    )

    mapping = defaultdict(list)
    for paths, target in zip(sources, targets):
        for path in paths.files:
            mapping[path].append(target)

    return PythonConstantMapping(
        (path, tuple(targets)) for path, targets in mapping.items()
    )


@rule
async def get_my_block_owners(
    request: PythonConstantBlockOwnersRequest,
    mapping: PythonConstantMapping,
) -> Owners:
    logger.debug("block owners request: %s", request)
    owners = set()
    for path, blocks in request.blocks.items():
        targets = mapping.get(path)
        if not targets:
            continue

        for target, block in itertools.product(targets, blocks):
            start_lineno = target.get(PythonConstantLinenoField).value
            end_lineno = target.get(PythonConstantEndLinenoField).value

            if block.start > end_lineno:
                continue
            if block.start + block.count - 1 < start_lineno:  # TODO tests
                continue
            owners.add(target.address)

    return Owners(owners)


def rules():
    return [
        *collect_rules(),
        UnionRule(BlockOwnersRequest, PythonConstantBlockOwnersRequest),
    ]
