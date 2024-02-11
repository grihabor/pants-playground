from dataclasses import dataclass

from pants.engine.internals.graph import HunkOwnersRequest, Owners
from pants.engine.rules import collect_rules, rule
from pants.engine.unions import UnionRule
from pants.util.frozendict import FrozenDict
from pants.vcs.hunk import Hunk


@dataclass(frozen=True)
class MyHunkOwnersRequest(HunkOwnersRequest):
    pass


@rule
def get_my_hunk_owners(request: MyHunkOwnersRequest) -> Owners:
    raise RuntimeError(request)
    return Owners()


def rules():
    return [
        *collect_rules(),
        UnionRule(HunkOwnersRequest, MyHunkOwnersRequest),
    ]
