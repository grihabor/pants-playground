# POC for fine grained diff support

This repo demonstrates how to use fine grained diff support imlemented in [this pr](https://github.com/pantsbuild/pants/pull/20531).

You can find `BlockOwnersRequest` subclass in [pants-plugins/database_schema_plugin/changed.py](./pants-plugins/database_schema_plugin/changed.py).

To see it in action, first, change src/tables.py, then run:
```bash
PANTS_SOURCE=~/projects/pants pants -ldebug --changed-since=HEAD~1 --dependents-closed dependents
```

