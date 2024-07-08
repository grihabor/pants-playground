To reproduce the problem:
```
git apply patch.txt
pants list --changed-since=HEAD
```

I expect to see the helm chart, because one of the templates has changed, but the list is empty.
