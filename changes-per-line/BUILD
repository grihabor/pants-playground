tables(name="tables", source="tables.py", dependencies=["//tables.py:src"])
python_sources(
    name="src",
    overrides={
        "run.py": dict(dependencies=["!//tables.py:src"]),
    },
)
