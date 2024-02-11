tables(name="tables", source="tables.py", dependencies=["src/tables.py:src"])
python_sources(
    name="src",
    overrides={
        "run.py": dict(dependencies=["!//src/tables.py:src"]),
    },
)
