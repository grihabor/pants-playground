from pants.engine.target import SingleSourceField, StringField, Target


class TableSource(SingleSourceField):
    required = True


class TableName(StringField):
    alias = "table"


class TableTarget(Target):
    alias = "table"
    core_fields = (TableSource, TableName)
