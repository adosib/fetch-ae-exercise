"""
This module generates the inital DDL for each JSON schema file.
The generated CREATE statements are crude and need a manual pass-over to finalize.
"""

import sys
import json
from pathlib import Path


def generate_ddl(schema, table_name, pk="_id"):
    ddl = [f"CREATE TABLE {table_name} ("]
    columns = []

    def flatten_keys(data, prefix=""):
        for key, value in data.items():
            if value["nested"]:
                nested_prefix = f"{prefix}{key}_" if prefix else f"{key}_"
                flatten_keys(value["nested"], nested_prefix)
            else:
                column_name = f"{prefix}{key}"
                column_type = map_type(value["types"])
                nullable = (
                    "NOT NULL" if value["frequency"] == schema[pk]["frequency"] else ""
                )
                if key.startswith(pk):
                    nullable = "PRIMARY KEY"
                columns.append(f"{column_name} {column_type} {nullable}".strip())

    def map_type(types):
        if "string" in types:
            return "TEXT"
        elif "boolean" in types:
            return "BOOLEAN"
        elif "object" in types:
            return "TEXT"  # Flattened objects are treated as TEXT
        else:
            return "TEXT"  # Default to TEXT for unknown types

    flatten_keys(schema)
    ddl.append(",\n    ".join(columns))
    ddl.append(");\n")
    return "\n".join(ddl)


if __name__ == "__main__":
    this_dir = Path(__file__).parent
    schema_dir = this_dir / "schemas"
    ddl_path = schema_dir / "ddl.sql"
    if ddl_path.exists():
        sys.exit()

    for schema_path in schema_dir.glob("*.json"):
        with open(schema_path) as f:
            schema = json.load(f)
            table_name = (schema_path.stem).replace("_schema", "")

            ddl_statement = generate_ddl(schema, table_name)

            with open(ddl_path, "a+") as ddl:
                ddl.write(ddl_statement)
