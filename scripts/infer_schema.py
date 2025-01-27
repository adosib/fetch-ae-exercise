"""
This module is helpful for inferring the schema of the provided source files.
The schema files it writes are used downstream to create the database DDL.
"""

import gzip
import json
import shutil
import tarfile
from pathlib import Path
from collections import defaultdict


def extract_tar_gz(input_path, output_path):
    """
    Extracts a GZIP-compressed file from a TAR archive, falling back to regular
    GZIP decompression if the file is not TAR.
    """
    try:
        with tarfile.open(input_path, "r:gz") as tar:
            # Assume the first file in the TAR archive is the one we want
            member = tar.next()
            if member is None:
                raise ValueError("No files found in the TAR archive.")
            with open(output_path, "wb") as f_out:
                f_out.write(tar.extractfile(member).read())
    except tarfile.ReadError:
        with gzip.open(input_path, "rb") as f_in:
            with open(output_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)


def decode(file_path):
    """
    Decodes a file containing line-delimited JSON objects.
    Yields each JSON object as a Python dictionary.
    """
    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()  # Remove leading/trailing whitespace
            if line:  # Skip empty lines
                yield json.loads(line)


class JSONSchemaInferrer:
    def __init__(self):
        self.schema = defaultdict(
            lambda: {"types": set(), "frequency": 0, "nested": None}
        )

    def infer(self, data):
        """
        Infers the schema of a JSON object or array.
        """
        if isinstance(data, list):
            for item in data:
                self._infer_object(item)
        elif isinstance(data, dict):
            self._infer_object(data)
        else:
            raise ValueError("Input data must be a JSON object or array.")

    def _infer_object(self, obj):
        """
        Recursively infers the schema of a JSON object.
        """
        for key, value in obj.items():
            self.schema[key]["types"].add(self._map_types(value))
            self.schema[key]["frequency"] += 1

            if isinstance(value, dict):
                if self.schema[key]["nested"] is None:
                    self.schema[key]["nested"] = JSONSchemaInferrer()
                self.schema[key]["nested"]._infer_object(value)
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                if self.schema[key]["nested"] is None:
                    self.schema[key]["nested"] = JSONSchemaInferrer()
                for item in value:
                    self.schema[key]["nested"]._infer_object(item)

    def _map_types(self, value):
        """
        Returns the type of the value as a string.
        """
        if value is None:
            return "null"
        elif isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "integer"
        elif isinstance(value, float):
            return "number"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, dict):
            return "object"
        elif isinstance(value, list):
            return "array"
        else:
            return "unknown"

    def get_schema_summary(self):
        """
        Returns a summarized schema in a readable format.
        """
        summary = {}
        for key, info in self.schema.items():
            summary[key] = {
                "types": list(info["types"]),
                "frequency": info["frequency"],
                "nested": info["nested"].get_schema_summary()
                if info["nested"]
                else None,
            }
        return summary


if __name__ == "__main__":
    this_dir = Path(__file__).parent
    data_dir = this_dir / "data"

    gzipped_files = [
        "brands.json.gz",
        "receipts.json.gz",
        "users.json.gz",
    ]

    for gzfile in gzipped_files:
        unzipped_filename = gzfile.replace(".gz", "")

        extract_tar_gz(data_dir / gzfile, data_dir / unzipped_filename)

        json_arr = []
        for json_obj in decode(data_dir / unzipped_filename):
            json_arr.append(json_obj)

        inferrer = JSONSchemaInferrer()
        inferrer.infer(json_arr)
        schema_summary = inferrer.get_schema_summary()

        with open(
            this_dir / "schemas" / unzipped_filename.replace(".json", "_schema.json"),
            "w+",
        ) as sc:
            json.dump(schema_summary, sc, indent=2)
