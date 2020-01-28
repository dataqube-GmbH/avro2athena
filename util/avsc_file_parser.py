import json


def parse_literal_schema_from_file(path_to_avsc: str) -> str:
    with open(path_to_avsc, 'r') as f:
        avro_schema_literal = json.dumps(json.load(f))

    return avro_schema_literal
