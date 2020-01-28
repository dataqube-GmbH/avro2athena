from avro.datafile import DataFileReader
from avro.io import DatumReader


def infer_schema_from_avro_file(path_to_avro_file: str) -> str:
    reader = DataFileReader(open(path_to_avro_file, "rb"), DatumReader())

    return reader.meta['avro.schema'].decode('utf-8')
