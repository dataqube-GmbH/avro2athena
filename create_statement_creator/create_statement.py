from create_statement_creator.athena_schema import create_athena_schema_from_avro
from util.avsc_file_parser import parse_literal_schema_from_file


def create_athena_table_statement_from_avsc(database: str,
                                            table_name: str,
                                            path_to_avsc: str,
                                            partition_statement: str,
                                            s3_location: str) -> str:

    avro_schema_literal = parse_literal_schema_from_file(path_to_avsc)
    athena_schema = create_athena_schema_from_avro(avro_schema_literal)

    return f'''
    CREATE EXTERNAL TABLE IF NOT EXISTS 
    `{database}`.`{table_name}`
    ({athena_schema}) 
    {partition_statement}
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
    WITH SERDEPROPERTIES ('avro.schema.literal'='{avro_schema_literal}') 
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' 
    LOCATION '{s3_location}'
    '''
