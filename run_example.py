from create_statement_creator.create_statement import create_athena_table_statement_from_avsc

if __name__ == "__main__":
    database = 'my_database'
    table_name = 'my_table'
    path_to_avsc = 'example_schemas/avro_basic_example.avsc'
    partition_statement = 'PARTITIONED BY (year string, month string, day string)'  # set partition_statement = '' if you don't have partitions
    s3_location = 's3://my_bucket/my_folder/'

    create_statement = create_athena_table_statement_from_avsc(database,
                                                               table_name,
                                                               path_to_avsc,
                                                               partition_statement,
                                                               s3_location)

    print(create_statement)
