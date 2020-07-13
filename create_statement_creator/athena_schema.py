from avro.schema import parse, RecordSchema, PrimitiveSchema, ArraySchema, MapSchema, EnumSchema, UnionSchema, FixedSchema, \
    BytesDecimalSchema, FixedDecimalSchema, DateSchema, TimeMillisSchema, TimeMicrosSchema, TimestampMillisSchema, \
    TimestampMicrosSchema

def create_athena_schema_from_avro(avro_schema_literal: str) -> str:
    avro_schema: RecordSchema = parse(avro_schema_literal)

    column_schemas = []
    for field in avro_schema.fields:
        print(f'processing {field}\n\n')
        column_name = field.name.lower()
        print(f'column_name {column_name}\n\n')
        column_type = create_athena_column_schema(field.type)
        print(f'column_type {column_type}\n\n')
        column_schemas.append(f"`{column_name}` {column_type}")

    return ', '.join(column_schemas)


def create_athena_column_schema(avro_schema) -> str:
    if type(avro_schema) == PrimitiveSchema:
        return rename_type_names(avro_schema.type)

    elif type(avro_schema) == ArraySchema:
        items_type = create_athena_column_schema(avro_schema.items)
        return f'array<{items_type}>'

    elif type(avro_schema) == MapSchema:
        values_schema = create_athena_column_schema(avro_schema.values)
        return f'map<string,{values_schema}>'

    elif type(avro_schema) == RecordSchema:
        field_schemas = []
        for field in avro_schema.fields:
            field_name = field.name.lower()
            field_type = create_athena_column_schema(field.type)
            field_schemas.append(f'{field_name}:{field_type}')

        field_schema_concatenated = ','.join(field_schemas)
        return f'struct<{field_schema_concatenated}>'

    elif type(avro_schema) == UnionSchema:
        # pick the first schema which is not null
        union_schemas_not_null = [s for s in avro_schema.schemas if s.type != 'null']
        if len(union_schemas_not_null) > 0:
            return create_athena_column_schema(union_schemas_not_null[0])
        else:
            raise Exception('union schemas contains only null schema')

    elif type(avro_schema) in [EnumSchema, FixedSchema]:
        return 'string'

    elif type(avro_schema) in [BytesDecimalSchema, FixedDecimalSchema]:
        return f'decimal({avro_schema.precision},{avro_schema.scale})'

    elif type(avro_schema) == DateSchema:
        return 'date'

    elif type(avro_schema) == TimeMillisSchema:
        return 'int'

    elif type(avro_schema) == TimeMicrosSchema:
        return 'long'

    elif type(avro_schema) in [TimestampMillisSchema, TimestampMicrosSchema]:
        return 'timestamp'

    else:
        raise Exception(f'unknown avro schema type {avro_schema.type}')


def rename_type_names(typ: str) -> str:
    if typ in ['long']:
        return 'bigint'
    else:
        return typ
