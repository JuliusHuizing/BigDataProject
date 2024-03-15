import yaml
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def convert_config_to_pyspark_schema():
    # Define a dictionary to map the types from the YAML file to PySpark types
    type_mapping = {
        "IntegerType": IntegerType(),
        "StringType": StringType()
    }

    # Load the schema from the YAML file
    with open("schema.yaml", "r") as file:
        schema_yaml = yaml.safe_load(file)

    # Dynamically construct the StructType based on the YAML content
    schema_fields = []
    for field in schema_yaml["fields"]:
        field_type = type_mapping[field["type"]]
        schema_fields.append(StructField(field["name"], field_type, field["nullable"]))

    schema = StructType(schema_fields)

    # Now, TRAIN_SCHEMA is ready to be used for reading CSV files into PySpark DataFrame
    return schema
