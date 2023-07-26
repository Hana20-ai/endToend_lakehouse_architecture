import os
import sys
from pyspark.sql.functions import col, isnan, when, count, lit
from functools import reduce
from pyspark.sql.types import BooleanType, StringType, NumericType


# Read the repository path from command-line argument
repository_path = sys.argv[1]

# Get a list of all Delta table paths in the repository
delta_table_paths = [os.path.join(repository_path, file) for file in os.listdir(repository_path) if os.path.isfile(os.path.join(repository_path, file))]

# Iterate over Delta tables and process each one
for delta_table_path in delta_table_paths:
    # Read the Delta table
    df = spark.read.format("delta").load(delta_table_path)

    # Find columns with null values
    columns_with_nulls = [column for column in df.columns if df.filter(col(column).isNull()).count() > 0]
    print(f"Columns with null values in {delta_table_path}:")
    _ = [print(f"{column}: {df.filter(col(column).isNull() | (col(column) if isinstance(df.schema[column].dataType, BooleanType) else lit(False))).count()} null value(s)") for column in columns_with_nulls]

    # Replace null values
    for column in columns_with_nulls:
        if isinstance(df.schema[column].dataType, StringType):
            df = df.withColumn(column, when(col(column).isNull(), "Autre").otherwise(col(column)))
        elif isinstance(df.schema[column].dataType, NumericType):
            df = df.withColumn(column, when(col(column).isNull(), 0).otherwise(col(column)))

    # Write the updated dataframe back to the Delta table
    df.write.format("delta").mode("overwrite").save(delta_table_path)

    print(f"Updated dataframe for {delta_table_path} has been written back to the Delta table.")
