from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count, lit
from functools import reduce
from pyspark.sql.types import BooleanType, StringType, NumericType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("NullValuesDetection") \
    .getOrCreate()
# Specify the path to the Delta table
delta_table_path = "/path/to/delta_table"
# Read the Delta table
df = spark.read.format("delta").load("/delta/silver/res_partner")
# Find columns with null values doesnt consider boolean columns 
columns_with_nulls = []
columns_with_nulls = [column for column in df.columns if df.filter(col(column).isNull()).count() > 0]
print("Columns with null values:")
_ = [print(f"{column}: {df.filter(col(column).isNull() | (col(column) if df.schema[column].dataType == BooleanType() else lit(False))).count()} null value(s)") for column in columns_with_nulls]
for column in columns_with_nulls:
    if isinstance(df.schema[column].dataType, StringType):
        df = df.withColumn(column, when(col(column).isNull(), "Autre").otherwise(col(column)))
    elif isinstance(df.schema[column].dataType, NumericType):
        df = df.withColumn(column, when(col(column).isNull(), 0).otherwise(col(column)))



# Define lists of numerical columns to be replaced with 0 and -1
string_columns_to_replace = ["identity_document", "type_return"]
numerical_columns_to_replace_with_0 = ["weight", "volume", "returned_qty", "planned_qty", "preorder_qty", ]
numerical_columns_to_replace_with_minus1 = []

for column in columns_with_nulls:
    if isinstance(df.schema[column].dataType, StringType):
        if column in string_columns_to_replace:
            df = df.withColumn(column, when(col(column).isNull(), "Autre").otherwise(col(column)))
    elif isinstance(df.schema[column].dataType, NumericType):
        if column in numerical_columns_to_replace_with_0:
            df = df.withColumn(column, when(col(column).isNull(), 0).otherwise(col(column)))
        elif column in numerical_columns_to_replace_with_minus1:
            df = df.withColumn(column, when(col(column).isNull(), -1).otherwise(col(column)))

#separate code 
# Replace null values in string columns
for column in columns_with_nulls:
    if isinstance(df.schema[column].dataType, StringType):
        if column in string_columns_to_replace:
            df = df.withColumn(column, when(col(column).isNull(), "Autre").otherwise(col(column)))

# Replace null values in numerical columns
# Replace null values in numerical columns
for column in columns_with_nulls:
    if isinstance(df.schema[column].dataType, NumericType):
        if column in numerical_columns_to_replace_with_0:
            df = df.withColumn(column, when(col(column).isNull(), 0).otherwise(col(column)))
        elif column in numerical_columns_to_replace_with_minus1:
            df = df.withColumn(column, when(col(column).isNull(), -1).otherwise(col(column)))
# Write the updated dataframe back to the Delta table
df.write.format("delta").mode("overwrite").save("/delta/silver/sale_order_line")
print("Updated dataframe has been written back to the Delta table.")

 


