from pyspark.sql import SparkSession
from pyspark.sql import SaveMode

def convert_parquet_to_delta(input_path, output_path):
    spark = SparkSession.builder \
        .appName("ConvertParquetToDelta") \
        .getOrCreate()

    file_list = spark.read.format("parquet").load(input_path).inputFiles()

    for file in file_list:
        df = spark.read.format("parquet").load(file)
        file_name = file.split("/")[-1]
        delta_path = f"{output_path}/{file_name}"
        df.write.format("delta").mode(SaveMode.Append).save(delta_path)

input_path = "/path/to/input/dir"
output_path = "/path/to/output/dir"

convert_parquet_to_delta(input_path, output_path)
