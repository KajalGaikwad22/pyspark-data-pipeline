# src/formats/json_format.py
from pyspark.sql import SparkSession

file_path = 'dataset.csv'

def read_json(spark, file_path):
    return spark.read.json(file_path)

def write_json(spark, df, file_path):
    df.write.json(file_path)


