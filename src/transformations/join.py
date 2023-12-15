# src/transformations/join.py
from pyspark.sql import SparkSession

def join_datasets(df1, df2, join_key):
    return df1.join(df2, join_key)
