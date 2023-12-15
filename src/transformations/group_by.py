# src/transformations/group_by.py
from pyspark.sql import SparkSession

def group_by_measure(df, measure_col):
    return df.groupBy(measure_col).agg({"value": "sum"})
