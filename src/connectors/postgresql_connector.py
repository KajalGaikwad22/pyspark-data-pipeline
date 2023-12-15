# src/connectors/postgresql_connector.py
from pyspark.sql import SparkSession

def read_from_postgresql(url, table):
    spark = SparkSession.builder.getOrCreate()
    return spark.read.format("jdbc").option("url", url).option("Taiyo", table).load()

def write_to_postgresql(df, url, table):
    df.write.format("jdbc").option("url", url).option("Taiyo2", table).mode("overwrite").save()
