# src/main.py
from pyspark.sql import SparkSession
from src.transformations.group_by import group_by_measure
from src.transformations.join import join_datasets
from src.connectors.postgresql_connector import read_from_postgresql, write_to_postgresql
from src.formats.json_format import read_json, write_json

def main():
    spark = SparkSession.builder.appName("TaiyoApp").getOrCreate()

    postgres_url = "jdbc:postgresql://:5432/PDdb"
    source_table = "Taiyo"
    df_postgres = read_from_postgresql(spark, postgres_url, source_table)

    df_grouped = group_by_measure(df_postgres, "some_column")

    df_another = read_json(spark, "dataset.json")

    df_joined = join_datasets(df_grouped, df_another, "common_column")

    target_table = "Taiyo2"
    write_to_postgresql(df_joined, postgres_url, target_table)

    write_json(spark, df_joined, "output.json")

if __name__ == "__main__":
    main()

