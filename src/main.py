# src/main.py
from pyspark.sql import SparkSession
from src.transformations.group_by import group_by_measure
from src.transformations.join import join_datasets
from src.connectors.postgresql_connector import read_from_postgresql, write_to_postgresql
from src.formats.json_format import read_json, write_json

def main():
    # Create a Spark session
    spark = SparkSession.builder.appName("YourAppName").getOrCreate()

    # Read from PostgreSQL (assuming 'Taiyo' is the table name)
    postgres_url = "jdbc:postgresql://:5432/PDdb"
    source_table = "Taiyo"
    df_postgres = read_from_postgresql(spark, postgres_url, source_table)

    # Apply transformations
    df_grouped = group_by_measure(df_postgres, "name")
    
    # Read from JSON (adjust file path accordingly)
    df_another = read_json(spark, "dataset.json")

    # Perform join
    df_joined = join_datasets(df_grouped, df_another, "user_Id")

    # Write to PostgreSQL (adjust 'your_target_table' accordingly)
    target_table = "Taiyo2"
    write_to_postgresql(df_joined, postgres_url, target_table)

    # Write result to JSON (adjust file path accordingly)
    write_json(spark, df_joined, "output.json")

if __name__ == "__main__":
    main()
