import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import col, concat_ws
import sys
import os

#script_pos = os.path.dirname(os.path.abspath(__file__))



def create_spark_session():
    """Creates SparkSession. General configurarion
    of the script.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def spark_etl(spark,input_data,output_data):
    df = spark.read.json(input_data)
    df_author=df.selectExpr("id","explode(authors_parsed) as e")
    df_author=df_author.withColumn("author",concat_ws(" ",col("e")))
    df_author=df_author.select("id","author")
    df_author.write.mode("overwrite").parquet(output_data+"authors")

    df_abstracts=df.select("id","abstract")
    df_abstracts.write.mode("overwrite").parquet(output_data+"abstracts")

    df_categories=df.selectExpr("id","explode(split(categories,' ')) as category")
    df_categories.write.mode("overwrite").parquet(output_data+"categories")


def main():
    spark=create_spark_session()
    input_data = "/home/gari/Desktop/final_project/input_data/arxiv-metadata-oai-snapshot.json"
    output_data="/home/gari/Desktop/final_project/parquet_area/"
    spark_etl(spark,input_data,output_data)


if __name__ == "__main__":
    main()