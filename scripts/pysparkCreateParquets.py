import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import col, concat_ws,lit
import sys
import os
import configparser
#script_pos = os.path.dirname(os.path.abspath(__file__))

# Get credendials for AWS.
config = configparser.ConfigParser()
#config.read('dl.cfg')
config.read('/home/gari/.aws/credentials')
os.environ['AWS_ACCESS_KEY_ID']=config.get('credentials','KEY')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('credentials','SECRET')


def create_spark_session():
    """Creates SparkSession. General configurarion
    of the script.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def spark_etl(spark,input_data_1,input_data_2,output_data):

    #Main papars db. arXiv.
    df = spark.read.json(input_data_1)
    df.persist()

    #NIPS db. Enrichment of orignal arXiv db.
    df_papers_nips = spark.read.format("csv").option("delimiter", 
                "|").option("header","True").load(input_data_2)
    
    #papers fact table
    df.write.mode("overwrite").parquet(output_data+"papers")

    #dimension tables.
    # titles tables
    df_titles=df.select('id','title').union(df_papers_nips.select('id','title'))
    df_titles.write.mode("overwrite").parquet(output_data+"titles")

    # authors table
    df_author=df.selectExpr("id","explode(authors_parsed) as e")
    df_author=df_author.withColumn("author",concat_ws(" ",col("e")))
    df_author=df_author.select("id","author")
    df_author.write.mode("overwrite").parquet(output_data+"authors")

    # abstracts table
    df_abstracts=df.select("id","abstract").withColumn("origin",lit("arXiv"))
    df_abstract_nips=df_papers_nips.select("id",
                                "abstract").withColumn("origin",lit("NIPS"))
    df_abstracts.union(df_abstract_nips).write.mode("overwrite").\
                                            parquet(output_data+"abstracts")

    # categories table
    df_categories=df.selectExpr("id","explode(split(categories,' ')) as category")
    df_categories.write.mode("overwrite").parquet(output_data+"categories")

    # versions table
    df_versions=df.selectExpr("id","versions","explode(versions) as info_versions")
    df_versions=df_versions.withColumn(
                        "created", col("info_versions.created")).\
                        withColumn("version", col("info_versions.version")).\
                        selectExpr("id","created","version").\
                        withColumn("month", 
                        regexp_extract(col('created'), 
                        '(,)(\s+)(\w+)(\s+)(\w+)', 5)).\
                        withColumn("year", regexp_extract(col('created'),
                        '(,)(\s+)(\w+)(\s+)(\w+)(\s+)(\w+)', 7))
    df_versions_nips=df_papers_nips.select('id','year').\
                    withColumn('created',lit('no info')).\
                    withColumn('version',lit('no info')).\
                    withColumn('mouth',lit('no info'))
    df_versions_nips=df_versions_nips.select('id','created',
                                            'version',
                                            'mouth',
                                            'year')
    df_versions.union(df_versions_nips).write.mode("overwrite").\
                                        parquet(output_data+"versions")

def main():
    spark=create_spark_session()
    input_data_1= "/home/gari/Desktop/final_project/input_data/arxiv-metadata-oai-snapshot.json"
    input_data_2= "/home/gari/Desktop/final_project/input_data/NIPS.csv"
    output_data="s3a://arxivs3/"
    spark_etl(spark,input_data_1,input_data_2,output_data)


if __name__ == "__main__":
    main()