# -*- coding: utf-8 -*-
"""
Created on Sept 15 2020
@author: gari.ciodaro.guerra
pyspark script that reads from s3 bucket .json from 
arXiv db, and process the data to deposit it as parquet files
with the star schema on S3.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import col, concat_ws,lit
from pyspark.sql.functions import trim
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id
import sys
import os
import re
import configparser
config = configparser.ConfigParser()
# s3 bucked having the arxiv-metadata-oai-snapshot.json kaggle dataset.
S3_BUCKED = config.get("APP","S3_BUCKED")


REPLACE_BY_SPACE_RE = re.compile('[/(){}\[\]\|@,;]')
BAD_SYMBOLS_RE = re.compile('[^0-9a-z #+_]')

def create_spark_session():
    """Creates SparkSession. General configurarion
    of the script.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def text_prepare(text):
    """
        text: a string
        return: standarized initial string
    """
    try:
        text = text.lower() 
        # replace by space
        text = REPLACE_BY_SPACE_RE.sub(" ",text) 
        # remove bad symbols
        text = BAD_SYMBOLS_RE.sub(" ",text)
        # remove redudant blank spaces
        text = re.sub(r'\s+'," ",text)
        # trucate for redshift
        text = (text[:256]) if len(text) > 256 else text
    except:
        pass
    return text

def prepare_abstract(text):
    """
        text: a the text of the abstract.
        return: standarized initial string
    """
    try:
        # truncate abstract to redshift maximum lenght.
        text = (text[:2048]) if len(text) > 2048 else text
    except:
        pass
    return text

# registering user difined functions on pyspark
clean_text = udf(lambda x: text_prepare(x))
abstrac_text = udf(lambda x: prepare_abstract(x))

def fix_col_var(df):
    """ Prepare data to be injested on redshift. The limit length
        of string in redshif is 2048.
        df: dataframe
    """
    columns_list=df.columns
    for name_col in columns_list:
        if name_col != "abstract":
            df=df.withColumn(name_col, trim(clean_text(col(name_col)))).\
                withColumn(name_col, 
                        col(name_col).alias("", metadata={"maxlength":256}))
        else:
            df=df.withColumn(name_col, trim(abstrac_text(col(name_col)))).\
                withColumn(name_col, 
                        col(name_col).alias("", metadata={"maxlength":2048}))
    return df

def spark_etl(spark,input_data_1,input_data_2,output_data):
    """Main etl function. It processes raw json and csv files and writes
        structured data to  S3.
    """
    #Main papers db. arXiv.
    df = spark.read.json(input_data_1)
    df.persist()
    #NIPS db. Enrichment of orignal arXiv db.
    df_papers_nips = spark.read.format("csv").option("delimiter", 
                "|").option("header","True").load(input_data_2)
    # prepare columns lenght so that they are compatible with redshift.
    df_papers_nips=fix_col_var(df_papers_nips)
    #papers fact table papers
    fix_col_var(df.select("id",
          "title",
          "categories",
          "doi",
          "comments",
          "journal-ref",
          "license",
          "report-no",
          "submitter",
          "update_date")).write.mode("overwrite").parquet(output_data+"papers")
    #dimension tables.
    # authors table
    df_author=df.selectExpr("id","explode(authors_parsed) as e")
    df_author=fix_col_var(df_author.withColumn("author",
                    concat_ws(" ",col("e"))).select("id","author"))
    df_author.write.mode("overwrite").parquet(output_data+"authors")
    # abstracts table
    df_abstracts=df.select("id","abstract").withColumn("origin",lit("arXiv"))
    df_abstract_nips=df_papers_nips.select("id",
                                "abstract").withColumn("origin",lit("NIPS"))
    df_en_abstracts=df_abstracts.union(df_abstract_nips)
    fix_col_var(df_en_abstracts.na.drop()).write.mode("overwrite").parquet(output_data+"abstracts")
    # categories table
    df_categories=fix_col_var(df.selectExpr("id",
                                "explode(split(categories,' ')) as category"))
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
                    withColumn('month',lit('no info'))
    df_versions_nips=df_versions_nips.select('id','created',
                                            'version',
                                            'month',
                                            'year')
    fix_col_var(df_versions.union(df_versions_nips).na.drop()).\
        select("id","created","version","month","year").\
        write.mode("overwrite").parquet(output_data+"versions")

    # titles table
    df_titles=df.select('id','title').union(df_papers_nips.select('id','title'))
    df_titles=fix_col_var(df_titles.na.drop())
    df_titles.write.mode("overwrite").parquet(output_data+"titles")


def main():
    spark=create_spark_session()
    input_data_1= "s3a://{}/input_data/arxiv-metadata-oai-snapshot.json".format(S3_BUCKED)
    input_data_2= "s3a://{}/input_data/NIPS.csv".format(S3_BUCKED)
    output_data="s3a://{}/".format(S3_BUCKED)
    spark_etl(spark,input_data_1,input_data_2,output_data)

if __name__ == "__main__":
    main()