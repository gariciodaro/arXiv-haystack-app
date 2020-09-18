#import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import col, concat_ws,lit
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import udf
import sys
import os
import re
REPLACE_BY_SPACE_RE = re.compile('[/(){}\[\]\|@,;]')
BAD_SYMBOLS_RE = re.compile('[^a-z #+_]')

#import configparser
#script_pos = os.path.dirname(os.path.abspath(__file__))

# Get credendials for AWS.
#config = configparser.ConfigParser()
#config.read('dl.cfg')
#config.read('/home/gari/.aws/credentials')
#os.environ['AWS_ACCESS_KEY_ID']=config.get('credentials','KEY')
#os.environ['AWS_SECRET_ACCESS_KEY']=config.get('credentials','SECRET')


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
        text = text.lower() # Todo a minuscula
        text = REPLACE_BY_SPACE_RE.sub(" ",text) # Reemplazamos REPLACE_BY_SPACE_RE symboos por espacio
        text = BAD_SYMBOLS_RE.sub(" ",text) # borramos los BAD_SYMBOLS_RE
        text = re.sub(r'\s+'," ",text) # Eliminamos los espacios en blanco repetidos
    except:
        pass
    return text

clean_text = udf(lambda x: text_prepare(x))

def fix_col_var(df,name_col):
    df_new=df.withColumn(name_col, clean_text(col(name_col))).\
        withColumn(name_col, 
                col(name_col).alias("", metadata={"maxlength":2048}))
    return df_new


def spark_etl(spark,input_data_1,input_data_2,output_data):

    #Main papars db. arXiv.
    df = spark.read.json(input_data_1)
    df.persist()

    #NIPS db. Enrichment of orignal arXiv db.
    df_papers_nips = spark.read.format("csv").option("delimiter", 
                "|").option("header","True").load(input_data_2)
    
    df=fix_col_var(df,"id")
    df=fix_col_var(df,"title")
    df=fix_col_var(df,"comments")
    df=fix_col_var(df,"abstract")

    df_papers_nips=fix_col_var(df_papers_nips,"abstract")
    df_papers_nips=fix_col_var(df_papers_nips,"id")
    df_papers_nips=fix_col_var(df_papers_nips,"title")
    #papers fact table
    df.select("id",
          "title",
          "authors",
          "categories",
          "doi",
          "comments",
          "journal-ref",
          "license",
          "report-no",
          "submitter",
          "update_date").write.mode("overwrite").parquet(output_data+"papers")

    #dimension tables.
    # titles tables
    df_titles=df.select('id','title').union(df_papers_nips.select('id','title'))
    df_titles=df_titles.na.drop()
    df_titles.write.mode("overwrite").parquet(output_data+"titles")

    # authors table
    df_author=df.selectExpr("id","explode(authors_parsed) as e")
    df_author=df_author.withColumn("author",concat_ws(" ",col("e")))
    df_author=df_author.withColumn("id_author",monotonically_increasing_id()).select("id_author","id","author")
    df_author.write.mode("overwrite").parquet(output_data+"authors")

    # abstracts table
    df_abstracts=df.select("id","abstract").withColumn("origin",lit("arXiv"))
    df_abstract_nips=df_papers_nips.select("id",
                                "abstract").withColumn("origin",lit("NIPS"))

    df_en_abstracts=df_abstracts.union(df_abstract_nips)
    df_en_abstracts.na.drop().write.mode("overwrite").parquet(output_data+"abstracts")

    # categories table
    df_categories=df.selectExpr("id","explode(split(categories,' ')) as category")
    df_categories=df_categories.\
        withColumn("id_category",monotonically_increasing_id()).\
            select("id_category","id","author")
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
    df_versions.union(df_versions_nips).na.drop().\
        withColumn("id_version",monotonically_increasing_id()).\
        select("id_version","id","created","version","month","year").\
        write.mode("overwrite").parquet(output_data+"versions")

def main():
    spark=create_spark_session()
    input_data_1= "s3a://arxivs3/input_data/arxiv-metadata-oai-snapshot.json"
    input_data_2= "s3a://arxivs3/input_data/NIPS.csv"
    output_data="s3a://arxivs3/"
    spark_etl(spark,input_data_1,input_data_2,output_data)


if __name__ == "__main__":
    main()