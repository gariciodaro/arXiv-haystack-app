#import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import col, concat_ws,lit
from pyspark.sql.functions import trim
from pyspark.sql.functions import udf
import sys
import os
import re
REPLACE_BY_SPACE_RE = re.compile('[/(){}\[\]\|@,;]')
BAD_SYMBOLS_RE = re.compile('[^0-9a-z #+_]')


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
        text = (text[:256]) if len(text) > 256 else text
    except:
        pass
    return text

def prepare_abstract(text):
    try:
        text = (text[:2048]) if len(text) > 2048 else text
    except:
        pass
    return text

clean_text = udf(lambda x: text_prepare(x))

abstrac_text = udf(lambda x: prepare_abstract(x))

def fix_col_var(df):
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

    #Main papars db. arXiv.
    df = spark.read.json(input_data_1)
    #df = df.withColumn("id", trim(col("id")))
    df.persist()

    #NIPS db. Enrichment of orignal arXiv db.
    df_papers_nips = spark.read.format("csv").option("delimiter", 
                "|").option("header","True").load(input_data_2)
    #df_papers_nips = df_papers_nips.withColumn("id", trim(col("id")))
    
    df=fix_col_var(df)

    df_papers_nips=fix_col_var(df_papers_nips)
    #papers fact table
    df.select("id",
          "abstract",
          "title",
          "categories",
          "doi",
          "comments",
          "journal-ref",
          "license",
          "report-no",
          "submitter",
          "update_date").write.mode("overwrite").parquet(output_data+"papers")


def main():
    spark=create_spark_session()
    input_data_1= "s3a://arxivs3/input_data/arxiv-metadata-oai-snapshot.json"
    input_data_2= "s3a://arxivs3/input_data/NIPS.csv"
    output_data="s3a://arxivs3/"
    spark_etl(spark,input_data_1,input_data_2,output_data)


if __name__ == "__main__":
    main()