#import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import col, concat_ws,lit
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

def spark_etl(spark,input_data_1,input_data_2,output_data):

    #Main papars db. arXiv.
    df = spark.read.json(input_data_1)
    df.persist()
    #NIPS db. Enrichment of orignal arXiv db.
    df_papers_nips = spark.read.format("csv").option("delimiter", 
                "|").option("header","True").load(input_data_2)
    # abstracts table
    df_abstracts=df.select("id","abstract").withColumn("origin",lit("arXiv"))
    df_abstract_nips=df_papers_nips.select("id",
                                "abstract").withColumn("origin",lit("NIPS"))
    df_en_abstracts=df_abstracts.union(df_abstract_nips)
    clean_text = udf(lambda x: text_prepare(x))

    new_abs=df_en_abstracts.\
        withColumn("abstract", clean_text(df.abstract)).\
            withColumn("abstract", col("abstract").alias("", metadata={"maxlength":2048}))
    new_abs.na.drop().write.mode("overwrite").parquet(output_data+"abstracts")


def main():
    spark=create_spark_session()
    input_data_1= "s3a://arxivs3/input_data/arxiv-metadata-oai-snapshot.json"
    input_data_2= "s3a://arxivs3/input_data/NIPS.csv"
    output_data="s3a://arxivs3/"
    spark_etl(spark,input_data_1,input_data_2,output_data)


if __name__ == "__main__":
    main()