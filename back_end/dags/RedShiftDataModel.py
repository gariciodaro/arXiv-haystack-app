# -*- coding: utf-8 -*-
"""
Created on Sat Sept 13 2020
@author: gari.ciodaro.guerra
DAG of AirFlow to create star schema of 
in Redshift. Run on demand.
"""


import datetime
import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.postgres_operator import PostgresOperator
import configparser

config = configparser.ConfigParser()

# AWS credentials
config.read('/home/gari/.aws/credentials')

DWH_ROLE = config.get("DWH","DWH_ROLE")

aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()

copy_sql = ("""
    COPY {} 
    FROM '{}'
    IAM_ROLE '{}'
    FORMAT AS PARQUET
""")

list_tables=['papers','authors','abstracts','categories','versions','titles']
#list_tables=['authors','abstracts','categories','versions','titles']

args = {
    'owner': 'Gari',
    'start_date': days_ago(2),
    'catchup': False,
    'depends_on_past':False
}

dag = DAG(
        dag_id='load_data_to_redshift',
        default_args=args,
        schedule_interval=None
        )

for each_table in list_tables:
    PostgresOperator(
    task_id="loading_table_"+each_table,
    dag=dag,
    postgres_conn_id="redshift",
    sql=copy_sql.format(each_table,
                        's3://arxivs3/'+each_table+'/',DWH_ROLE)
    )

