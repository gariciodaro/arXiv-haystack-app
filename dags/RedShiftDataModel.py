# -*- coding: utf-8 -*-

import datetime
import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.postgres_operator import PostgresOperator


aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()

copy_sql = ("""
    COPY {} FROM '{}'
    FORMAT AS PARQUET
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    region 'us-west-2';
""")

list_tables=['papers','authors','abstracts','categories','versions','titles']

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
    postgres_conn_id="redshift_conn_id",
    sql=copy_sql.format(each_table,
                        's3a://arxivs3/input_data/'+each_table,
                        credentials.access_key, 
                        credentials.secret_key)
    )

