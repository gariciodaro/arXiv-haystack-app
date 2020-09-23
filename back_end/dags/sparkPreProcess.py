# -*- coding: utf-8 -*-
"""
Created on Sat Sept 13 2020
@author: gari.ciodaro.guerra
DAG of AirFlow to upload from local machine
scripts/pysparkCreateParquets_TEMP.py, to execute it on a
EMR pyspark cluster..
"""

import datetime
import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.contrib.operators.ssh_operator import SSHOperator
from  airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.sftp_operator import SFTPOperator
import configparser

config = configparser.ConfigParser()
#local path the folder scripts
PATH_SCRIPS = config.get("APP","PATH_SCRIPS")

ssh_hook = SSHHook(ssh_conn_id="ssh_emr")

args = {
    'owner': 'Gari',
    'start_date': days_ago(2),
    'catchup': False,
    'depends_on_past':False
}

dag = DAG(
        dag_id='create_parquet_area',
        default_args=args,
        schedule_interval=None
        )

copy_spark_task = SFTPOperator(
    task_id="spark_job_to_emr",
    ssh_hook=ssh_hook,
    local_filepath=PATH_SCRIPS+"scripts/pysparkCreateParquets_TEMP.py",
    remote_filepath="/home/hadoop/pysparkCreateParquets_TEMP.py",
    operation="put",
    create_intermediate_dirs=True,
    dag=dag
)

execute_spark_task = SSHOperator(
    ssh_hook=ssh_hook,
    task_id='execute_spark_task',
    command='/usr/bin/spark-submit --master yarn ./pysparkCreateParquets_TEMP.py',
    dag=dag
    )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
end_operator   = DummyOperator(task_id='Stop_execution' ,  dag=dag)

start_operator >> copy_spark_task
copy_spark_task >> execute_spark_task
execute_spark_task >> end_operator