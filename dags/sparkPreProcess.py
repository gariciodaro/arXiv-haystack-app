# -*- coding: utf-8 -*-

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
"""
upload_spark_task = BashOperator(
    task_id='upload_spark_script',
    dag=dag,
    bash_command='scp -i ~/.aws/keypairspark.pem /home/gari/Desktop/final_project/scripts/pysparkCreateParquets_TEMP.py hadoop@ec2-52-33-207-156.us-west-2.compute.amazonaws.com:/home/hadoop/'
    )
"""

copy_spark_task = SFTPOperator(
    task_id="spark_job_to_emr",
    ssh_hook=ssh_hook,
    local_filepath="/home/gari/Desktop/final_project/scripts/pysparkCreateParquets.py",
    remote_filepath="/home/hadoop/pysparkCreateParquets.py",
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