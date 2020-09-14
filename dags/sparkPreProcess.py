# -*- coding: utf-8 -*-

import datetime
import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.contrib.operators.ssh_operator import SSHOperator


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

upload_spark_task = BashOperator(
    task_id='upload_spark_script',
    dag=dag,
    bash_command='scp -i ~/keypair.pem /pysparkCreateParquets.py hadoop@ec2-54-212-140-28.us-west-2.compute.amazonaws.com:/home/hadoop/'
    )

execute_spark_task = SSHOperator(
    ssh_conn_id='aws_emr',
    task_id='execute_spark_task',
    command='/usr/bin/spark-submit --master yarn ./pysparkCreateParquets.py',
    dag=dag
    )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
end_operator   = DummyOperator(task_id='Stop_execution' ,  dag=dag)

start_operator >> upload_spark_task
upload_spark_task >> execute_spark_task
execute_spark_task >> end_operator