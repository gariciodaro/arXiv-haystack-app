# -*- coding: utf-8 -*-

import datetime
import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

def test_python(*args, **kwargs):
    print("i am running!!")
    for each in range(120):
        print("fuck")
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

spark_task = BashOperator(
    task_id='spark_tasks',
    dag=dag,
    bash_command='python /home/gari/Desktop/final_project/scripts/pysparkCreateParquets.py'
)
python_test=PythonOperator(
    task_id='python_test',
    dag=dag,
    python_callable=test_python
)
"""
run_this = BashOperator(
    task_id='run_after_loops',
    bash_command='echo 1',
    dag=dag,
)
"""

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
end_operator   = DummyOperator(task_id='Stop_execution' ,  dag=dag)

"""
start_operator>>spark_task
spark_task>>end_operator
"""

start_operator >> [spark_task,python_test]
[spark_task,python_test] >> end_operator