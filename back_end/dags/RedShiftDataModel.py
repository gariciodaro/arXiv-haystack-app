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
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import  DataQualityOperator
from airflow.models import Variable

# get role name from variables of ariflow.
DWH_ROLE=Variable.get("AWS_ROLE_S3_DWH")
S3_BUCKED    = Variable.get("S3_BUCKED")

copy_sql = ("""
    COPY {} 
    FROM '{}'
    IAM_ROLE '{}'
    FORMAT AS PARQUET
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

# quality checks operator
dq_checks=[
{'check_sql':'SELECT COUNT(*) FROM {}' , 'fail':'result<1'},
{'check_sql':'SELECT COUNT(*) FROM {} where id is null' , 'fail':'result>100'}
]
run_quality_checks = DataQualityOperator(
    table_list=list_tables,
    redshift_conn_id = 'redshift',
    task_id='Run_data_quality_checks',
    dq_checks=dq_checks,
    dag=dag
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
end_operator   = DummyOperator(task_id='Stop_execution' ,  dag=dag)


for each_table in list_tables:
    temp_oper=PostgresOperator(
    task_id="loading_table_"+each_table,
    dag=dag,
    postgres_conn_id="redshift",
    sql=copy_sql.format(each_table,
                        's3://{}/{}/'.format(S3_BUCKED,each_table),DWH_ROLE)
    )
    start_operator>>temp_oper
    temp_oper>>run_quality_checks

run_quality_checks>>end_operator