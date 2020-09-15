# -*- coding: utf-8 -*-
"""
Created on Sat Sept 13 2020
@author: gari.ciodaro.guerra
DAG of AirFlow to create star schema of 
in Redshift. Run on demand.
"""

#DWH_ENDPOINT  dwhcluster.crh9wevfd8it.us-west-2.redshift.amazonaws.com
#DWH_ROLE_ARN  arn:aws:iam::384278250086:role/dwhRole

import datetime
from airflow.operators.postgres_operator import PostgresOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.hooks.aws_hook import AwsHook

#get AWS credentials from AirFlow webserver
aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()

args = {
    'owner': 'Gari',
    'start_date': days_ago(2),
    'catchup': False,
    'depends_on_past':False
}


#Default configuration for DAG
#prevent parallelized process with max_active_runs
dag = DAG('create_tables_redshift',
          default_args=args,
          description='create tables on redshift',
          max_active_runs=1,
          schedule_interval=None
        )
# set of queries for start schema.
create_table = PostgresOperator(
    task_id="create_tables",
    postgres_conn_id="redshift",
    dag=dag,
    sql=("""
    BEGIN;
    DROP TABLE IF EXISTS public.papers; 
    CREATE TABLE public.papers (
    abstract varchar(256) NOT NULL,
    authors varchar(256) NOT NULL,
    authors_parsed varchar(256) NOT NULL,
    categories varchar(256) NOT NULL,
    comments varchar(256) NOT NULL,
    doi varchar(256) NOT NULL,
    id varchar(20) NOT NULL,
    journal_ref varchar(256) NOT NULL,
    license varchar(256) NOT NULL,
    report_no varchar(256) NOT NULL,
    submitter varchar(256) NOT NULL,
    title varchar(256) NOT NULL,
    update_date varchar(256) NOT NULL,
    versions varchar(256) NOT NULL,
    CONSTRAINT papers_pk PRIMARY KEY (id)
    ) diststyle even;
    DROP TABLE IF EXISTS public.authors; 
    CREATE TABLE public.authors (
        id varchar(20) NOT NULL,
        author varchar(256) NOT NULL
    ) diststyle all;
    DROP TABLE IF EXISTS public.abstracts;
    CREATE TABLE public.abstracts (
        id varchar(20) NOT NULL PRIMARY KEY,
        abstract varchar(256) NOT NULL,
        origin  varchar(256) NOT NULL
    ) diststyle auto;
    DROP TABLE IF EXISTS public.categories;
    CREATE TABLE public.categories (
        id varchar(20) NOT NULL,
        category varchar(256) NOT NULL
    ) diststyle all;
    DROP TABLE IF EXISTS public.versions;
    CREATE TABLE public.versions (
        id varchar(20) NOT NULL,
        version varchar(10) NOT NULL,
        month varchar(10) NOT NULL,
        year INT NOT NULL
    ) diststyle all;
    DROP TABLE IF EXISTS public.titles;
    CREATE TABLE public.titles (
        id varchar(20) NOT NULL PRIMARY KEY,
        title varchar(256) NOT NULL
    ) diststyle all;
    COMMIT;
    """),
)