# -*- coding: utf-8 -*-
"""
Created on Sat Sept 13 2020
@author: gari.ciodaro.guerra
DAG of AirFlow tables schemas for Arxiv database. 
Run on demand.
"""

import datetime
from airflow.operators.postgres_operator import PostgresOperator
from airflow import DAG
from airflow.utils.dates import days_ago

args = {
    'owner': 'arXiv-haystack-app',
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
    id          varchar(256) NOT NULL,
    title       varchar(256) NOT NULL,
    categories  varchar(256) NULL,
    doi         varchar(256) NULL,
    comments    varchar(256) NULL,
    journalref  varchar(256) NULL,
    license     varchar(256) NULL,
    reportno    varchar(256) NULL,
    submitter   varchar(256) NULL,
    updatedate  varchar(256) NULL
    ) diststyle even;
    DROP TABLE IF EXISTS public.authors; 
    CREATE TABLE public.authors (
        id varchar(256) NOT NULL,
        author varchar(256) NOT NULL
    ) diststyle all;
    DROP TABLE IF EXISTS public.abstracts;
    CREATE TABLE public.abstracts (
        id varchar(256) NOT NULL PRIMARY KEY,
        abstract varchar(2048) NOT NULL,
        origin  varchar(256) NOT NULL
    ) diststyle auto;
    DROP TABLE IF EXISTS public.categories;
    CREATE TABLE public.categories (
        id varchar(256) NOT NULL,
        category varchar(256) NOT NULL
    ) diststyle all;
    DROP TABLE IF EXISTS public.versions;
    CREATE TABLE public.versions (
        id varchar(256) NOT NULL,
        created varchar(256) NOT NULL,
        version varchar(10) NOT NULL,
        month varchar(10) NOT NULL,
        year varchar(10) NOT NULL
    ) diststyle all;
    DROP TABLE IF EXISTS public.titles;
    CREATE TABLE public.titles (
        id varchar(256) NOT NULL PRIMARY KEY,
        title varchar(256) NOT NULL
    ) diststyle all;
    COMMIT;
    """),
)
