# -*- coding: utf-8 -*-
"""
Created on Sat Sept 13 2020
@author: gari.ciodaro.guerra
DAG of AirFlow to create star schema of 
in Redshift. Run on demand.
"""

import datetime
from airflow.operators.postgres_operator import PostgresOperator
from airflow import DAG

#Default configuration for DAG
#prevent parallelized process with max_active_runs
dag = DAG('create_tables_redshift',
          start_date=datetime.datetime.now(),
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
    journal-ref varchar(256) NOT NULL,
    license varchar(256) NOT NULL,
    report-no varchar(256) NOT NULL,
    submitter varchar(256) NOT NULL,
    title varchar(256) NOT NULL,
    update_date varchar(256) NOT NULL,
    versions varchar(256) NOT NULL,
    CONSTRAINT papers_pk PRIMARY KEY (id)
    ) diststyle even;
    DROP TABLE IF EXISTS public.authors; 
    CREATE TABLE public.authors (
        pk_author INT IDENTITY(0,1) PRIMARY KEY,
        id varchar(20) NOT NULL,
        author varchar(256) NOT NULL
    ) diststyle all;
    DROP TABLE IF EXIST public.abstracts;
    CREATE TABLE public.abstracts (
        id varchar(20) NOT NULL PRIMARY KEY,
        abstract varchar(256) NOT NULL,
    ) diststyle auto;
    DROP TABLE IF EXIST public.categories;
    CREATE TABLE public.categories (
        pk_categories INT IDENTITY(0,1) PRIMARY KEY,
        id varchar(20) NOT NULL,
        category varchar(256) NOT NULL
    ) diststyle all;
    DROP TABLE IF EXIST public.versions;
    CREATE TABLE public.versions (
        pk_versions INT IDENTITY(0,1) PRIMARY KEY,
        id varchar(20) NOT NULL,
        version varchar(10) NOT NULL,
        month varchar(10) NOT NULL,
        year INT NOT NULL
    ) diststyle all;
    DROP TABLE IF EXIST public.titles;
    CREATE TABLE public.titles (
        id varchar(20) NOT NULL PRIMARY KEY,
        id title(256) NOT NULL,
    ) diststyle all;
    COMMIT;
    """),
)