# -*- coding: utf-8 -*-
"""
Created on Sat Aug 22 2020
@author: gari.ciodaro.guerra
Custom Postgress operator to perform data quality check.
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

class DataQualityOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 table_list,
                 redshift_conn_id,
                 dq_checks,
                 *args, **kwargs):
        """ Operator constructor
        Parameters
        ----------
        table_list : list
        redshift_conn_id : string
            conection id of redshift. Configured on Airflow web server.
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table_list = table_list
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        for table in self.table_list:
            for check in self.dq_checks:
                sql = check.get('check_sql')
                exp_result = check.get('fail')
                self.log.info(f'Begin data quality on table: {table} ')
                result = redshift_hook.get_records(sql.format(table))[0][0]
                if eval(exp_result):
                    raise ValueError(f"Quality check failed. {table}. test: {exp_result}")
                logging.info(f"Quality check passed. {table} has {result} records")