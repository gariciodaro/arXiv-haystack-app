# -*- coding: utf-8 -*-
"""
Created on Thu July 30 2020
@author: gari.ciodaro.guerra
Create EMR instance
with Spark.
"""

import boto3 
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.base_hook import BaseHook

aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()

# AWS credentials
KEY      = credentials.access_key
SECRET   = credentials.secret_key
REGION   = BaseHook.get_connection("aws_credentials").extra_dejson['region']

def main():
    connection = boto3.client('emr',region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)

    cluster_id = connection.run_job_flow(
        Name='emr_spark',
        ReleaseLabel='emr-5.20.0',
        Applications=[
            {
                'Name': 'Spark'
            },
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': "Slave",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 2,
                }
            ],
            'Ec2KeyName': 'keypairspark',
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False
        },
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
    )
    print(cluster_id)

if __name__ == "__main__":
    main()