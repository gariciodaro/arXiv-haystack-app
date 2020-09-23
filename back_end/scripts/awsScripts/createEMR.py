# -*- coding: utf-8 -*-
"""
Created on Thu July 30 2020
@author: gari.ciodaro.guerra
Create EMR instance
with Spark.
"""

import boto3 
import configparser

config = configparser.ConfigParser()

# AWS credentials
config.read('/home/gari/.aws/credentials')
KEY      = config.get('credentials','KEY')
SECRET   = config.get('credentials','SECRET')
REGION   = config.get('credentials','REGION')

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