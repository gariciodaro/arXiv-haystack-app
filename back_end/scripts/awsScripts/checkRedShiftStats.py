# -*- coding: utf-8 -*-
"""
Created on Sept 5 2020
@author: gari.ciodaro.guerra
Auxliar script to check redshift status
"""

import boto3
import configparser
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.hooks.aws_hook import AwsHook

aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()
# get reshift cluster id
connection = BaseHook.get_connection("redshift")

KEY      = credentials.access_key
SECRET   = credentials.secret_key
REGION   = BaseHook.get_connection("aws_credentials").extra_dejson['region']
DWH_CLUSTER_IDENTIFIER = connection.host.split(".")[0]

#initilizied redshift client
# other region us-east-2
redshift = boto3.client('redshift',
                        region_name=REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

cluster_props=redshift.describe_clusters(
    ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

if __name__=="__main__":
    if cluster_props["ClusterStatus"]=="available":
        print("DWH_ENDPOINT ",cluster_props['Endpoint']['Address'])
        print("DWH_ROLE_ARN ",cluster_props['IamRoles'][0]['IamRoleArn'])
    else:
        print(cluster_props["ClusterStatus"])