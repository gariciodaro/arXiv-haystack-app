# -*- coding: utf-8 -*-
"""
Created on Sept 5 2020
@author: gari.ciodaro.guerra
Auxliar script to cleate a redshift cluster
"""

import argparse
import boto3
import configparser
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.base_hook import BaseHook

config = configparser.ConfigParser()

# AWS internal credentials
aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()
KEY      = credentials.access_key
SECRET   = credentials.secret_key
REGION   = BaseHook.get_connection("aws_credentials").extra_dejson['region']

# redshift configuration.
config.read('/home/gari/.aws/credentials')
DWH_CLUSTER_TYPE       = 'multi-node'
DWH_NUM_NODES          = 2
DWH_NODE_TYPE          = 'dc2.large'
DWH_CLUSTER_IDENTIFIER = 'dwhCluster'
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_IAM_ROLE_NAME      = 'dwhRole'


#initilizied iam client
# other region us-east-2
iam = boto3.client('iam',
                    region_name=REGION,
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET)

#initilizied redshift client
redshift = boto3.client('redshift',
                        region_name=REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

# python redshiftCluster.py --action create
# python redshiftCluster.py --action erase
if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", help="create or erase. redshift cluster")

    #parse input
    args = parser.parse_args()

    roleArn=iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    if args.action=="create":
        try:
            response = redshift.create_cluster(
                #HW
                ClusterType=DWH_CLUSTER_TYPE,
                NodeType=DWH_NODE_TYPE,
                NumberOfNodes=int(DWH_NUM_NODES),

                #Identifiers & Credentials
                DBName=DWH_DB,
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                MasterUsername=DWH_DB_USER,
                MasterUserPassword=DWH_DB_PASSWORD,
                
                #Roles (for s3 access)
                IamRoles=[roleArn]  
            )
        except Exception as e:
            print(e)


    if args.action=="erase":
        redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                                SkipFinalClusterSnapshot=True)

# remember to add inbound rule on port 5439 TCP from your IP.