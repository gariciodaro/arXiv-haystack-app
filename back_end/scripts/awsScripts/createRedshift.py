# -*- coding: utf-8 -*-
"""
Created on Sept 5 2020
@author: gari.ciodaro.guerra
Auxliar script to cleate a redshift cluster
"""

import argparse
import boto3
import configparser

config = configparser.ConfigParser()

# AWS credentials
config.read('/home/gari/.aws/credentials')
KEY      = config.get('credentials','KEY')
SECRET   = config.get('credentials','SECRET')
REGION   = config.get('credentials','REGION')


DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

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