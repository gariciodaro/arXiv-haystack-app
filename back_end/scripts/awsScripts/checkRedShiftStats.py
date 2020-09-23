# -*- coding: utf-8 -*-
"""
Created on Sept 5 2020
@author: gari.ciodaro.guerra
Auxliar script to check redshift status
"""

import boto3
import configparser

config = configparser.ConfigParser()

# AWS credentials
config.read('/home/gari/.aws/credentials')
KEY      = config.get('credentials','KEY')
SECRET   = config.get('credentials','SECRET')
REGION   = config.get('credentials','REGION')
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

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