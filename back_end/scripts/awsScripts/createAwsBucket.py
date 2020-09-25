# -*- coding: utf-8 -*-
"""
Created on Thu July 30 2020
@author: gari.ciodaro.guerra
Auxliar script to create bucket on AWS
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
    s3 = boto3.client('s3',region_name=REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)
    response=s3.create_bucket(
                Bucket='arxivs3',
                CreateBucketConfiguration={'LocationConstraint':REGION})
    print(response)

if __name__ == "__main__":
    main()

