# -*- coding: utf-8 -*-
"""
Created on Thu July 30 2020
@author: gari.ciodaro.guerra
Create bucket on AWS
"""

import boto3 
import configparser

config = configparser.ConfigParser()

# AWS credentials
config.read('/home/gari/.aws/credentials')
KEY      = config.get('credentials','KEY')
SECRET   = config.get('credentials','SECRET')

def main():
    s3 = boto3.client('s3',region_name='us-west-2',
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)
    response=s3.create_bucket(
                Bucket='arxivs3',
                CreateBucketConfiguration={'LocationConstraint':'us-west-2'})
    print(response)

if __name__ == "__main__":
    main()