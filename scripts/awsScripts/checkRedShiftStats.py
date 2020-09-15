import boto3
import configparser

config = configparser.ConfigParser()

# AWS credentials
config.read('/home/gari/.aws/credentials')
KEY      = config.get('credentials','KEY')
SECRET   = config.get('credentials','SECRET')
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

#initilizied redshift client
# other region us-east-2
redshift = boto3.client('redshift',
                        region_name='us-west-2',
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