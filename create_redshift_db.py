import boto3
import time
from botocore.exceptions import ClientError

# Establish connection to Redshift
redshift = boto3.client('redshift')

# Declare parameters as variables
cluster_identifier = 'joes-capstone-cluster'
database = 'joescapstonedb'
user = 'billybob'
password = 'Capstone69'
port = '5439'

def create_redshift_cluster():
    try:
        response = redshift.create_cluster(
            ClusterIdentifier=cluster_identifier,
            NodeType='dc2.large',
            MasterUsername=user,
            MasterUserPassword=password,
            DBName=database,
            ClusterType='single-node',
            AvailabilityZone='us-west-2a',
            IamRoles=[
                'arn:aws:iam::459793645210:role/service-role/AmazonRedshift-CommandsAccessRole-20231031T191434',
                'arn:aws:iam::459793645210:role/spectrumRole',
                'arn:aws:iam::459793645210:role/aws-service-role/redshift.amazonaws.com/AWSServiceRoleForRedshift'
            ],
            PubliclyAccessible=True
        )
        print("Redshift cluster created successfully!")
        return response
    except ClientError as e:
        if e.response['Error']['Code'] == 'ClusterAlreadyExists':
            print("Redshift cluster already exists. Continuing...")
        else:
            raise

# Create Redshift cluster (call the function)
create_redshift_cluster()

# Check the status of the cluster until it becomes available
while True:
    response = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)
    cluster_status = response['Clusters'][0]['ClusterStatus']
    print(f"Cluster status: {cluster_status}")
    if cluster_status == 'available':
        break
    time.sleep(60)  # Wait for 60 seconds 