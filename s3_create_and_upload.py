# This script will create an S3 bucket and upload all .csv files in a specified directory to the bucket.
# You will need to have the AWS CLI installed and configured on your machine for this to work.

import boto3
import logging
import os
from botocore.exceptions import ClientError
from datetime import datetime
from config import S3_BUCKET_NAME, DIRECTORY
import config

# Declare your variables
nwis_directory = os.path.join(DIRECTORY, 'output')
s3 = boto3.resource('s3') # <-- Do not alter this variable.
bucket_name = S3_BUCKET_NAME
directory = DIRECTORY
files = os.listdir(directory) # <-- Do not alter this variable.
region = 'us-west-2'

# Define the function to create a bucket
def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :return: True if bucket created, else False
    """

    # Creates a bucket with some error handling. If the bucket already exists, it will print an error message.
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True

# Call the create_bucket function
create_bucket(bucket_name, region)

# Get the list of files in each directory
files = os.listdir("./output")

for file in files:
    if file.endswith('.json') or file.endswith('.csv'):
        file_path = os.path.join(nwis_directory, file)
        
        # Get the current date and format it as 'YYYY/MM/DD'
        current_date = datetime.now()

        # Get the filename without the extension
        filename_without_extension = os.path.splitext(file)[0]

        # Include the filename in the path structure
        path_structure = 'nwis/' + current_date.strftime('%Y/%m/%d/') + filename_without_extension + '/' + file
        
        # Upload the file
        with open(file_path, 'rb') as data:
            s3.Object(bucket_name, path_structure).put(Body=data)
            print(f'Uploaded {file} to {bucket_name} at {path_structure}')
# # Upload each .json or .csv file in the first directory to the bucket
# for file in files1:
#     if file.endswith('.json') or file.endswith('.csv'):
#         file_path = os.path.join(nwis_directory, file)
        
#         # Get the current date and format it as 'YYYY/MM/DD'
#         current_date = datetime.now()
#         nwis_path_structure = 'nwis/' + current_date.strftime('%Y/%m/%d/') + file
        
#         # Upload the file
#         with open(file_path, 'rb') as data:
#             s3.Object(bucket_name, nwis_path_structure).put(Body=data)
#             print(f'Uploaded {file} to {bucket_name} at {nwis_path_structure}')

# # Upload each .json or .csv file in the second directory to the bucket
# for file in files2:
#     if file.endswith('.json') or file.endswith('.csv'):
#         file_path = os.path.join(nrcs_directory, file)
        
#         # Get the current date and format it as 'YYYY/MM/DD'
#         current_date = datetime.now()
#         nrcs_path_structure = 'nrcs/' + current_date.strftime('%Y/%m/%d/') + file

        # Set the S3_NRCS_PREFIX variable in the config.py file to the path structure above
        # config.S3_NRCS_PREFIX = path_structure
        
        # Upload the file
        # with open(file_path, 'rb') as data:
        #     s3.Object(bucket_name, path_structure).put(Body=data)
        #     print(f'Uploaded {file} to {bucket_name} at {path_structure}')