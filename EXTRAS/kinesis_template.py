import boto3

rds_data_client = boto3.client('rds-data')
kinesis_client = boto3.client('kinesis')
firehose_client = boto3.client('firehose')
s3_client = boto3.client('s3')


def create_s3_bucket(bucket_name):
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f'S3 Bucket {bucket_name} created successfully.')
    except Exception as e:
        print(f"Error creating S3 bucket: {e}")


def create_kinesis_data_stream(stream_name):
    try:
        kinesis_client.create_stream(StreamName=stream_name, ShardCount=1)
        print(f'Kinesis Data Stream {stream_name} created successfully.')
    except Exception as e:
        print(f"Error creating Kinesis data stream: {e}")


def create_firehose_delivery_stream(firehose_name, s3_bucket, data_stream_name):
    try:
        response = firehose_client.create_delivery_stream(
            DeliveryStreamName=firehose_name,
            DeliveryStreamType='KinesisStreamAsSource',
            KinesisStreamSourceConfiguration={
                'KinesisStreamARN': f'arn:aws:kinesis:<REGION>:<ACCOUNT_ID>:stream/{data_stream_name}',
                # Replace <REGION> and <ACCOUNT_ID>
                'RoleARN': 'YOUR_IAM_ROLE_ARN'  # Replace with your IAM Role ARN
            },
            S3DestinationConfiguration={
                'BucketARN': f'arn:aws:s3:::{s3_bucket}',
                'RoleARN': 'YOUR_IAM_ROLE_ARN'  # Replace with your IAM Role ARN
            }
        )
        print(f'Delivery Stream {firehose_name} created with ARN: {response["DeliveryStreamARN"]}')
    except Exception as e:
        print(f"Error creating Kinesis Firehose delivery stream: {e}")


def simulate_data_changes_in_aurora(cluster_arn, secret_arn, database):
    sql_insert = "INSERT INTO `your_table_name` (`column1`, `column2`) VALUES ('value1', 'value2')"
    sql_update = "UPDATE `your_table_name` SET `column2` = 'new_value2' WHERE `column1` = 'value1'"

    try:
        # Insert operation
        rds_data_client.execute_statement(
            resourceArn=cluster_arn,
            secretArn=secret_arn,
            database=database,
            sql=sql_insert
        )

        # Update operation
        rds_data_client.execute_statement(
            resourceArn=cluster_arn,
            secretArn=secret_arn,
            database=database,
            sql=sql_update
        )
    except Exception as e:
        print(f"Error executing SQL via rds-data: {e}")


# Usage:

bucket_name = 'YOUR_S3_BUCKET_NAME'
data_stream_name = 'YOUR_DATA_STREAM_NAME'
firehose_name = 'YOUR_FIREHOSE_NAME'
cluster_arn = "your-cluster-arn"
secret_arn = "your-secret-arn-for-db-credentials"
database_name = "your-database-name"

create_s3_bucket(bucket_name)
create_kinesis_data_stream(data_stream_name)
create_firehose_delivery_stream(firehose_name, bucket_name, data_stream_name)
simulate_data_changes_in_aurora(cluster_arn, secret_arn, database_name)
