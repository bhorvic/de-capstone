import boto3
import json
import re

# Declare some variables to initialize the boto3 clients
rds_data_client = boto3.client('rds-data')
kinesis_client = boto3.client('kinesis')
firehose_client = boto3.client('firehose')
s3_client = boto3.client('s3')

# This is a function to create a Kinesis Data Stream
def create_kinesis_data_stream(stream_name, shard_count):
    try:
        kinesis_client.create_stream(StreamName=stream_name, ShardCount=shard_count)
        print(f"Kinesis Data Stream {stream_name} created with {shard_count} shards.")
    
    except Exception as e:
        print(f"Error creating Kinesis Data Stream: {e}")

    # Wait for the Kinesis Data Stream to become ACTIVE
    kinesis_stream_name = f'{stream_name}'
    kinesis_stream_waiter = kinesis_client.get_waiter('stream_exists')
    kinesis_stream_waiter.wait(StreamName=kinesis_stream_name)
    print(f"Kinesis Data Stream {kinesis_stream_name} is ACTIVE.")

# This is a function to ingest records into Kinesis Data Stream as JSON
def records_ingest(data_stream, records):
    try:    
        processed_records = []

        for i in range(0, len(records), 4):
            record = {
                "agency": json.loads(records[i])["stringValue"],
                "id": json.loads(records[i+1])["stringValue"],
                "date": json.loads(records[i+2])["stringValue"],
                "value": json.loads(records[i+3])["longValue"]
            }
            processed_records.append(record)
        # Convert the processed records into JSON format
        json_data = json.dumps(processed_records)
        loaded_data = json.loads(json_data)
        
        kinesis_records = [{"Data": json.dumps(i), "PartitionKey": str(i.get("id"))} for i in loaded_data]
        
        # print(kinesis_records) # <-- Can be used to verify that json_data is in JSON format

        kinesis_client.put_records(StreamName=data_stream, Records=kinesis_records)
    except Exception as e:
        print(f"Error ingesting records into Kinesis Data Stream: {e}")

# This is a function to create a Kinesis Firehose Delivery Stream that will deliver data to S3
def create_firehose_delivery_stream(firehose_name, s3_bucket):
    try:
        response = firehose_client.create_delivery_stream(
            DeliveryStreamName=firehose_name,
            DeliveryStreamType='KinesisStreamAsSource',
            KinesisStreamSourceConfiguration={
                'KinesisStreamARN': 'arn:aws:kinesis:us-west-2:459793645210:stream/special-stream',
                'RoleARN': 'arn:aws:iam::459793645210:role/service-role/KinesisFirehoseServiceRole-kinesis-fireh-us-west-2-1697150533931'  # Replace with your IAM Role ARN
            },
            S3DestinationConfiguration={
                'BucketARN': f'arn:aws:s3:::{s3_bucket}',
                'RoleARN': 'arn:aws:iam::459793645210:role/service-role/KinesisFirehoseServiceRole-kinesis-fireh-us-west-2-1697150533931'  # Replace with your IAM Role ARN
            }
        )
        print(f'Delivery Stream {firehose_name} created with ARN: {response["DeliveryStreamARN"]}')
    except Exception as e:
        print(f"Error creating Kinesis Firehose delivery stream: {e}")

# Declare some variables to be used in the functions
s3_bucket = 'waterdata-db-bucket'
stream_name = 'special-stream'
firehose_name = 'my-big-firehose'
shard_count = 1  # Adjust the shard count as needed


# This converts the data from 'streamdata.txt' into a list of dictionaries
with open('streamdata.txt', 'r') as f:
    for i in f:
        new_f = i.replace("'", '"')
        records = re.findall(r'(\{[^\}]+\})', new_f)

# Call the functions that were defined above
create_kinesis_data_stream(stream_name, shard_count)
records_ingest(stream_name, records)
create_firehose_delivery_stream(firehose_name, s3_bucket)