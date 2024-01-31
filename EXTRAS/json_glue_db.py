import boto3
import logging
from botocore.exceptions import ClientError
from config import S3_BUCKET_NAME, DIRECTORY, DATABASE_NAME, TABLE_NAME

def create_glue_database_and_crawler_json():
    # Initialize the Glue client
    glue_client = boto3.client('glue')

    # Create a Glue database
    try:
        create_database_response = glue_client.create_database(
            DatabaseInput={'Name': DATABASE_NAME}
        )
        print(f"Database '{DATABASE_NAME}' created.")
    except ClientError as e:
        print(f"Failed to create database: {e}")
        return False

    # Create a JSON crawler
    crawler_name = 'json_crawler'
    try:
        create_crawler_response = glue_client.create_crawler(
            Name=crawler_name,
            Role='AWSGlueServiceRole',
            DatabaseName=DATABASE_NAME,
            Targets={'S3Targets': [{'Path': f's3://{S3_BUCKET_NAME}/{DIRECTORY}/json/'}]},
            SchemaChangePolicy={'UpdateBehavior': 'UPDATE_IN_DATABASE', 'DeleteBehavior': 'DEPRECATE_IN_DATABASE'}
        )
        print(f"Crawler '{crawler_name}' created.")

        # Run the JSON crawler
        glue_client.start_crawler(Name=crawler_name).wait()
        print(f"Crawler '{crawler_name}' finished.")
    except ClientError as e:
        print(f"Failed to create or run crawler: {e}")
        return False

    print("JSON Glue Database and Crawler creation completed.")
    return True

if __name__ == "__main__":
    create_glue_database_and_crawler_json()
