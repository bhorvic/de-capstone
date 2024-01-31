import boto3
import logging
from botocore.exceptions import ClientError
from config import S3_BUCKET_NAME, DIRECTORY, DATABASE_NAME, TABLE_NAME

def create_glue_database_and_crawler_csv():
    # Initialize the Glue client
    glue_client = boto3.client('glue')

    # Create a CSV crawler
    crawler_name = 'csv_crawler'
    create_crawler_response = glue_client.create_crawler(
        Name=crawler_name,
        Role='AWSGlueServiceRoleDefault',
        DatabaseName=DATABASE_NAME,
        Targets={'S3Targets': [{'Path': f's3://{S3_BUCKET_NAME}/{DIRECTORY}/csv/'}]},
        SchemaChangePolicy={'UpdateBehavior': 'UPDATE_IN_DATABASE', 'DeleteBehavior': 'DEPRECATE_IN_DATABASE'}
    )

    # Run the CSV crawler
    glue_client.start_crawler(Name=crawler_name).wait()
    print(f"Crawler '{crawler_name}' finished.")

    print("CSV Glue Database and Crawler creation completed.")

if __name__ == "__main__":
    create_glue_database_and_crawler_csv()
