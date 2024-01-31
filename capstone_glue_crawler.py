import boto3
import time
# from config import S3_BUCKET_NAME, DATABASE_NAME, TABLE_NAME
# from s3_create_and_upload import nrcs_path_structure, nwis_path_structure

def create_crawler(glue_client, crawler_name, s3_path, database_name):
    try:
        glue_client.create_crawler(
            Name=crawler_name,
            Role='arn:aws:iam::459793645210:role/service-role/AWSGlueServiceRole',  # Use your Glue IAM role ARN
            DatabaseName=database_name,
            Targets={
                'S3Targets': [{'Path': s3_path}]
            },
        )
        return crawler_name
    except Exception as e:
        print(f"Error creating crawler: {e}")
        return None

def start_crawler(glue_client, crawler_name):
    try:
        glue_client.start_crawler(Name=crawler_name)
        print(f"Started crawler: {crawler_name}")
    except Exception as e:
        print(f"Error starting crawler: {e}")

def wait_for_crawler(glue_client, crawler_name):
    while True:
        try:
            response = glue_client.get_crawler(Name=crawler_name)
            status = response['Crawler']['State']
            print(f"Crawler {crawler_name} status: {status}")

            if status == 'READY':
                break
        except KeyError:
            print("Error: 'Crawler' key not found in the response.")
            break
        except glue_client.exceptions.EntityNotFoundException:
            print(f"Crawler {crawler_name} not found. Possibly not created yet.")
            break
        except Exception as e:
            print(f"Error while checking crawler status: {e}")

        time.sleep(60)

def main():
    try:
        # AWS Glue parameters
        region = 'us-west-2'  # Region to run the crawler in
        database_name = 'joecapstonedb'  # Replace with your Glue database name
        s3_bucket = 'joes-cap-stone-bucket'  # Name of S3 bucket data is stored in
        # Boto3 Glue client
        glue_client = boto3.client('glue', region_name=region)

        # Create and start the crawler
        crawler_name = 'capstone-crawler'
        s3_path = f's3://{s3_bucket}/nwis/'
        crawler_name = create_crawler(glue_client, crawler_name, s3_path, database_name)
        start_crawler(glue_client, crawler_name)

        # Wait for the crawler to finish
        wait_for_crawler(glue_client, crawler_name)
        print("Crawling completed successfully!")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    main()