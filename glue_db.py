import boto3
from config import DATABASE_NAME

def create_database(glue_client, database_name):
    try:
        glue_client.create_database(
            DatabaseInput={
                'Name': database_name
            }
        )
        print(f"Created database: {database_name}")
    except Exception as e:
        print(f"Error creating database: {e}")

def main():
    region = 'us-west-2'  # Replace with your AWS region
    database_name = DATABASE_NAME
    # Boto3 Glue client
    glue_client = boto3.client('glue', region_name=region)

    # Create the database
    create_database(glue_client, database_name)

if __name__ == '__main__':
    main()