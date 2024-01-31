A more in-depth summary of the project can be found in the usda_analytics.ipynb file 

This project is designed to collect USGS streamflow and SNOTEL data through their respective REST APIs. The collected data, initially in JSON format, is then converted to CSV files and stored in a local directory. Subsequently, the CSV files are loaded into an AWS S3 bucket using the boto3 Python library.

Workflow
Data Collection:

Utilizes the USGS streamflow and SNOTEL REST APIs to fetch data in JSON format.
Converts the JSON files to CSV for easier handling and analysis.

Storage:
Stores the converted CSV files locally for quick access.

AWS S3 Integration:
Uploads the CSV files to an AWS S3 bucket using the boto3 library.

AWS Glue Integration:
Creates a Glue crawler using boto3 to crawl the S3 bucket and infer schema for a Glue database.
Designates the S3 bucket as a data lake for seamless integration.

AWS Redshift Database:
Creates a Redshift database using boto3 for data storage and querying.
Defines external tables in Redshift that reference the data lake location in the S3 bucket.
Loads data from the data lake into the Redshift database.
