import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, String, Column, Integer

# Define the input and output directories
input_dir = 'output'
output_dir = os.path.join(input_dir, 'schema_files')

# Create the output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Get a list of all CSV files in the input directory
csv_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.csv')]

# Create a SQLAlchemy engine (replace with your actual database connection)
engine = create_engine('sqlite:///my_database.db')

# Create a metadata instance
metadata = MetaData()

# Iterate over the CSV files
for csv_file in csv_files:
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file)

    # Create a new table with the same columns as the DataFrame
    table_name = os.path.splitext(os.path.basename(csv_file))[0]  # Use the file name as the table name
    table = Table(
        table_name, metadata,
        *(Column(col_name, String()) for col_name in df.columns)
    )

    # Create the table
    metadata.create_all(engine)

    # Output the schema to a file
    with open(os.path.join(output_dir, f'{table_name}_schema.sql'), 'w') as f:
        for column in table.columns:
            f.write(f'{column.name} {column.type}\n')