import json
import os

# Get the directory that this script is located in
dir_path = os.path.dirname(os.path.realpath(__file__))

# Get a list of all JSON files in the directory
json_files = [os.path.join(dir_path, f) for f in os.listdir(dir_path) if f.endswith('.json')]

# Open the output file
with open(os.path.join(dir_path, 'create_tables.sql'), 'w') as output_file:
    # Iterate over the JSON files
    for json_file in json_files:
        # Load the JSON schema
        with open(json_file, 'r') as input_file:
            schema = json.load(input_file)

        # Derive the table name from the filename
        table_name = os.path.splitext(os.path.basename(json_file))[0]

        # Convert the JSON schema to a SQL CREATE EXTERNAL TABLE statement
        columns = ',\n'.join([f'    {column["Name"]} VARCHAR' if column["Type"] == 'string' else f'    {column["Name"]} {column["Type"]}' for column in schema])
        create_table_statement = f'CREATE EXTERNAL TABLE {table_name} (\n{columns}\n)\nROW FORMAT DELIMITED\nFIELDS TERMINATED BY \',\'\nSTORED AS TEXTFILE\nLOCATION \'s3://joes-cap-stone-bucket/nwis/\';\n\n'

        # Write the CREATE EXTERNAL TABLE statement to the output file
        output_file.write(create_table_statement)