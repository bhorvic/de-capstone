import os

# Get the directory that this script is located in
dir_path = os.path.dirname(os.path.realpath(__file__))

# Get a list of all SQL files in the directory
sql_files = [os.path.join(dir_path, f) for f in os.listdir(dir_path) if f.endswith('.sql') and f != 'redshift_create_tables.sql']

try:
    # Open the output file
    with open(os.path.join(dir_path, 'redshift_create_tables.sql'), 'w') as output_file:
        # Iterate over the SQL files
        for sql_file in sql_files:
            try:
                # Open the SQL file
                with open(sql_file, 'r') as input_file:
                    # Read the column definitions
                    column_definitions = input_file.read()

                    # Derive the table name from the filename
                    table_name = os.path.splitext(os.path.basename(sql_file))[0]

                    # Create the CREATE TABLE command
                    create_table_command = f"CREATE TABLE {table_name} (\n{column_definitions}\n);"

                    # Write the CREATE TABLE command to the output file
                    output_file.write(create_table_command)
                    output_file.write('\n\n')  # add a couple of newlines for separation
            except Exception as e:
                print(f"Error reading file {sql_file}: {e}")
except Exception as e:
    print(f"Error writing to output file: {e}")