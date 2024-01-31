import os
import pandas as pd

# Walk through the current directory
for root, dirs, files in os.walk('.'):
    # Process each file
    for file in files:
        # Check if the file is a CSV file
        if file.endswith('.csv'):
            # Construct the full file path
            file_path = os.path.join(root, file)

            # Load the CSV file into a DataFrame
            df = pd.read_csv(file_path)

            # Display the DataFrame
            print(f"DataFrame for {file_path}:")
            print(df)
            print("\n")