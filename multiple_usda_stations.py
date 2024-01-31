import requests
from requests.exceptions import RequestException
import os
import csv
from config import DIRECTORY

# Define the base URL for the AWDB REST API
base_url = 'https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1'

# Define the endpoint and parameters for the getData method
endpoint = '/data'
# Define a variable
stationTriplets = ['365:MT:SNTL', '789:OR:SNTL']

# Use the variable in the params dictionary
params = {
    'elements': 'WTEQ',
    'elementCd': 'in',
    'ordinal': 1,
    'heightDepth': ',',
    'duration': 'DAILY',
    'getFlags': True,
    'beginDate': '2023-01-01 01:00',
    'endDate': '2023-12-18 01:00',
    'stationTriplets': stationTriplets,
}

# Loop over each station triplet
for stationTriplet in stationTriplets:
    # Update the 'stationTriplets' parameter for the current station triplet
    params['stationTriplets'] = stationTriplet

    # Send a GET request to the API
    response = requests.get(base_url + endpoint, params=params)

    try:
        if response.status_code == 200:
            # Parse the JSON data from the response
            data = response.json()

            # Check if the output directory exists and create it if it doesn't
            output_dir = os.path.join(DIRECTORY, 'output')
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            # Write the data to a CSV file
            safe_stationTriplet = stationTriplet.replace(':', '_')  # Replace colons with underscores
            output_file_path = os.path.join(output_dir, f'{safe_stationTriplet}_station_data.csv')
            # Extract the relevant information from the JSON data
            records = []
            for item in data:
                station_triplet = item.get('stationTriplet', '')
                for entry in item.get('data', []):
                    station_element = entry.get('stationElement', {})
                    values = entry.get('values', [])
                    for value_entry in values:
                        record = {
                            'stationTriplet': station_triplet,
                            'elementCode': station_element.get('elementCode', ''),
                            'ordinal': station_element.get('ordinal', ''),
                            'heightDepth': station_element.get('heightDepth', ''),
                            'durationName': station_element.get('durationName', ''),
                            'dataPrecision': station_element.get('dataPrecision', ''),
                            'storedUnitCode': station_element.get('storedUnitCode', ''),
                            'originalUnitCode': station_element.get('originalUnitCode', ''),
                            'beginDate': station_element.get('beginDate', ''),
                            'endDate': station_element.get('endDate', ''),
                            'value_date': value_entry.get('date', ''),
                            'value': value_entry.get('value', ''),
                        }
                        records.append(record)

            # Write to CSV
            with open(output_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = records[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                # Write header
                writer.writeheader()

                # Write data
                for record in records:
                    writer.writerow(record)

            print(f"Data saved to {output_file_path}")
        else:
            print(f"Error: {response.status_code}, {response.text}")
    except RequestException as e:
        print(f"An error occurred during the request: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
