import requests

# Define the API endpoint and parameters
endpoint = "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data"
params = {
    "stationTriplets": "12415500:ID:BOR",
    "format": "json"
}

# Send a GET request to the API
response = requests.get(endpoint, params=params)

# Check the response status code
if response.status_code == 200:
    # Get the JSON data
    data = response.json()

    # Print the station triplet information
    for station in data["stations"]:
        print(f"Station ID: {station['stationId']}")
        print(f"Station Name: {station['name']}")
        print(f"Network Code: {station['networkCd']}")
        print(f"Latitude: {station['latitude']}")
        print(f"Longitude: {station['longitude']}")
        print(f"Elevation: {station['elevation']} feet")
        print("---")
else:
    print(f"Error: {response.status_code} - {response.text}")