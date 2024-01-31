import dataretrieval.nwis as nwis
import pandas as pd
from io import StringIO
import matplotlib.pyplot as plt
from config import DIRECTORY
import os
import csv

# specify the range of dates for which we want data.
start = '2008-04-01'
end = '2008-07-01'
# specify the USGS site code and service for which we want data.
sites = ['06191500', '06186500', '06192500']
service = 'iv'

# Create the output directory if it doesn't exist
output_dir = os.path.join(DIRECTORY, 'output')
os.makedirs(output_dir, exist_ok=True)

for site in sites:
    # get instantaneous values (iv)
    df = nwis.get_record(sites=site, service=service, start= start, end= end, parameterCd='00060')

    # get basic info about the site
    df3 = nwis.get_record(sites=site, service='site')

    # Convert the DataFrames to dictionaries
    df_dict = df.to_dict('records')
    df3_dict = df3.to_dict('records')

    # Get the keys (column names) from the first dictionary in the list
    keys_df = df_dict[0].keys()
    keys_df3 = df3_dict[0].keys()

    # Write the dictionaries to CSV files
    with open(os.path.join(output_dir, f'{site}_test.csv'), 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys_df)
        dict_writer.writeheader()
        dict_writer.writerows(df_dict)

    with open(os.path.join(output_dir, f'{site}_site_info.csv'), 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys_df3)
        dict_writer.writeheader()
        dict_writer.writerows(df3_dict)

# Save the DataFrames to CSV files in the output directory
# df.to_csv(os.path.join(nwis_directory, 'test.csv'), index=False)
# df3.to_csv(os.path.join(nwis_directory, 'site_info.csv'), index=False)

# Plot the dataframe
# df.plot(title=f'Flow at USGS Site {site}')
# plt.xlabel('Date')
# plt.ylabel('Flow Rate (cfs)')
# plt.show()
