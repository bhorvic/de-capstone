# first import the functions for downloading data from NWIS
import dataretrieval.nwis as nwis

# specify the USGS site code for which we want data.
site = '06191500'


# get instantaneous values (iv)
df = nwis.get_record(sites=site, service='iv', start='2022-06-01', end='2022-07-01', parameterCd='00060')

# get water quality samples (qwdata)
# df2 = nwis.get_record(sites=site, service='qwdata', start='2022-12-31', end='2023-01-01')

# get basic info about the site
# df3 = nwis.get_record(sites=site, service='site')

# Save df to a CSV file
# df.to_csv('yellowstone.csv', index=False)  # Specify the file name you want


print(df)
# print(df2)
# print(df3)