# first import the functions for downloading data from NWIS
import dataretrieval.nwis as nwis
import pandas as pd



# specify the USGS site code for which we want data.
site = '06191500'


# get daily discharge values (dv)
df = nwis.get_record(sites=site, service='dv', start='2022-06-01', end='2022-07-15')
df.to_csv('output/yellowstone_flood.csv', index=False)