# List of columns to keep
columns_to_keep = ['agency_cd', 'site_no', 'station_nm', 'site_tp_cd', 'lat_va', 'long_va', 'district_cd', 'state_cd', 'county_cd', 'country_cd']

# List of all columns in the table
all_columns = [
    'agency_cd',
    'site_no',
    'station_nm',
    'site_tp_cd',
    'lat_va',
    'long_va',
    'dec_lat_va',
    'dec_long_va',
    'coord_meth_cd',
    'coord_acy_cd',
    'coord_datum_cd',
    'dec_coord_datum_cd',
    'district_cd',
    'state_cd',
    'county_cd',
    'country_cd',
    'land_net_ds',
    'map_nm',
    'map_scale_fc',
    'alt_va',
    'alt_meth_cd',
    'alt_acy_va',
    'alt_datum_cd',
    'huc_cd',
    'basin_cd',
    'topo_cd',
    'instruments_cd',
    'construction_dt',
    'inventory_dt',
    'drain_area_va',
    'contrib_drain_area_va',
    'tz_cd',
    'local_time_fg',
    'reliability_cd',
    'gw_file_cd',
    'nat_aqfr_cd'
]

# Derive the table name from the filename
table_name = '06186500_site_info_schema'  # replace with your actual table name

# Generate the ALTER TABLE commands
alter_table_commands = []
for column in all_columns:
    if column not in columns_to_keep:
        alter_table_commands.append(f"ALTER TABLE {table_name} DROP COLUMN {column};")

# Print the ALTER TABLE commands
for command in alter_table_commands:
    print(command)