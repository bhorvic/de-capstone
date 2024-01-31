CREATE EXTERNAL TABLE 06186500_site_info (
    agency_cd VARCHAR,
    site_no bigint,
    station_nm VARCHAR,
    site_tp_cd VARCHAR,
    lat_va double,
    long_va double,
    district_cd bigint,
    state_cd bigint,
    county_cd bigint,
    country_cd VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://joes-cap-stone-bucket/nwis/';

CREATE EXTERNAL TABLE 06186500_test (
    intantaneous_flow_cfs double,
    data_status VARCHAR,
    site_no VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://joes-cap-stone-bucket/nwis/';

CREATE EXTERNAL TABLE 06191500_site_info (
    agency_cd VARCHAR,
    site_no bigint,
    station_nm VARCHAR,
    site_tp_cd VARCHAR,
    lat_va double,
    long_va double,
    district_cd bigint,
    state_cd bigint,
    county_cd bigint,
    country_cd VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://joes-cap-stone-bucket/nwis/';

CREATE EXTERNAL TABLE 06191500_test (
    instantaneous_flow_cfs double,
    data_status VARCHAR,
    site_no VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://joes-cap-stone-bucket/nwis/';

CREATE EXTERNAL TABLE 06192500_site_info (
    agency_cd VARCHAR,
    site_no bigint,
    station_nm VARCHAR,
    site_tp_cd VARCHAR,
    lat_va double,
    long_va double,
    district_cd bigint,
    state_cd bigint,
    county_cd bigint,
    country_cd VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://joes-cap-stone-bucket/nwis/';

CREATE EXTERNAL TABLE 06192500_test (
    instantaneous_flow_cfs double,
    data_status VARCHAR,
    site_no VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://joes-cap-stone-bucket/nwis/';

CREATE EXTERNAL TABLE 365_mt_sntl_station_data (
    stationtriplet VARCHAR,
    elementcode VARCHAR,
    durationname VARCHAR,
    storedunitcode VARCHAR,
    value_date VARCHAR,
    value double
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://joes-cap-stone-bucket/nwis/';

CREATE EXTERNAL TABLE 789_or_sntl_station_data (
    stationtriplet VARCHAR,
    elementcode VARCHAR,
    durationname VARCHAR,
    originalunitcode VARCHAR,
    value_date VARCHAR,
    value double
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://joes-cap-stone-bucket/nwis/';

