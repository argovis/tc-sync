# usage: python convert-hurdat.py <original file name> <output filename>
# see for example https://www.nhc.noaa.gov/data/hurdat/hurdat2-atl-1851-2023-042624.txt
# and https://www.nhc.noaa.gov/data/hurdat/hurdat2-nepac-1949-2023-042624.txt
# for upstream data.

import pandas as pd
import re, sys, numpy

# Read the file
with open(sys.argv[1], 'r') as file:
    raw = file.readlines()

# Find the start of each storm's data
storm_start = [i for i, line in enumerate(raw) if line.startswith('AL')]
if len(storm_start) == 0:
    storm_start = [i for i, line in enumerate(raw) if line.startswith('EP')]

# Extract the storm headers
storm_headers = [[i] + re.split(',', raw[i].strip()) for i in storm_start]

def get_storm(header):
    first_entry = int(header[0]) + 1
    lines = int(header[3])
    ID = header[1]
    name = header[2]

    # Extract the storm data and create a DataFrame
    storm_data = [re.split(',', raw[i].strip())[:8] for i in range(first_entry, first_entry + lines)]
    storm = pd.DataFrame(storm_data, columns=['DATE', 'TIME', 'L', 'CLASS', 'LAT', 'LONG', 'WIND', 'PRESS'])

    # Add the storm ID and name to each row
    storm['ID'] = ID
    storm['NAME'] = name

    return storm


# Apply the get_storm function to each storm header
storms = [get_storm(header) for header in storm_headers]

# Combine the list of DataFrames into a single DataFrame
storms = pd.concat(storms)
storms.reset_index(drop=True, inplace=True)

# Replace -999 pressure readings with NaN
storms['PRESS'] = storms['PRESS'].str.strip()
storms['PRESS'] = storms['PRESS'].replace('-999', numpy.NaN)

# Create 'SEASON', 'NUM' and 'LINK' columns
storms['SEASON'] = storms['ID'].str[4:8]
storms['NUM'] = storms['ID'].str[2:4].astype(int)
storms['LINK'] = 'https://www.nhc.noaa.gov/data/hurdat/' + sys.argv[1]

# Convert 'DATE' to datetime
storms['DATE'] = pd.to_datetime(storms['DATE'], format='%Y%m%d')
storms['TIME'] = storms['TIME'].str.zfill(4)

# Remove 'N' from 'LAT' and convert to numeric
storms['LAT'] = storms['LAT'].str.replace('N', '').astype(float)

# Remove 'E' or 'W' from 'LONG', convert to numeric, and multiply by -1 for 'W'
storms['LONG'] = storms['LONG'].str.strip()
storms.loc[storms['LONG'] == '-0.0W', 'LONG'] = '0.0'
storms.loc[storms['LONG'].str.contains('E'), 'LONG'] = storms['LONG'].str.replace('E', '')
storms.loc[storms['LONG'].str.contains('W'), 'LONG'] = '-' + storms['LONG'].str.replace('W', '')
storms['LONG'] = storms['LONG'].astype(float)

# dump whitespace
storms['NAME'] = storms['NAME'].str.strip()
storms['L'] = storms['L'].str.strip()
storms['CLASS'] = storms['CLASS'].str.strip()

# preserve legacy column order for compatibility with downstream code
column_order = ["ID","NAME","DATE","TIME","L","CLASS","LAT","LONG","WIND","PRESS","SEASON","NUM", "LINK"]
storms = storms.reindex(columns=column_order)

storms.to_csv(sys.argv[2], index=True)