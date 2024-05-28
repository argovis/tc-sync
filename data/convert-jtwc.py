# usage: python convert-jtwc.py <directory of original files> <output filename>
# see for example url roots https://www.metoc.navy.mil/jtwc/jtwc.html?southern-hemisphere
# https://www.metoc.navy.mil/jtwc/jtwc.html?north-indian-ocean
# and https://www.metoc.navy.mil/jtwc/jtwc.html?western-pacific
# for upstream data.

import os, sys
import numpy as np
import pandas as pd

def getLat(lst):
    rawlat = lst['LAT']
    if rawlat[-1] == 'N':
        sign = 1
    else:
        sign = -1
    return float(rawlat[:-1]) / 10 * sign

def getLon(lst):
    rawlon = lst['LON']
    if rawlon[-1] == 'W':
        sign = -1
    else:
        sign = 1
    return float(rawlon[:-1]) / 10 * sign

def getTime(lst):
    timestamp = lst['DATE']
    return timestamp[-2:] + '00'

def getDate(lst):
    ts = lst['DATE']
    return '-'.join([ts[:4], ts[4:6], ts[6:8]])

def getClass(lst):
    return lst['CLASS']

def getName(lst):
    name = lst['STORMNAME']
    if name is None:
        name = 'UNNAMED'
    return name

def getSeason(lst):
    ts = lst['DATE']
    return ts[:4]

def getNum(lst):
    rawnum = lst['CY']
    return int(rawnum)

def getID(lst):
    return lst['BASIN'] + lst['CY'] + lst['DATE'][0:4]

def getWind(lst):
    if lst['VMAX'] is not None:
        return int(lst['VMAX'])
    else:  
        return None 

def getPress(lst):
    if lst['MSLP'] is not None:
        return int(lst['MSLP']  )
    else:
        return None

def try_get(func, r):
    try:
        return func(r)
    except:
        print(r)
        return 0xDEADBEEF

def parse_filename(name):
    region_prefix = name[0:3]
    if region_prefix == 'bcp':
        region_prefix = 'bwp' # a few files unpack as bcp, but the upstrem zip files are all labeled bwp
    storm_number = name[3:5]
    year = name[5:9]

    return region_prefix, storm_number, year

def convert_df(df):
    n, _ = df.shape
    df = pd.DataFrame({
        'ID':       df.apply(lambda r: try_get(getID, r), axis=1),           # ID
        'NAME':     df.apply(lambda r: try_get(getName, r), axis=1),                # NAME
        'DATE':     df.apply(lambda r: try_get(getDate, r), axis=1),         # DATE
        'TIME':     df.apply(lambda r: try_get(getTime, r), axis=1),         # TIME
        'L':        ['  ' for _ in range(n)],                       # L
        'CLASS':    df.apply(lambda r: try_get(getClass, r), axis=1),        # CLASS
        'LAT':      df.apply(lambda r: try_get(getLat, r), axis=1),          # LAT
        'LONG':     df.apply(lambda r: try_get(getLon, r), axis=1),          # LONG
        'WIND':     df.apply(lambda r: try_get(getWind, r), axis=1),         # WIND
        'PRESS':    df.apply(lambda r: try_get(getPress, r), axis=1),        # PRESS
        'SEASON':   df.apply(lambda r: try_get(getSeason, r), axis=1),       # SEASON
        'NUM':      df.apply(lambda r: try_get(getNum, r), axis=1),          # NUM
        'LINK':     df['LINK'],                                              # LINK
        }).drop_duplicates() # ie drop rows that are only distinct in variables we aren't interested in
    df = df[~df.isin([0xDEADBEEF]).any(axis=1)]

    return df


df_lst = []
dr = sys.argv[1]
for fn in os.listdir(dr):
    if fn[-3:] == 'txt' or fn[-3:] == 'dat':
        with open(dr+fn, 'r') as file:
            for line in file:
                # Split the line
                row = line.strip().split(',')
                row = [x.strip() for x in row]
                # If there are less than 28 columns, fill the missing ones with None
                # then drop extra stuff
                row += [None] * (28 - len(row))
                row = row[:28]
                # add an appropriate filename
                region_prefix, storm_number, storm_year = parse_filename(fn)
                row.append(f'https://www.metoc.navy.mil/jtwc/products/best-tracks/{storm_year}/{storm_year}s-{region_prefix}/{region_prefix}{storm_year}.zip')
                # Append the row to df_lst
                df_lst.append(row)

# Convert df_lst to a DataFrame
raw = pd.DataFrame(df_lst, columns=['BASIN', 'CY', 'DATE', 'TECHNUM', 'TECH', 'TAU', 'LAT', 'LON', 'VMAX', 'MSLP', 'CLASS', 'RAD' , 'WINDCODE' , 'RAD1' , 'RAD2' , 'RAD3' , 'RAD4' , 'RADP' , 'RRP' , 'MRD' , 'GUSTS' , 'EYE' , 'SUBREGN' , 'MAXSEAS' , 'INITIALS' , 'DIR' , 'SPEED' , 'STORMNAME', 'LINK'])
# Replace empty strings with None
raw.replace("", None, inplace=True)
raw['VMAX'] = raw['VMAX'].replace('-999', None)
raw['MSLP'] = raw['MSLP'].replace('-999', None)
# perform conversion
df = convert_df(raw)

df.to_csv(sys.argv[2], index=True)

