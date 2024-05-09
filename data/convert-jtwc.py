# usage: python convert-jtwc.py <original file name> <output filename>
# see for example https://www.metoc.navy.mil/jtwc/jtwc.html?southern-hemisphere
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

def getTimestamp(lst):
    ts = lst['DATE']
    year = int(ts[:4])
    month = int(ts[4:6])
    day = int(ts[6:8])
    hour = int(ts[8:10])
    return pd.Timestamp(
            year=year,
            month=month,
            day=day,
            hour=hour,
            )

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
        return 0xDEADBEEF

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
        'TIMESTAMP':df.apply(lambda r: try_get(getTimestamp, r), axis=1),    # TIMESTAMP
        }).drop_duplicates()
    df = df[~df.isin([0xDEADBEEF]).any(axis=1)]
    df['ID_TIMESTAMP'] = df['ID'] + df['TIMESTAMP'].astype(str)

    # Check if 'ID_TIMESTAMP' is unique
    if not df['ID_TIMESTAMP'].is_unique:
        # Find duplicates
        duplicates = df[df['ID_TIMESTAMP'].duplicated(keep=False)]
        duplicates = duplicates.sort_values('TIMESTAMP')
        pd.set_option('display.max_rows', None)
        print(duplicates)

    # Drop the temporary column
    df = df.drop(columns=['ID_TIMESTAMP'])
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
                row += [None] * (28 - len(row))
                # Append the row to df_lst
                df_lst.append(row[:28])

# Convert df_lst to a DataFrame
raw = pd.DataFrame(df_lst, columns=['BASIN', 'CY', 'DATE', 'TECHNUM', 'TECH', 'TAU', 'LAT', 'LON', 'VMAX', 'MSLP', 'CLASS', 'RAD' , 'WINDCODE' , 'RAD1' , 'RAD2' , 'RAD3' , 'RAD4' , 'RADP' , 'RRP' , 'MRD' , 'GUSTS' , 'EYE' , 'SUBREGN' , 'MAXSEAS' , 'INITIALS' , 'DIR' , 'SPEED' , 'STORMNAME'])
# Replace empty strings with None
raw.replace("", None, inplace=True)
raw['VMAX'] = raw['VMAX'].replace('-999', None)
raw['MSLP'] = raw['MSLP'].replace('-999', None)
# perform conversion
df = convert_df(raw)

df.to_csv(sys.argv[2], index=True)

