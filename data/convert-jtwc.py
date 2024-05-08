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

def convert_df(df):
    n, _ = df.shape
    df = pd.DataFrame({
        'ID':       df.apply(lambda r: getID(r), axis=1),           # ID
        'NAME':     ['UNNAMED' for _ in range(n)],                  # NAME
        'DATE':     df.apply(lambda r: getDate(r), axis=1),         # DATE
        'TIME':     df.apply(lambda r: getTime(r), axis=1),         # TIME
        'L':        ['  ' for _ in range(n)],                       # L
        'CLASS':    df.apply(lambda r: getClass(r), axis=1),        # CLASS
        'LAT':      df.apply(lambda r: getLat(r), axis=1),          # LAT
        'LONG':     df.apply(lambda r: getLon(r), axis=1),          # LONG
        'WIND':     df.apply(lambda r: getWind(r), axis=1),         # WIND
        'PRESS':    df.apply(lambda r: getPress(r), axis=1),        # PRESS
        'SEASON':   df.apply(lambda r: getSeason(r), axis=1),       # SEASON
        'NUM':      df.apply(lambda r: getNum(r), axis=1),          # NUM
        'TIMESTAMP':df.apply(lambda r: getTimestamp(r), axis=1),    # TIMESTAMP
        }).drop_duplicates()
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
                # If there are less than 11 columns, fill the missing ones with None
                row += [None] * (11 - len(row))
                # Append the row to df_lst
                df_lst.append(row[:11])

# Convert df_lst to a DataFrame
raw = pd.DataFrame(df_lst, columns=['BASIN', 'CY', 'DATE', 'TECHNUM', 'TECH', 'TAU', 'LAT', 'LON', 'VMAX', 'MSLP', 'CLASS'])
# Replace empty strings with None
raw.replace("", None, inplace=True)
raw['VMAX'] = raw['VMAX'].replace('-999', None)
raw['MSLP'] = raw['MSLP'].replace('-999', None)
# perform conversion
df = convert_df(raw)

# print(raw)
print(df)

