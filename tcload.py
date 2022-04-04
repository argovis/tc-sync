import sys, json, xarray, math, datetime
from geopy import distance

from pymongo import MongoClient
client = MongoClient('mongodb://database/argo')
db = client.argo

def find_basin(lon, lat):
    # for a given lon, lat,
    # identify the basin from the lookup table.
    # choose the nearest non-nan grid point.

    gridspacing = 0.5
    basins = xarray.open_dataset('parameters/basinmask_01.nc')

    basin = basins['BASIN_TAG'].sel(LONGITUDE=lon, LATITUDE=lat, method="nearest").to_dict()['data']
    if math.isnan(basin):
        # nearest point was on land - find the nearest non nan instead.
        lonplus = math.ceil(lon / gridspacing)*gridspacing
        lonminus = math.floor(lon / gridspacing)*gridspacing
        latplus = math.ceil(lat / gridspacing)*gridspacing
        latminus = math.floor(lat / gridspacing)*gridspacing
        grids = [(basins['BASIN_TAG'].sel(LONGITUDE=lonminus, LATITUDE=latminus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latminus, lonminus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonminus, LATITUDE=latplus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latplus, lonminus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonplus, LATITUDE=latplus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latplus, lonplus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonplus, LATITUDE=latminus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latminus, lonplus)).miles)]

        grids = [x for x in grids if not math.isnan(x[0])]
        if len(grids) == 0:
            # all points on land
            #print('warning: all surrounding basin grid points are NaN')
            basin = -1
        else:
            grids.sort(key=lambda tup: tup[1])
            basin = grids[0][0]
    basins.close()
    return int(basin)

with open(sys.argv[1]) as raw:

	header = raw.readline().split(',')[1:] 				# split and drop first column
	header = [x.replace('"', '').replace('\n', '') for x in header]		# drop quotes

	record = raw.readline()
	documents = []
	while record:
		record = record.split(',')[1:]
		record = [x.replace('"', '').replace('\n', '') for x in record]

		# raw flat dict
		doc = {header[i].lower():record[i].replace(' ', '') for i in range(len(header))}

		# drop redundants
		del doc['date']
		del doc['time']
		del doc['season']

		# renames & constructs
		doc['record_identifier'] = doc['l'].replace(' ','')
		del doc['l']

		doc['_id'] = doc['id'] + '_' + doc['timestamp'].replace('-','').replace(' ','').replace(':', '')
		del doc['id']

		doc['basin'] = find_basin(float(doc['long']), float(doc['lat']))

		doc['data_type'] = 'tropicalCyclone'

		doc['date_updated_argovis'] = str(datetime.datetime.now())

		doc['geolocation'] = {"type": "Point", "coordinates": [float(doc['long']), float(doc['lat'])]}

		doc['source_info'] = {}
		if 'jtwc' in sys.argv[1].lower():
			doc['source_info']['source'] = 'tc_jtwc'
		elif 'hurdat' in sys.argv[1].lower():
			doc['source_info']['source'] = 'tc_hurdat'

		data_keys = []
		data = []
		if 'wind' in doc and doc['wind'] != 'NA':
			data_keys.append('wind')
			data.append(float(doc['wind']))
			del doc['wind']
		if 'press' in doc and doc['press'] != 'NA':
			data_keys.append('press')
			data.append(float(doc['press']))
			del doc['press']
		doc['data_keys'] = data_keys
		doc['data'] = data

		# write to mongo
		try:
			db.tc.insert_one(doc)
		except BaseException as err:
			print('error: db write failure')
			print(err)
			print(doc)
		# documents.append(doc)

		record = raw.readline()

# out = open('out.json', 'w')
# json.dump(documents, out)