import sys, json, xarray, math, datetime
from geopy import distance

from pymongo import MongoClient
client = MongoClient('mongodb://database/argo')
db = client.argo
loadtime = datetime.datetime.now()

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
		row = {header[i].lower():record[i].replace(' ', '') for i in range(len(header))}

		# construct metadata record
		meta = {
			'_id': row['id'],
			'data_type': 'tropicalCyclone',
			'data_keys': ['wind', 'pres'],
			'units': ['kt', 'mb'],
			'date_updated_argovis': loadtime,
			'source': {},
			'name': row['name'], 
			'num': row['num']
		}
		if 'jtwc' in sys.argv[1].lower():
			meta['source']['source'] = 'tc_jtwc'
		elif 'hurdat' in sys.argv[1].lower():
			meta['source']['source'] = 'tc_hurdat'

		# write to mongo
		try:
			# each row that generates the same metadata record will overwrite the last;
			# this is ok as long as whatever generates the _id field isn't degenerate when it shouldn't be,
			# ie generates unique IDs for unique combinations of metadata.
			db.tcMetax.replace_one({"_id": meta['_id']}, meta, upsert=True)
		except BaseException as err:
			print('error: db write failure')
			print(err)
			print(meta)

		# construct data record
		data = {
			'_id': row['id'] + '_' + row['timestamp'].replace('-','').replace(' ','').replace(':', ''),
			'metadata': row['id'],
			'geolocation': {"type": "Point", "coordinates": [float(row['long']), float(row['lat'])]},
			'basin': find_basin(float(row['long']), float(row['lat'])),
			'timestamp': datetime.datetime.strptime(row['timestamp'],'%Y-%m-%d %H:%M:%S'),
			'data': [[]],
			'record_identifier': row['l'].replace(' ',''),
			'class': row['class']
		}
		if row['wind'] != 'NA':
			data['data'][0] = float(row['wind'])
		else:
			data['data'][0] = None
		if row['press'] != 'NA':
			data['data'][1] = float(row['press'])
		else:
			data['data'][1] = None

		# write to mongo
		try:
			db.tcx.insert_one(data)
		except BaseException as err:
			print('error: db write failure')
			print(err)
			print(doc)
		# documents.append(doc)

		record = raw.readline()

