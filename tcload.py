# usage: python tcload.py <file produced by either data/convert-hurdat.py or data/convert-jtwc.py>
import sys, json, xarray, math, datetime
from geopy import distance

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

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

def munge_timestamp(year, month, day, hour, minute):
	# one record was indicated as being from October zeroth; catch, assume they meant the first, and add a warning
	
	data_warning = {}
	try:
		dt = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute))
	except ValueError:
		if int(day) == 0:
			dt = datetime.datetime(int(year), int(month), 1, int(hour))
			data_warning['true_timestamp'] = [int(year), int(month), int(day), int(hour), int(minute)]
		else:
			print(ValueError)

	return dt.strftime('%Y-%m-%d%H:%M:%S'), data_warning

def remap_longitude(longitude):
    longitude = longitude % 360
    if longitude > 180:
        longitude -= 360
    return longitude

with open(sys.argv[1]) as raw:

	header = raw.readline().split(',')[1:] 				# split and drop first column
	header = [x.replace('"', '').replace('\n', '') for x in header]		# drop quotes

	record = raw.readline()
	documents = []
	while record:
		data_warning = {}
		record = record.split(',')[1:]
		record = [x.replace('"', '').replace('\n', '') for x in record]
		# raw flat dict
		row = {header[i].lower():record[i].replace(' ', '') for i in range(len(header))}
		row['timestamp'], data_warning = munge_timestamp(row['date'][0:4], row['date'][5:7], row['date'][8:10], int(row['time'][0:2]), int(row['time'][2:4]))

		# construct metadata record
		meta = {
			'_id': row['id'],
			'data_type': 'tropicalCyclone',
			'data_info': [['wind', 'surface_pressure'], ['units'], [['kt'],['mb']]],
			'date_updated_argovis': loadtime,
			'source': [{"url": row['link']}],
			'name': row['name'], 
			'num': int(row['num'])
		}
		if 'jtwc' in sys.argv[1].lower():
			meta['source'][0]['source'] = ['tc_jtwc']
		elif 'hurdat' in sys.argv[1].lower():
			meta['source'][0]['source'] = ['tc_hurdat']

		# construct data record
		data = {
			'_id': str(row['id']) + '_' + row['timestamp'].replace('-','').replace(' ','').replace(':', ''),
			'metadata': [row['id']],
			'geolocation': {"type": "Point", "coordinates": [remap_longitude(float(row['long'])), float(row['lat'])]},
			'basin': find_basin(remap_longitude(float(row['long'])), float(row['lat'])),
			'timestamp': datetime.datetime.strptime(row['timestamp'],'%Y-%m-%d%H:%M:%S'),
			'data': [[None], [None]],
			'record_identifier': row['l'].replace(' ',''),
			'class': row['class']
		}
		if row['wind'] != 'NA':
			if row['wind'] == '':
				data['data'][0][0] = None
			else:
				data['data'][0][0] = float(row['wind'])
		if row['press'] != 'NA':
			if row['press'] == '':
				data['data'][1][0] = None
			else:
				data['data'][1][0] = float(row['press'])
		if len(list(data_warning.keys())) > 0:
			data['data_warning'] = data_warning

		if data['data'] == [[None], [None]]:
			pass
		else:
			# write to mongo
			## metadata write
			try:
				db.tcMeta.insert_one(meta)
			except DuplicateKeyError:
				# there are some instances where a storm IDed in one year is found in the zip archive of another year
				existing_meta = db.tcMeta.find_one({'_id': meta['_id']})
				urls = [x['url'] for x in existing_meta['source']]
				if row['link'] not in urls:
					existing_meta['source'].append(meta['source'][0])
					db.tcMeta.replace_one({'_id': meta['_id']}, existing_meta)
			except BaseException as err:
				print('error: meta db write failure')
				print(err)
				print(meta)
			## data write
			try:
				db.tc.insert_one(data)
			except DuplicateKeyError:
				# duplicate ID; keep the original with a flag
				doc = db.tc.find_one({'_id': data['_id']})
				if 'data_warning' in doc:
					if 'duplicate' in doc['data_warning']:
						doc['data_warning']['duplicate'].append(row['link'])
					else:
						doc['data_warning']['duplicate'] = [row['link']]
				else:
					doc['data_warning'] = {'duplicate': [row['link']]}
				db.tc.replace_one({'_id': data['_id']}, doc)
			except BaseException as err:
				print('error: data db write failure')
				print(err)
				print(data)

		record = raw.readline()

