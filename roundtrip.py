# usage: python roundtrip.py <hurdat/jtwc filename>
import requests, sys, datetime, time

def match(a,b):
	if a != b:
		print(f'mismatch between {a} and {b}', flush=True)

print(f'checking {sys.argv[1]}', flush=True)

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

		tcid = row['id'] + '_' + row['timestamp'].replace(':','').replace('-','').replace(' ','')
		print(f'checking {tcid}', flush=True)

		# parse some types
		row['timestamp'] = datetime.datetime.strptime(row['timestamp'], "%Y-%m-%d%H:%M:%S")
		row['lat'] = float(row['lat'])
		row['long'] = float(row['long'])
		if row['wind'] == 'NA':
			row['wind'] = None
		else:
			row['wind'] = float(row['wind'])
		if row['press'] == 'NA':
			row['press'] = None
		else:
			row['press'] = float(row['press'])
		row['num'] = int(row['num'])

		data = requests.get('https://argovis-api.colorado.edu/tc', params = {'id': tcid, 'data':'all', 'compression':'array'}).json()
		try:
			data = data[0]
		except:
			print(data, flush=True)
			if 'code' in data and data['code'] == 429:
				time.sleep(2)
				continue
		metadata = requests.get('https://argovis-api.colorado.edu/tc/meta', params = {'id': data['metadata']}).json()
		try:
			metadata = metadata[0]
		except:
			print(metadata, flush=True)
			if 'code' in metadata and metadata['code'] == 429:
				time.sleep(2)
				continue

		match(row['name'], metadata['name'])
		match(row['timestamp'], datetime.datetime.strptime(data['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ"))
		match(row['l'], data['record_identifier'])
		match(row['class'], data['class'])
		match(row['long'], data['geolocation']['coordinates'][0])
		match(row['lat'], data['geolocation']['coordinates'][1])
		match(row['wind'], data['data'][0][0])
		match(row['press'], data['data'][0][1])
		match(row['num'], metadata['num'])

		time.sleep(2)
		record = raw.readline()


