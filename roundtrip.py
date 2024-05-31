import random, re, os, urllib.request, glob
from pymongo import MongoClient

client = MongoClient('mongodb://database/argo')
db = client.argo

def hurdat_munge_lat(lat):
	compass = None
	if(lat >= 0):
		compass = 'N'
	else:
		compass = 'S'
	return f"{round(abs(lat), 1)}{compass}"

def hurdat_munge_lon(lon, force=None):
	if force == 'E':
		if lon > 0:
			return f"{round(abs(lon), 1)}E"
		elif lon < 0:
			return f"{round(abs(360+lon), 1)}E"
		else:
			return '.0.0E' # longitude 0 can be 0E, 0W, -0E, or -0W, allow any
	elif force == 'W':
		if lon > 0:
			return f"{round(abs(360-lon), 1)}W"
		elif lon < 0:
			return f"{round(abs(360), 1)}W"
		else:
			return '.0.0W' 
	else:
		if lon > 0:
			return f"{round(abs(lon), 1)}E"
		elif lon < 0:
			return f"{round(abs(lon), 1)}W"
		else:
			return '.0.0W' 

def jtwc_munge_lat(lat):
	compass = None
	if(lat >= 0):
		compass = 'N'
	else:
		compass = 'S'
	return f"{int(abs(lat)*10)}{compass}" 

def jtwc_munge_lon(lon):
	compass = None
	if(lon >= 0):
		compass = 'E'
	else:
		compass = 'W'
	return f"{int(abs(lon)*10)}{compass}"


def find_prefix_match(prefix, fn):
	with open(fn, 'r') as file:
		for line in file:
			if re.match(prefix, line):
				return True
		return False

while True:
	# Select a random document from the 'tc' collection
	doc = db.tc.aggregate([{ "$sample": { "size": 1 } }])
	doc = next(doc, None)

	# Select a specific document from the 'tc' collection by id - for checking pathologies
	#doc = db.tc.find_one({ "_id": "SH051989_19891031060000" })
	
	if doc is None:
		print("No document found.")
		break

	# Fetch the corresponding metadata document from the 'tcMeta' collection
	metadata_doc = db.tcMeta.find_one({ "_id": doc['metadata'][0] })

	if metadata_doc is None:
		print(f"No metadata found for ID {doc['metadata'][0]}.")
		continue

	# Determine the source of the document and filename
	origin = None
	fn = None
	url = metadata_doc['source'][0]['url']
	if 'hurdat' in url:
		origin='hurdat'
		fn = url.split('/')[-1]
	elif 'jtwc' in url:
		origin='jtwc'
		fn = url.split('/')[-1]
	else:
		print(f"Unknown source: {url}")
		continue

	if origin == 'hurdat':
		hurdatstring = ''
		hurdatstring += doc['_id'].split('_')[1][0:8] + ', '
		hurdatstring += doc['_id'].split('_')[1][8:12] + ','
		hurdatstring += str(doc['record_identifier']).rjust(2) + ', '
		hurdatstring += doc['class'].rjust(2) + ','
		hurdatstring += hurdat_munge_lat(doc['geolocation']['coordinates'][1]).rjust(6) + ','
		hurdatstring += hurdat_munge_lon(doc['geolocation']['coordinates'][0]).rjust(7) + ','
		hurdatstring += str(int(doc['data'][0][0])).rjust(4) + ',' if doc['data'][0][0] is not None else ' -999' + ','
		hurdatstring += str(int(doc['data'][1][0])).rjust(5) if doc['data'][1][0] is not None else ' -999'
		valid = find_prefix_match(hurdatstring, f'data/{fn}')
		if not valid:	
			# every once in a while they report in degrees W instead of E
			wlon = hurdat_munge_lon(doc['geolocation']['coordinates'][0], 'W').rjust(7)
			hurdatstring = f"{hurdatstring[0:29]}{wlon}{hurdatstring[36:]}"
			valid = find_prefix_match(hurdatstring, f'data/{fn}')
			if not valid:
				print('hurdat mismatch')
				print(doc)
				print(metadata_doc)
				print(hurdatstring)
	elif origin == 'jtwc':
		jtwcstring = ''
		jtwcstring += doc['_id'][0:2] + ', '
		jtwcstring += doc['_id'][2:4] + ', '
		jtwcstring += doc['_id'].split('_')[1][0:10] + ','
		jtwcstring += '...,.....,....,'
		jtwcstring += '[\s0]*' + jtwc_munge_lat(doc['geolocation']['coordinates'][1]) + ',' # sometimes ' ' padded, sometimes 0 padded...
		jtwcstring += jtwc_munge_lon(doc['geolocation']['coordinates'][0]).rjust(6) + ','
		jtwcstring += '\s*' + str(int(doc['data'][0][0])) if doc['data'][0][0] is not None else '-999' # usually padded to 4 spaces, sometimes 3
		if doc['data'][1][0] is not None:
			jtwcstring += ',' + str(int(doc['data'][1][0])).rjust(5) + ','
			jtwcstring += doc['class'].rjust(3)

		# fetch the zip file, extract the files, and look for a match
		output_dir = './testpen'
		# Create the output directory if it doesn't exist
		os.makedirs(output_dir, exist_ok=True)
		# Download the file
		for n in metadata_doc['source']:
			url = n['url']
			filename = url.split('/')[-1]
			print(url)
			urllib.request.urlretrieve(url, os.path.join(output_dir, filename))
			# Extract the file
			os.system(f"unzip -o {output_dir}/{filename} -d {output_dir} > /dev/null 2>&1")
		# Get a list of all .txt and .dat files in the directory
		files = glob.glob(f"{output_dir}/*.txt") + glob.glob(f"{output_dir}/*.dat")
		# Search each file for a line that begins with jtwcstring
		valid = False
		for fn in files:
			valid = valid or find_prefix_match(jtwcstring, fn)
		if not valid:
			print('jtwc mismatch')
			print(doc)
			print(metadata_doc)
			print(jtwcstring)
		# Clean up the extracted files
		for fn in glob.glob(f"{output_dir}/*"):
			os.remove(fn)








