python tcload.py 'data/HURDAT_ATLANTIC.csv' > atlantic.log
python tcload.py 'data/HURDAT_PACIFIC.csv' > pacific.log
python tcload.py 'data/JTWC_INDIANOCEAN.csv' > indian.log
python tcload.py 'data/JTWC_SOUTHERNHEMISPHERE.csv' > southern.log
python tcload.py 'data/JTWC_WESTPACIFIC.csv' > westpac.log
sleep 10000000 # keep the container alive so we can fetch the logfiles; write to a volume instead once more storage becomes available