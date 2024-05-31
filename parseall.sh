python tcload.py '/tmp/HURDAT_ATLANTIC_2024.csv' > /tmp/atlantic.log
python tcload.py '/tmp/HURDAT_PACIFIC_2024.csv' > /tmp/pacific.log
python tcload.py '/tmp/JTWC_INDIANOCEAN.csv' > /tmp/indian.log
python tcload.py '/tmp/JTWC_SOUTHERNHEMISPHERE.csv' > /tmp/southern.log
python tcload.py '/tmp/JTWC_WESTPACIFIC.csv' > /tmp/westpac.log
python summary-computation.py
sleep 10000000 # keep the container alive so we can fetch the logfiles; write to a volume instead once more storage becomes available
