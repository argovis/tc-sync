python tcload.py '/tmp/HURDAT_ATLANTIC.csv' > /tmp/tc/atlantic.log
python tcload.py '/tmp/HURDAT_PACIFIC.csv' > /tmp/tc/pacific.log
python tcload.py '/tmp/JTWC_INDIANOCEAN.csv' > /tmp/tc/indian.log
python tcload.py '/tmp/JTWC_SOUTHERNHEMISPHERE.csv' > /tmp/tc/southern.log
python tcload.py '/tmp/JTWC_WESTPACIFIC.csv' > /tmp/tc/westpac.log
sleep 10000000 # keep the container alive so we can fetch the logfiles; write to a volume instead once more storage becomes available
python summary-computation.py