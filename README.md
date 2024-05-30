This repo contains scripts and data for populating tropical cyclone information in Argovis.

## Rebuilding from scratch

Database rebuild proceeds in two steps: munging the HURDAT and JTWC data into consistent CSVs, then populating mongoDB with JSON representations of that data.

 - Download the following:
  - https://www.nhc.noaa.gov/data/hurdat/hurdat2-atl-1851-2023-042624.txt
  - https://www.nhc.noaa.gov/data/hurdat/hurdat2-nepac-1949-2023-042624.txt
  - every zip file from:
   - https://www.metoc.navy.mil/jtwc/jtwc.html?southern-hemisphere
   - https://www.metoc.navy.mil/jtwc/jtwc.html?north-indian-ocean
   - https://www.metoc.navy.mil/jtwc/jtwc.html?western-pacific
 - Use `data/convert-hurdat.py` and `data/convert-jtwc.py` to convert the upstream data to homogenous CSVs. Note this step will drop rows in the upstream data that are identical over the variables we concern ourselves with.
 - Recreate a schema-enforced collection for tropical cyclones and TC metadata using https://github.com/argovis/db-schema/blob/main/tc.py
 - populate collections using `tcload.py`. Note this step will drop rows that have the same ID and timestamp as a previous row, adding a data warning to the pre-existing document indicating the duplicate.
 - `roundtrip.py` will randomly pull TC records and look for a corresponding line in the upstream data.