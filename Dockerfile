FROM python:3.9

RUN apt-get update -y
RUN apt-get install -y nano jq
RUN pip install nose pymongo numpy pandas xarray netcdf4 geopy requests

WORKDIR /app
COPY tcload.py tcload.py
COPY parseall.sh parseall.sh
COPY summary-computation.py summary-computation.py
COPY parameters parameters
RUN chown -R 1000660000 /app